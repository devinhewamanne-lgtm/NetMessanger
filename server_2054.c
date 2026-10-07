#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

/* =========================================================
   PERSONALIZATION
   ========================================================= */

#define REGISTRATION_NUMBER "IT23752054"

#define PORT 8054
#define NID_TAG "NID:7520"

#define LOG_FILE "netmsg_IT23752054.log"

#define STORAGE_ROOT "./storage/IT23752054"

#define BACKLOG 10

#define MAX_CLIENTS 20
#define MAX_ROOMS 20

#define MAX_USERNAME 32
#define MAX_ROOM_NAME 32

#define BUFFER_SIZE 4096
#define LIST_BUFFER_SIZE 640

/*
 * Maximum accepted file size: 10 MiB
 */
#define MAX_FILE_SIZE (10ULL * 1024ULL * 1024ULL)


/* =========================================================
   DATA STRUCTURES
   ========================================================= */

typedef struct
{
    int socket_fd;
    int registered;

    char username[MAX_USERNAME];

    /*
     * joined_rooms[i] == 1
     * means this client belongs to rooms[i].
     */
    int joined_rooms[MAX_ROOMS];

    /*
     * TCP receive buffer.
     */
    char recv_buffer[BUFFER_SIZE];
    size_t recv_len;

} Client;


typedef struct
{
    int active;
    char name[MAX_ROOM_NAME];

} Room;


/* =========================================================
   GLOBAL STATE
   ========================================================= */

Client clients[MAX_CLIENTS];
Room rooms[MAX_ROOMS];

pthread_mutex_t clients_mutex =
    PTHREAD_MUTEX_INITIALIZER;


/* =========================================================
   FUNCTION PROTOTYPES
   =========================================================
   These declarations fix the previous:
   "implicit declaration of function" errors.
   ========================================================= */

void log_event(const char *event);

int send_all(int socket_fd,
             const char *data,
             size_t length);

int send_response(int socket_fd,
                  const char *response);

int receive_line(Client *client,
                 char *line,
                 size_t line_size);

int receive_exact(Client *client,
                  unsigned char *data,
                  size_t length);

int discard_exact(Client *client,
                  size_t length);

int valid_username(const char *username);

int valid_room_name(const char *room_name);

Client *find_client_by_username(const char *username);

int find_room_index(const char *room_name);

int safe_filename(const char *filename,
                  char *output,
                  size_t output_size);

int create_user_storage(const char *username,
                        char *path,
                        size_t path_size);

void notify_presence(Client *client,
                     const char *action);

int register_user(Client *client,
                  const char *username);

void list_users(Client *client);

void broadcast_message(Client *sender,
                        const char *message);

void private_message(Client *sender,
                     const char *target_username,
                     const char *message);

void join_room(Client *client,
               const char *room_name);

void leave_room(Client *client,
                const char *room_name);

void list_rooms(Client *client);

void room_message(Client *sender,
                  const char *room_name,
                  const char *message);

int handle_sendfile(Client *sender,
                    const char *command);

void remove_client(Client *client);

void *handle_client(void *arg);


/* =========================================================
   LOGGING
   ========================================================= */

void log_event(const char *event)
{
    FILE *fp =
        fopen(LOG_FILE, "a");

    if (fp == NULL)
    {
        perror("[SERVER] fopen log");
        return;
    }

    time_t now =
        time(NULL);

    struct tm *tm_info =
        localtime(&now);

    char timestamp[64];

    if (tm_info != NULL)
    {
        strftime(timestamp,
                 sizeof(timestamp),
                 "%Y-%m-%d %H:%M:%S",
                 tm_info);
    }
    else
    {
        snprintf(timestamp,
                 sizeof(timestamp),
                 "UNKNOWN-TIME");
    }

    fprintf(fp,
            "[%s] %s\n",
            timestamp,
            event);

    fclose(fp);
}


/* =========================================================
   SEND ALL
   ========================================================= */

int send_all(int socket_fd,
             const char *data,
             size_t length)
{
    size_t total_sent = 0;

    while (total_sent < length)
    {
        ssize_t sent =
            send(socket_fd,
                 data + total_sent,
                 length - total_sent,
                 0);

        if (sent < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }

            return -1;
        }

        if (sent == 0)
        {
            return -1;
        }

        total_sent +=
            (size_t)sent;
    }

    return 0;
}


/* =========================================================
   SEND SERVER RESPONSE
   ========================================================= */

int send_response(int socket_fd,
                  const char *response)
{
    char line[BUFFER_SIZE];

    int written =
        snprintf(line,
                 sizeof(line),
                 "%s %s\n",
                 response,
                 NID_TAG);

    if (written < 0 ||
        (size_t)written >= sizeof(line))
    {
        return -1;
    }

    return send_all(socket_fd,
                    line,
                    (size_t)written);
}


/* =========================================================
   RECEIVE ONE COMPLETE LINE
   ========================================================= */

int receive_line(Client *client,
                 char *line,
                 size_t line_size)
{
    while (1)
    {
        /*
         * Search for newline already in buffer.
         */
        for (size_t i = 0;
             i < client->recv_len;
             i++)
        {
            if (client->recv_buffer[i] == '\n')
            {
                size_t line_length =
                    i + 1;

                if (line_length >= line_size)
                {
                    return -2;
                }

                memcpy(line,
                       client->recv_buffer,
                       line_length);

                line[line_length] = '\0';

                /*
                 * Remove processed line.
                 */
                size_t remaining =
                    client->recv_len -
                    line_length;

                memmove(client->recv_buffer,
                        client->recv_buffer +
                            line_length,
                        remaining);

                client->recv_len =
                    remaining;

                return 1;
            }
        }

        /*
         * Protect receive buffer.
         */
        if (client->recv_len >=
            sizeof(client->recv_buffer) - 1)
        {
            return -2;
        }

        /*
         * Receive more TCP bytes.
         */
        ssize_t received =
            recv(client->socket_fd,
                 client->recv_buffer +
                     client->recv_len,
                 sizeof(client->recv_buffer) -
                     client->recv_len - 1,
                 0);

        if (received == 0)
        {
            return 0;
        }

        if (received < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }

            return -1;
        }

        client->recv_len +=
            (size_t)received;

        client->recv_buffer[
            client->recv_len] = '\0';
    }
}


/* =========================================================
   RECEIVE EXACTLY N BYTES
   Used for file transfer.
   ========================================================= */

int receive_exact(Client *client,
                  unsigned char *data,
                  size_t length)
{
    size_t total_received = 0;

    /*
     * First consume bytes already buffered after
     * the SENDFILE header.
     */
    while (total_received < length &&
           client->recv_len > 0)
    {
        size_t available =
            client->recv_len;

        size_t needed =
            length - total_received;

        size_t to_copy =
            available < needed
                ? available
                : needed;

        memcpy(data + total_received,
               client->recv_buffer,
               to_copy);

        total_received +=
            to_copy;

        size_t remaining =
            client->recv_len -
            to_copy;

        memmove(client->recv_buffer,
                client->recv_buffer +
                    to_copy,
                remaining);

        client->recv_len =
            remaining;
    }

    /*
     * Receive remaining bytes directly
     * from the TCP socket.
     */
    while (total_received < length)
    {
        ssize_t received =
            recv(client->socket_fd,
                 data + total_received,
                 length - total_received,
                 0);

        if (received == 0)
        {
            return 0;
        }

        if (received < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }

            return -1;
        }

        total_received +=
            (size_t)received;
    }

    return 1;
}


/* =========================================================
   DISCARD EXACTLY N BYTES
   Used to keep TCP stream synchronized after a rejected
   file transfer.
   ========================================================= */

int discard_exact(Client *client,
                  size_t length)
{
    unsigned char temp[8192];

    size_t total =
        0;

    /*
     * Discard bytes already buffered.
     */
    while (total < length &&
           client->recv_len > 0)
    {
        size_t available =
            client->recv_len;

        size_t needed =
            length - total;

        size_t to_consume =
            available < needed
                ? available
                : needed;

        size_t remaining =
            client->recv_len -
            to_consume;

        memmove(client->recv_buffer,
                client->recv_buffer +
                    to_consume,
                remaining);

        client->recv_len =
            remaining;

        total +=
            to_consume;
    }

    /*
     * Discard remaining bytes from socket.
     */
    while (total < length)
    {
        size_t chunk =
            length - total;

        if (chunk > sizeof(temp))
        {
            chunk = sizeof(temp);
        }

        ssize_t received =
            recv(client->socket_fd,
                 temp,
                 chunk,
                 0);

        if (received == 0)
        {
            return 0;
        }

        if (received < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }

            return -1;
        }

        total +=
            (size_t)received;
    }

    return 1;
}


/* =========================================================
   USERNAME VALIDATION
   ========================================================= */

int valid_username(const char *username)
{
    if (username == NULL)
    {
        return 0;
    }

    size_t length =
        strlen(username);

    if (length == 0 ||
        length >= MAX_USERNAME)
    {
        return 0;
    }

    for (size_t i = 0;
         i < length;
         i++)
    {
        if (username[i] == ' ' ||
            username[i] == '\t' ||
            username[i] == '\r' ||
            username[i] == '\n')
        {
            return 0;
        }
    }

    return 1;
}


/* =========================================================
   ROOM NAME VALIDATION
   ========================================================= */

int valid_room_name(const char *room_name)
{
    if (room_name == NULL)
    {
        return 0;
    }

    size_t length =
        strlen(room_name);

    if (length == 0 ||
        length >= MAX_ROOM_NAME)
    {
        return 0;
    }

    for (size_t i = 0;
         i < length;
         i++)
    {
        if (room_name[i] == ' ' ||
            room_name[i] == '\t' ||
            room_name[i] == '\r' ||
            room_name[i] == '\n')
        {
            return 0;
        }
    }

    return 1;
}


/* =========================================================
   FIND CLIENT BY USERNAME
   Caller should hold clients_mutex.
   ========================================================= */

Client *find_client_by_username(const char *username)
{
    for (int i = 0;
         i < MAX_CLIENTS;
         i++)
    {
        if (clients[i].registered &&
            strcmp(clients[i].username,
                   username) == 0)
        {
            return &clients[i];
        }
    }

    return NULL;
}


/* =========================================================
   FIND ROOM
   Caller should hold clients_mutex.
   ========================================================= */

int find_room_index(const char *room_name)
{
    for (int i = 0;
         i < MAX_ROOMS;
         i++)
    {
        if (rooms[i].active &&
            strcmp(rooms[i].name,
                   room_name) == 0)
        {
            return i;
        }
    }

    return -1;
}


/* =========================================================
   SAFE FILENAME
   ========================================================= */

int safe_filename(const char *filename,
                  char *output,
                  size_t output_size)
{
    if (filename == NULL ||
        output == NULL ||
        output_size == 0)
    {
        return 0;
    }

    size_t length =
        strlen(filename);

    if (length == 0 ||
        length >= output_size)
    {
        return 0;
    }

    /*
     * Prevent path traversal.
     */
    if (strcmp(filename, ".") == 0 ||
        strcmp(filename, "..") == 0)
    {
        return 0;
    }

    if (strchr(filename, '/') != NULL ||
        strchr(filename, '\\') != NULL)
    {
        return 0;
    }

    if (strstr(filename, "..") != NULL)
    {
        return 0;
    }

    memcpy(output,
           filename,
           length + 1);

    return 1;
}


/* =========================================================
   CREATE USER STORAGE DIRECTORY
   ========================================================= */

int create_user_storage(const char *username,
                        char *path,
                        size_t path_size)
{
    int written =
        snprintf(path,
                 path_size,
                 "%s/%s",
                 STORAGE_ROOT,
                 username);

    if (written < 0 ||
        (size_t)written >= path_size)
    {
        return 0;
    }

    /*
     * Create root directory.
     */
    if (mkdir(STORAGE_ROOT, 0755) < 0 &&
        errno != EEXIST)
    {
        return 0;
    }

    /*
     * Create sender directory.
     */
    if (mkdir(path, 0755) < 0 &&
        errno != EEXIST)
    {
        return 0;
    }

    return 1;
}


/* =========================================================
   PRESENCE NOTIFICATION
   ========================================================= */

void notify_presence(Client *client,
                     const char *action)
{
    char message[BUFFER_SIZE];

    int written =
        snprintf(message,
                 sizeof(message),
                 "MSG BCAST SERVER %s %s\n",
                 client->username,
                 action);

    if (written < 0 ||
        (size_t)written >= sizeof(message))
    {
        return;
    }

    int source_fd =
        client->socket_fd;

    pthread_mutex_lock(&clients_mutex);

    for (int i = 0;
         i < MAX_CLIENTS;
         i++)
    {
        if (clients[i].registered &&
            clients[i].socket_fd != source_fd)
        {
            send_all(clients[i].socket_fd,
                     message,
                     (size_t)written);
        }
    }

    pthread_mutex_unlock(&clients_mutex);
}


/* =========================================================
   REGISTER USER
   ========================================================= */

int register_user(Client *client,
                  const char *username)
{
    if (!valid_username(username))
    {
        send_response(client->socket_fd,
                      "ERR 006 INVALID_USERNAME");

        return -1;
    }

    pthread_mutex_lock(&clients_mutex);

    if (find_client_by_username(username) != NULL)
    {
        pthread_mutex_unlock(&clients_mutex);

        send_response(client->socket_fd,
                      "ERR 001 USERNAME_TAKEN");

        return -1;
    }

    snprintf(client->username,
             sizeof(client->username),
             "%s",
             username);

    client->registered = 1;

    pthread_mutex_unlock(&clients_mutex);

    char log_message[BUFFER_SIZE];

    snprintf(log_message,
             sizeof(log_message),
             "User registered: %s",
             username);

    log_event(log_message);

    char response[BUFFER_SIZE];

    snprintf(response,
             sizeof(response),
             "OK REGISTERED %s",
             username);

    if (send_response(client->socket_fd,
                      response) < 0)
    {
        return -1;
    }

    notify_presence(client,
                    "joined");

    return 0;
}


/* =========================================================
   LIST USERS
   ========================================================= */

void list_users(Client *client)
{
    char users[LIST_BUFFER_SIZE];

    users[0] = '\0';

    pthread_mutex_lock(&clients_mutex);

    int first = 1;

    for (int i = 0;
         i < MAX_CLIENTS;
         i++)
    {
        if (clients[i].registered)
        {
            if (!first)
            {
                strncat(users,
                        ",",
                        sizeof(users) -
                            strlen(users) -
                            1);
            }

            strncat(users,
                    clients[i].username,
                    sizeof(users) -
                        strlen(users) -
                        1);

            first = 0;
        }
    }

    pthread_mutex_unlock(&clients_mutex);

    char response[BUFFER_SIZE + 32];

    if (users[0] == '\0')
    {
        snprintf(response,
                 sizeof(response),
                 "OK USERS");
    }
    else
    {
        snprintf(response,
                 sizeof(response),
                 "OK USERS %s",
                 users);
    }

    send_response(client->socket_fd,
                  response);

    log_event("LIST command executed");
}


/* =========================================================
   BROADCAST MESSAGE
   ========================================================= */

void broadcast_message(Client *sender,
                        const char *message)
{
    if (message == NULL ||
        *message == '\0')
    {
        send_response(sender->socket_fd,
                      "ERR 008 INVALID_COMMAND");

        return;
    }

    char outgoing[BUFFER_SIZE];

    int written =
        snprintf(outgoing,
                 sizeof(outgoing),
                 "MSG BCAST %s %s\n",
                 sender->username,
                 message);

    if (written < 0 ||
        (size_t)written >= sizeof(outgoing))
    {
        send_response(sender->socket_fd,
                      "ERR 008 INVALID_COMMAND");

        return;
    }

    int sender_fd =
        sender->socket_fd;

    pthread_mutex_lock(&clients_mutex);

    for (int i = 0;
         i < MAX_CLIENTS;
         i++)
    {
        if (clients[i].registered &&
            clients[i].socket_fd != sender_fd)
        {
            send_all(clients[i].socket_fd,
                     outgoing,
                     (size_t)written);
        }
    }

    pthread_mutex_unlock(&clients_mutex);

    send_response(sender->socket_fd,
                  "OK SENT");

    char log_message[BUFFER_SIZE];

    snprintf(log_message,
             sizeof(log_message),
             "Broadcast from %s: %s",
             sender->username,
             message);

    log_event(log_message);
}


/* =========================================================
   PRIVATE MESSAGE
   ========================================================= */

void private_message(Client *sender,
                     const char *target_username,
                     const char *message)
{
    if (!valid_username(target_username) ||
        message == NULL ||
        *message == '\0')
    {
        send_response(sender->socket_fd,
                      "ERR 008 INVALID_COMMAND");

        return;
    }

    int target_fd = -1;

    pthread_mutex_lock(&clients_mutex);

    Client *target =
        find_client_by_username(target_username);

    if (target != NULL)
    {
        target_fd =
            target->socket_fd;
    }

    pthread_mutex_unlock(&clients_mutex);

    if (target_fd == -1)
    {
        send_response(sender->socket_fd,
                      "ERR 002 USER_NOT_FOUND");

        char log_message[BUFFER_SIZE];

        snprintf(log_message,
                 sizeof(log_message),
                 "Private message failed: %s -> %s",
                 sender->username,
                 target_username);

        log_event(log_message);

        return;
    }

    char outgoing[BUFFER_SIZE];

    int written =
        snprintf(outgoing,
                 sizeof(outgoing),
                 "MSG PRIV %s %s\n",
                 sender->username,
                 message);

    if (written < 0 ||
        (size_t)written >= sizeof(outgoing))
    {
        send_response(sender->socket_fd,
                      "ERR 008 INVALID_COMMAND");

        return;
    }

    send_all(target_fd,
             outgoing,
             (size_t)written);

    send_response(sender->socket_fd,
                  "OK SENT");

    char log_message[BUFFER_SIZE];

    snprintf(log_message,
             sizeof(log_message),
             "Private message: %s -> %s: %s",
             sender->username,
             target_username,
             message);

    log_event(log_message);
}


/* =========================================================
   JOIN ROOM
   ========================================================= */

void join_room(Client *client,
               const char *room_name)
{
    if (!valid_room_name(room_name))
    {
        send_response(client->socket_fd,
                      "ERR 008 INVALID_COMMAND");

        return;
    }

    pthread_mutex_lock(&clients_mutex);

    int room_index =
        find_room_index(room_name);

    /*
     * Create room if it does not exist.
     */
    if (room_index == -1)
    {
        int free_index = -1;

        for (int i = 0;
             i < MAX_ROOMS;
             i++)
        {
            if (!rooms[i].active)
            {
                free_index = i;
                break;
            }
        }

        if (free_index == -1)
        {
            pthread_mutex_unlock(&clients_mutex);

            send_response(client->socket_fd,
                          "ERR 010 ROOM_LIMIT");

            return;
        }

        room_index =
            free_index;

        rooms[room_index].active =
            1;

        snprintf(rooms[room_index].name,
                 sizeof(rooms[room_index].name),
                 "%s",
                 room_name);
    }

    client->joined_rooms[room_index] =
        1;

    pthread_mutex_unlock(&clients_mutex);

    char response[BUFFER_SIZE];

    snprintf(response,
             sizeof(response),
             "OK JOINED %s",
             room_name);

    send_response(client->socket_fd,
                  response);

    char log_message[BUFFER_SIZE];

    snprintf(log_message,
             sizeof(log_message),
             "User %s joined room %s",
             client->username,
             room_name);

    log_event(log_message);
}


/* =========================================================
   LEAVE ROOM
   ========================================================= */

void leave_room(Client *client,
                const char *room_name)
{
    if (!valid_room_name(room_name))
    {
        send_response(client->socket_fd,
                      "ERR 008 INVALID_COMMAND");

        return;
    }

    pthread_mutex_lock(&clients_mutex);

    int room_index =
        find_room_index(room_name);

    if (room_index == -1)
    {
        pthread_mutex_unlock(&clients_mutex);

        send_response(client->socket_fd,
                      "ERR 003 ROOM_NOT_FOUND");

        return;
    }

    if (!client->joined_rooms[room_index])
    {
        pthread_mutex_unlock(&clients_mutex);

        send_response(client->socket_fd,
                      "ERR 009 NOT_IN_ROOM");

        return;
    }

    client->joined_rooms[room_index] =
        0;

    pthread_mutex_unlock(&clients_mutex);

    char response[BUFFER_SIZE];

    snprintf(response,
             sizeof(response),
             "OK LEFT %s",
             room_name);

    send_response(client->socket_fd,
                  response);

    char log_message[BUFFER_SIZE];

    snprintf(log_message,
             sizeof(log_message),
             "User %s left room %s",
             client->username,
             room_name);

    log_event(log_message);
}


/* =========================================================
   LIST ROOMS
   ========================================================= */

void list_rooms(Client *client)
{
    char room_list[LIST_BUFFER_SIZE];

    room_list[0] = '\0';

    pthread_mutex_lock(&clients_mutex);

    int first = 1;

    for (int i = 0;
         i < MAX_ROOMS;
         i++)
    {
        if (rooms[i].active)
        {
            if (!first)
            {
                strncat(room_list,
                        ",",
                        sizeof(room_list) -
                            strlen(room_list) -
                            1);
            }

            strncat(room_list,
                    rooms[i].name,
                    sizeof(room_list) -
                        strlen(room_list) -
                        1);

            first = 0;
        }
    }

    pthread_mutex_unlock(&clients_mutex);

    char response[BUFFER_SIZE + 32];

    if (room_list[0] == '\0')
    {
        snprintf(response,
                 sizeof(response),
                 "OK ROOMS");
    }
    else
    {
        snprintf(response,
                 sizeof(response),
                 "OK ROOMS %s",
                 room_list);
    }

    send_response(client->socket_fd,
                  response);

    log_event("ROOMS command executed");
}


/* =========================================================
   ROOM MESSAGE
   ========================================================= */

void room_message(Client *sender,
                  const char *room_name,
                  const char *message)
{
    if (!valid_room_name(room_name) ||
        message == NULL ||
        *message == '\0')
    {
        send_response(sender->socket_fd,
                      "ERR 008 INVALID_COMMAND");

        return;
    }

    pthread_mutex_lock(&clients_mutex);

    int room_index =
        find_room_index(room_name);

    if (room_index == -1)
    {
        pthread_mutex_unlock(&clients_mutex);

        send_response(sender->socket_fd,
                      "ERR 003 ROOM_NOT_FOUND");

        return;
    }

    /*
     * Sender must belong to the room.
     */
    if (!sender->joined_rooms[room_index])
    {
        pthread_mutex_unlock(&clients_mutex);

        send_response(sender->socket_fd,
                      "ERR 009 NOT_IN_ROOM");

        return;
    }

    char outgoing[BUFFER_SIZE];

    int written =
        snprintf(outgoing,
                 sizeof(outgoing),
                 "MSG ROOM %s %s %s\n",
                 room_name,
                 sender->username,
                 message);

    if (written < 0 ||
        (size_t)written >= sizeof(outgoing))
    {
        pthread_mutex_unlock(&clients_mutex);

        send_response(sender->socket_fd,
                      "ERR 008 INVALID_COMMAND");

        return;
    }

    /*
     * Send to all room members.
     * Sender is included because sender is a member.
     */
    for (int i = 0;
         i < MAX_CLIENTS;
         i++)
    {
        if (clients[i].registered &&
            clients[i].joined_rooms[room_index])
        {
            send_all(clients[i].socket_fd,
                     outgoing,
                     (size_t)written);
        }
    }

    pthread_mutex_unlock(&clients_mutex);

    send_response(sender->socket_fd,
                  "OK SENT");

    char log_message[BUFFER_SIZE];

    snprintf(log_message,
             sizeof(log_message),
             "Room message: %s -> %s: %s",
             sender->username,
             room_name,
             message);

    log_event(log_message);
}


/* =========================================================
   SENDFILE
   ========================================================= */

int handle_sendfile(Client *sender,
                    const char *command)
{
    char target_name[MAX_USERNAME];

    char filename[256];

    unsigned long long filesize_ull;

    int parsed =
        sscanf(command,
               "SENDFILE %31s %255s %llu",
               target_name,
               filename,
               &filesize_ull);

    if (parsed != 3)
    {
        send_response(sender->socket_fd,
                      "ERR 008 INVALID_COMMAND");

        return 0;
    }

    /*
     * Check size first.
     */
    if (filesize_ull >
        MAX_FILE_SIZE)
    {
        send_response(sender->socket_fd,
                      "ERR 004 FILE_TOO_LARGE");

        /*
         * Close the connection because raw file bytes
         * may still follow this header.
         */
        return -1;
    }

    /*
     * Validate filename.
     */
    char safe_name[256];

    if (!safe_filename(filename,
                       safe_name,
                       sizeof(safe_name)))
    {
        send_response(sender->socket_fd,
                      "ERR 008 INVALID_FILENAME");

        return -1;
    }

    size_t filesize =
        (size_t)filesize_ull;

    /*
     * Determine the target.
     */
    int recipient_fds[MAX_CLIENTS];

    int recipient_count = 0;

    pthread_mutex_lock(&clients_mutex);

    /*
     * First try username.
     */
    Client *target_user =
        find_client_by_username(target_name);

    if (target_user != NULL)
    {
        recipient_fds[recipient_count++] =
            target_user->socket_fd;
    }
    else
    {
        /*
         * Otherwise try room.
         */
        int room_index =
            find_room_index(target_name);

        if (room_index == -1)
        {
            pthread_mutex_unlock(&clients_mutex);

            /*
             * Consume the raw file bytes so the TCP
             * stream remains synchronized.
             */
            if (discard_exact(sender,
                              filesize) != 1)
            {
                return -1;
            }

            send_response(sender->socket_fd,
                          "ERR 002 USER_NOT_FOUND");

            char log_message[BUFFER_SIZE];

            snprintf(log_message,
                     sizeof(log_message),
                     "File target not found: %s",
                     target_name);

            log_event(log_message);

            return 0;
        }

        /*
         * Sender must belong to the room.
         */
        if (!sender->joined_rooms[room_index])
        {
            pthread_mutex_unlock(&clients_mutex);

            if (discard_exact(sender,
                              filesize) != 1)
            {
                return -1;
            }

            send_response(sender->socket_fd,
                          "ERR 009 NOT_IN_ROOM");

            return 0;
        }

        /*
         * Collect room members.
         */
        for (int i = 0;
             i < MAX_CLIENTS;
             i++)
        {
            if (clients[i].registered &&
                clients[i].joined_rooms[room_index])
            {
                recipient_fds[recipient_count++] =
                    clients[i].socket_fd;
            }
        }
    }

    pthread_mutex_unlock(&clients_mutex);


    /* =====================================================
       ALLOCATE FILE BUFFER
       ===================================================== */

    unsigned char *file_data =
        malloc(filesize > 0 ? filesize : 1);

    if (file_data == NULL)
    {
        /*
         * Drain incoming file bytes.
         */
        if (discard_exact(sender,
                          filesize) != 1)
        {
            return -1;
        }

        send_response(sender->socket_fd,
                      "ERR 011 MEMORY_ERROR");

        return 0;
    }


    /* =====================================================
       RECEIVE EXACTLY FILESIZE BYTES
       ===================================================== */

    int receive_result =
        receive_exact(sender,
                      file_data,
                      filesize);

    if (receive_result != 1)
    {
        free(file_data);

        char log_message[BUFFER_SIZE];

        snprintf(log_message,
                 sizeof(log_message),
                 "Incomplete file transfer from %s",
                 sender->username);

        log_event(log_message);

        return -1;
    }


    /* =====================================================
       CREATE SENDER STORAGE
       ===================================================== */

    char storage_dir[512];

    if (!create_user_storage(sender->username,
                             storage_dir,
                             sizeof(storage_dir)))
    {
        free(file_data);

        send_response(sender->socket_fd,
                      "ERR 011 STORAGE_ERROR");

        return 0;
    }


    /* =====================================================
       BUILD SERVER STORAGE PATH
       ===================================================== */

    char storage_path[768];

    int path_written =
        snprintf(storage_path,
                 sizeof(storage_path),
                 "%s/%s",
                 storage_dir,
                 safe_name);

    if (path_written < 0 ||
        (size_t)path_written >=
            sizeof(storage_path))
    {
        free(file_data);

        send_response(sender->socket_fd,
                      "ERR 011 STORAGE_ERROR");

        return 0;
    }


    /* =====================================================
       SAVE SERVER COPY
       ===================================================== */

    FILE *fp =
        fopen(storage_path, "wb");

    if (fp == NULL)
    {
        free(file_data);

        send_response(sender->socket_fd,
                      "ERR 011 STORAGE_ERROR");

        return 0;
    }


    if (filesize > 0)
    {
        size_t written_bytes =
            fwrite(file_data,
                   1,
                   filesize,
                   fp);

        if (written_bytes != filesize)
        {
            fclose(fp);

            free(file_data);

            send_response(sender->socket_fd,
                          "ERR 011 STORAGE_ERROR");

            return 0;
        }
    }

    fclose(fp);


    /* =====================================================
       BUILD FORWARD HEADER
       ===================================================== */

    char forward_header[BUFFER_SIZE];

    int header_written =
        snprintf(forward_header,
                 sizeof(forward_header),
                 "SENDFILE %s %s %llu\n",
                 target_name,
                 safe_name,
                 filesize_ull);

    if (header_written < 0 ||
        (size_t)header_written >=
            sizeof(forward_header))
    {
        free(file_data);

        send_response(sender->socket_fd,
                      "ERR 008 INVALID_COMMAND");

        return 0;
    }


    /* =====================================================
       FORWARD FILE
       ===================================================== */

    for (int i = 0;
         i < recipient_count;
         i++)
    {
        if (send_all(recipient_fds[i],
                     forward_header,
                     (size_t)header_written) < 0)
        {
            continue;
        }

        if (filesize > 0)
        {
            if (send_all(recipient_fds[i],
                         (const char *)file_data,
                         filesize) < 0)
            {
                continue;
            }
        }
    }


    /* =====================================================
       LOG FILE TRANSFER
       ===================================================== */

    char log_message[BUFFER_SIZE];

    snprintf(log_message,
             sizeof(log_message),
             "File transfer: %s -> %s: %s (%llu bytes)",
             sender->username,
             target_name,
             safe_name,
             filesize_ull);

    log_event(log_message);


    /* =====================================================
       CONFIRM TO SENDER
       ===================================================== */

    char response[BUFFER_SIZE];

    snprintf(response,
             sizeof(response),
             "OK FILE_RECEIVED %s",
             safe_name);

    send_response(sender->socket_fd,
                  response);

    free(file_data);

    return 0;
}


/* =========================================================
   REMOVE CLIENT
   ========================================================= */

void remove_client(Client *client)
{
    char username[MAX_USERNAME];

    username[0] = '\0';

    int was_registered = 0;

    int disconnected_fd = -1;

    pthread_mutex_lock(&clients_mutex);

    was_registered =
        client->registered;

    disconnected_fd =
        client->socket_fd;

    if (was_registered)
    {
        snprintf(username,
                 sizeof(username),
                 "%s",
                 client->username);
    }

    /*
     * Clear room memberships.
     */
    for (int i = 0;
         i < MAX_ROOMS;
         i++)
    {
        client->joined_rooms[i] =
            0;
    }

    client->registered = 0;

    client->username[0] =
        '\0';

    client->recv_len =
        0;

    /*
     * Mark slot free.
     */
    client->socket_fd =
        -1;

    pthread_mutex_unlock(&clients_mutex);


    if (!was_registered)
    {
        return;
    }


    /* Log disconnection. */
    char log_message[BUFFER_SIZE];

    snprintf(log_message,
             sizeof(log_message),
             "User disconnected: %s",
             username);

    log_event(log_message);


    /* Notify remaining users. */
    char message[BUFFER_SIZE];

    int written =
        snprintf(message,
                 sizeof(message),
                 "MSG BCAST SERVER %s left\n",
                 username);

    if (written < 0 ||
        (size_t)written >= sizeof(message))
    {
        return;
    }

    pthread_mutex_lock(&clients_mutex);

    for (int i = 0;
         i < MAX_CLIENTS;
         i++)
    {
        if (clients[i].registered &&
            clients[i].socket_fd !=
                disconnected_fd)
        {
            send_all(clients[i].socket_fd,
                     message,
                     (size_t)written);
        }
    }

    pthread_mutex_unlock(&clients_mutex);
}


/* =========================================================
   CLIENT THREAD
   ========================================================= */

void *handle_client(void *arg)
{
    Client *client =
        (Client *)arg;

    int client_fd =
        client->socket_fd;

    char log_message[BUFFER_SIZE];

    snprintf(log_message,
             sizeof(log_message),
             "Client connected: socket=%d",
             client_fd);

    log_event(log_message);

    printf("[SERVER] Client connected: socket=%d\n",
           client_fd);


    while (1)
    {
        char line[BUFFER_SIZE];

        int result =
            receive_line(client,
                         line,
                         sizeof(line));


        /* =================================================
           DISCONNECT
           ================================================= */

        if (result == 0)
        {
            printf("[SERVER] Client disconnected: socket=%d\n",
                   client_fd);

            break;
        }


        /* =================================================
           RECEIVE ERROR
           ================================================= */

        if (result == -1)
        {
            perror("[SERVER] recv");
            break;
        }


        /* =================================================
           LINE TOO LONG
           ================================================= */

        if (result == -2)
        {
            send_response(client->socket_fd,
                          "ERR 005 LINE_TOO_LONG");

            break;
        }


        /*
         * Remove CR/LF.
         */
        line[strcspn(line,
                     "\r\n")] = '\0';


        /*
         * Ignore empty lines.
         */
        if (line[0] == '\0')
        {
            continue;
        }


        printf("[SERVER] socket=%d: %s\n",
               client_fd,
               line);


        /* =================================================
           REGISTER MUST BE FIRST
           ================================================= */

        if (!client->registered)
        {
            if (strncmp(line,
                        "REGISTER ",
                        9) == 0)
            {
                if (register_user(client,
                                  line + 9) < 0)
                {
                    break;
                }

                continue;
            }

            send_response(client->socket_fd,
                          "ERR 005 NOT_REGISTERED");

            continue;
        }


        /* =================================================
           REGISTER AGAIN
           ================================================= */

        if (strncmp(line,
                    "REGISTER",
                    8) == 0)
        {
            send_response(client->socket_fd,
                          "ERR 007 ALREADY_REGISTERED");

            continue;
        }


        /* =================================================
           LIST
           ================================================= */

        if (strcmp(line,
                   "LIST") == 0)
        {
            list_users(client);
            continue;
        }


        /* =================================================
           BCAST
           ================================================= */

        if (strncmp(line,
                    "BCAST ",
                    6) == 0)
        {
            broadcast_message(client,
                              line + 6);

            continue;
        }


        /* =================================================
           PMSG
           ================================================= */

        if (strncmp(line,
                    "PMSG ",
                    5) == 0)
        {
            char *data =
                line + 5;

            char *separator =
                strchr(data,
                       ' ');

            if (separator == NULL ||
                separator == data ||
                *(separator + 1) == '\0')
            {
                send_response(client->socket_fd,
                              "ERR 008 INVALID_COMMAND");

                continue;
            }

            size_t target_length =
                (size_t)(separator - data);

            if (target_length >=
                MAX_USERNAME)
            {
                send_response(client->socket_fd,
                              "ERR 006 INVALID_USERNAME");

                continue;
            }

            char target[MAX_USERNAME];

            memcpy(target,
                   data,
                   target_length);

            target[target_length] =
                '\0';

            private_message(client,
                            target,
                            separator + 1);

            continue;
        }


        /* =================================================
           JOIN
           ================================================= */

        if (strncmp(line,
                    "JOIN ",
                    5) == 0)
        {
            join_room(client,
                      line + 5);

            continue;
        }


        /* =================================================
           LEAVE
           ================================================= */

        if (strncmp(line,
                    "LEAVE ",
                    6) == 0)
        {
            leave_room(client,
                       line + 6);

            continue;
        }


        /* =================================================
           ROOMS
           ================================================= */

        if (strcmp(line,
                   "ROOMS") == 0)
        {
            list_rooms(client);

            continue;
        }


        /* =================================================
           RMSG
           ================================================= */

        if (strncmp(line,
                    "RMSG ",
                    5) == 0)
        {
            char *data =
                line + 5;

            char *separator =
                strchr(data,
                       ' ');

            if (separator == NULL ||
                separator == data ||
                *(separator + 1) == '\0')
            {
                send_response(client->socket_fd,
                              "ERR 008 INVALID_COMMAND");

                continue;
            }

            size_t room_length =
                (size_t)(separator - data);

            if (room_length >=
                MAX_ROOM_NAME)
            {
                send_response(client->socket_fd,
                              "ERR 008 INVALID_COMMAND");

                continue;
            }

            char room_name[MAX_ROOM_NAME];

            memcpy(room_name,
                   data,
                   room_length);

            room_name[room_length] =
                '\0';

            room_message(client,
                         room_name,
                         separator + 1);

            continue;
        }


        /* =================================================
           SENDFILE
           ================================================= */

        if (strncmp(line,
                    "SENDFILE ",
                    9) == 0)
        {
            int transfer_result =
                handle_sendfile(client,
                                line);

            /*
             * -1 means connection should be closed.
             */
            if (transfer_result < 0)
            {
                break;
            }

            continue;
        }


        /* =================================================
           QUIT
           ================================================= */

        if (strcmp(line,
                   "QUIT") == 0)
        {
            send_response(client->socket_fd,
                          "OK BYE");

            log_event(
                "Client requested QUIT");

            break;
        }


        /* =================================================
           UNKNOWN COMMAND
           ================================================= */

        send_response(client->socket_fd,
                      "ERR 008 UNKNOWN_COMMAND");
    }


    /* =====================================================
       CLEANUP
       ===================================================== */

    int close_fd =
        client->socket_fd;

    remove_client(client);

    if (close_fd >= 0)
    {
        close(close_fd);
    }

    return NULL;
}


/* =========================================================
   MAIN
   ========================================================= */

int main(void)
{
    /*
     * Prevent the server from terminating if it sends
     * to a socket that has already been closed.
     */
    signal(SIGPIPE,
           SIG_IGN);


    /* =====================================================
       INITIALIZE CLIENT TABLE
       ===================================================== */

    for (int i = 0;
         i < MAX_CLIENTS;
         i++)
    {
        clients[i].socket_fd =
            -1;

        clients[i].registered =
            0;

        clients[i].username[0] =
            '\0';

        clients[i].recv_len =
            0;

        for (int j = 0;
             j < MAX_ROOMS;
             j++)
        {
            clients[i].joined_rooms[j] =
                0;
        }
    }


    /* =====================================================
       INITIALIZE ROOM TABLE
       ===================================================== */

    for (int i = 0;
         i < MAX_ROOMS;
         i++)
    {
        rooms[i].active =
            0;

        rooms[i].name[0] =
            '\0';
    }


    /* =====================================================
       CREATE SOCKET
       ===================================================== */

    int server_fd =
        socket(AF_INET,
               SOCK_STREAM,
               0);

    if (server_fd < 0)
    {
        perror("socket");
        return EXIT_FAILURE;
    }


    /* =====================================================
       SO_REUSEADDR
       ===================================================== */

    int opt = 1;

    if (setsockopt(server_fd,
                   SOL_SOCKET,
                   SO_REUSEADDR,
                   &opt,
                   sizeof(opt)) < 0)
    {
        perror("setsockopt");

        close(server_fd);

        return EXIT_FAILURE;
    }


    /* =====================================================
       SERVER ADDRESS
       ===================================================== */

    struct sockaddr_in server_addr;

    memset(&server_addr,
           0,
           sizeof(server_addr));

    server_addr.sin_family =
        AF_INET;

    server_addr.sin_addr.s_addr =
        htonl(INADDR_ANY);

    server_addr.sin_port =
        htons(PORT);


    /* =====================================================
       BIND
       ===================================================== */

    if (bind(server_fd,
             (struct sockaddr *)&server_addr,
             sizeof(server_addr)) < 0)
    {
        perror("bind");

        close(server_fd);

        return EXIT_FAILURE;
    }


    /* =====================================================
       LISTEN
       ===================================================== */

    if (listen(server_fd,
               BACKLOG) < 0)
    {
        perror("listen");

        close(server_fd);

        return EXIT_FAILURE;
    }


    /* =====================================================
       STARTUP INFORMATION
       ===================================================== */

    printf("========================================\n");
    printf("       NetMessenger Server\n");
    printf("========================================\n");
    printf("Registration : %s\n",
           REGISTRATION_NUMBER);
    printf("Port         : %d\n",
           PORT);
    printf("NID          : %s\n",
           NID_TAG);
    printf("Log file     : %s\n",
           LOG_FILE);
    printf("Storage      : %s\n",
           STORAGE_ROOT);
    printf("Max file     : %llu bytes\n",
           (unsigned long long)
               MAX_FILE_SIZE);
    printf("========================================\n");
    printf("[SERVER] Listening for connections...\n");


    log_event(
        "NetMessenger server started");


    /* =====================================================
       ACCEPT LOOP
       ===================================================== */

    while (1)
    {
        struct sockaddr_in client_addr;

        socklen_t client_len =
            sizeof(client_addr);


        /*
         * Find a free client slot.
         */
        pthread_mutex_lock(&clients_mutex);

        Client *client = NULL;

        for (int i = 0;
             i < MAX_CLIENTS;
             i++)
        {
            if (clients[i].socket_fd == -1)
            {
                client = &clients[i];
                break;
            }
        }

        pthread_mutex_unlock(&clients_mutex);


        /*
         * Accept incoming connection.
         */
        int accepted_fd =
            accept(server_fd,
                   (struct sockaddr *)&client_addr,
                   &client_len);

        if (accepted_fd < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }

            perror("accept");
            continue;
        }


        /*
         * Server full.
         */
        if (client == NULL)
        {
            const char *full_message =
                "ERR 010 SERVER_FULL NID:7520\n";

            send_all(accepted_fd,
                     full_message,
                     strlen(full_message));

            close(accepted_fd);

            continue;
        }


        /* =================================================
           INITIALIZE CLIENT SLOT
           ================================================= */

        pthread_mutex_lock(&clients_mutex);

        client->socket_fd =
            accepted_fd;

        client->registered =
            0;

        client->username[0] =
            '\0';

        client->recv_len =
            0;

        for (int j = 0;
             j < MAX_ROOMS;
             j++)
        {
            client->joined_rooms[j] =
                0;
        }

        pthread_mutex_unlock(&clients_mutex);


        /* =================================================
           CREATE THREAD
           ================================================= */

        pthread_t thread_id;

        if (pthread_create(&thread_id,
                           NULL,
                           handle_client,
                           client) != 0)
        {
            perror("pthread_create");

            pthread_mutex_lock(&clients_mutex);

            client->socket_fd =
                -1;

            client->registered =
                0;

            client->username[0] =
                '\0';

            client->recv_len =
                0;

            for (int j = 0;
                 j < MAX_ROOMS;
                 j++)
            {
                client->joined_rooms[j] =
                    0;
            }

            pthread_mutex_unlock(&clients_mutex);

            close(accepted_fd);

            continue;
        }

        pthread_detach(thread_id);
    }


    close(server_fd);

    return 0;
}
