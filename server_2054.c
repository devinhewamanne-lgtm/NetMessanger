#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <time.h>
#include <unistd.h>

/* =========================================================
   PERSONALIZED CONFIGURATION
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

/*
 * Maximum practical size for a users/rooms list.
 * 20 clients x 31-character usernames + commas.
 */
#define LIST_BUFFER_SIZE 1024


/* =========================================================
   CLIENT STRUCTURE
   ========================================================= */

typedef struct
{
    int socket_fd;
    int registered;

    char username[MAX_USERNAME];

    /*
     * joined_rooms[i] == 1
     * means the client is a member of rooms[i].
     */
    int joined_rooms[MAX_ROOMS];

    /*
     * TCP line receive buffer.
     * This allows us to correctly handle:
     * - partial lines
     * - multiple lines in one recv()
     */
    char recv_buffer[BUFFER_SIZE];
    size_t recv_len;

} Client;


/* =========================================================
   ROOM STRUCTURE
   ========================================================= */

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

pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;


/* =========================================================
   LOGGING
   ========================================================= */

void log_event(const char *event)
{
    FILE *fp = fopen(LOG_FILE, "a");

    if (fp == NULL)
    {
        perror("[SERVER] fopen log");
        return;
    }

    time_t now = time(NULL);

    struct tm *tm_info = localtime(&now);

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
   TCP send() is not guaranteed to send everything.
   ========================================================= */

int send_all(int socket_fd,
             const char *data,
             size_t length)
{
    size_t total_sent = 0;

    while (total_sent < length)
    {
        ssize_t sent = send(socket_fd,
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

        total_sent += (size_t)sent;
    }

    return 0;
}


/* =========================================================
   SEND PERSONALIZED SERVER RESPONSE
   Every OK/ERR response gets NID:7520.
   ========================================================= */

int send_response(int socket_fd,
                  const char *response)
{
    char line[BUFFER_SIZE];

    int written = snprintf(line,
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
   Handles:
   - partial recv()
   - multiple lines in one recv()
   ========================================================= */

int receive_line(Client *client,
                 char *line,
                 size_t line_size)
{
    while (1)
    {
        /*
         * Search for '\n' already in the buffer.
         */
        for (size_t i = 0;
             i < client->recv_len;
             i++)
        {
            if (client->recv_buffer[i] == '\n')
            {
                size_t line_length = i + 1;

                if (line_length >= line_size)
                {
                    return -2;
                }

                memcpy(line,
                       client->recv_buffer,
                       line_length);

                line[line_length] = '\0';

                /*
                 * Remove the processed line.
                 */
                size_t remaining =
                    client->recv_len - line_length;

                memmove(client->recv_buffer,
                        client->recv_buffer + line_length,
                        remaining);

                client->recv_len = remaining;

                return 1;
            }
        }

        /*
         * No complete line yet.
         * Make sure the buffer does not overflow.
         */
        if (client->recv_len >=
            sizeof(client->recv_buffer) - 1)
        {
            return -2;
        }

        ssize_t received =
            recv(client->socket_fd,
                 client->recv_buffer + client->recv_len,
                 sizeof(client->recv_buffer) -
                     client->recv_len - 1,
                 0);

        if (received == 0)
        {
            /*
             * Graceful or ungraceful disconnect.
             */
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

        client->recv_len += (size_t)received;

        client->recv_buffer[client->recv_len] = '\0';
    }
}


/* =========================================================
   CHECK USERNAME
   ========================================================= */

int valid_username(const char *username)
{
    if (username == NULL)
    {
        return 0;
    }

    size_t length = strlen(username);

    if (length == 0 ||
        length >= MAX_USERNAME)
    {
        return 0;
    }

    for (size_t i = 0; i < length; i++)
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
   CHECK ROOM NAME
   ========================================================= */

int valid_room_name(const char *room_name)
{
    if (room_name == NULL)
    {
        return 0;
    }

    size_t length = strlen(room_name);

    if (length == 0 ||
        length >= MAX_ROOM_NAME)
    {
        return 0;
    }

    for (size_t i = 0; i < length; i++)
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
   Caller must hold clients_mutex.
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
   BROADCAST PRESENCE
   ========================================================= */

void notify_presence(Client *joining_client,
                     const char *action)
{
    char message[BUFFER_SIZE];

    int written = snprintf(message,
                            sizeof(message),
                            "MSG BCAST SERVER %s %s\n",
                            joining_client->username,
                            action);

    if (written < 0 ||
        (size_t)written >= sizeof(message))
    {
        return;
    }

    int joining_fd =
        joining_client->socket_fd;

    pthread_mutex_lock(&clients_mutex);

    for (int i = 0;
         i < MAX_CLIENTS;
         i++)
    {
        if (clients[i].registered &&
            clients[i].socket_fd != joining_fd)
        {
            if (send_all(clients[i].socket_fd,
                         message,
                         strlen(message)) < 0)
            {
                perror("[SERVER] presence send");
            }
        }
    }

    pthread_mutex_unlock(&clients_mutex);
}


/* =========================================================
   REGISTER USER
   REGISTER must be the first command.
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

    /*
     * Check duplicate username.
     */
    if (find_client_by_username(username) != NULL)
    {
        pthread_mutex_unlock(&clients_mutex);

        send_response(client->socket_fd,
                      "ERR 001 USERNAME_TAKEN");

        return -1;
    }

    /*
     * Register this client.
     */
    strncpy(client->username,
            username,
            MAX_USERNAME - 1);

    client->username[MAX_USERNAME - 1] = '\0';

    client->registered = 1;

    pthread_mutex_unlock(&clients_mutex);

    /*
     * Log registration.
     */
    char log_message[BUFFER_SIZE];

    snprintf(log_message,
             sizeof(log_message),
             "User registered: %s",
             username);

    log_event(log_message);

    /*
     * Registration response.
     */
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

    /*
     * Notify all other connected clients.
     */
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

    char response[BUFFER_SIZE];

    int written;

    if (users[0] == '\0')
    {
        written = snprintf(response,
                           sizeof(response),
                           "OK USERS");
    }
    else
    {
        written = snprintf(response,
                           sizeof(response),
                           "OK USERS %s",
                           users);
    }

    if (written > 0 &&
        (size_t)written < sizeof(response))
    {
        send_response(client->socket_fd,
                      response);
    }

    log_event("LIST command executed");
}


/* =========================================================
   BROADCAST MESSAGE
   ========================================================= */

void broadcast_message(Client *sender,
                        const char *message)
{
    if (message == NULL ||
        strlen(message) == 0)
    {
        send_response(sender->socket_fd,
                      "ERR 008 INVALID_COMMAND");

        return;
    }

    char outgoing[BUFFER_SIZE];

    int written = snprintf(outgoing,
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
            if (send_all(clients[i].socket_fd,
                         outgoing,
                         (size_t)written) < 0)
            {
                perror("[SERVER] broadcast send");
            }
        }
    }

    pthread_mutex_unlock(&clients_mutex);

    /*
     * Confirm to sender.
     */
    send_response(sender->socket_fd,
                  "OK SENT");

    /*
     * Log.
     */
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
        strlen(message) == 0)
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
        target_fd = target->socket_fd;
    }

    pthread_mutex_unlock(&clients_mutex);

    /*
     * Target not found.
     */
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

    /*
     * Build required private-message format.
     */
    char outgoing[BUFFER_SIZE];

    int written = snprintf(outgoing,
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

    /*
     * Send only to the target.
     */
    if (send_all(target_fd,
                 outgoing,
                 (size_t)written) < 0)
    {
        perror("[SERVER] private message send");
    }

    /*
     * Confirm to sender.
     */
    send_response(sender->socket_fd,
                  "OK SENT");

    /*
     * Log.
     */
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
   Creates room if it does not exist.
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

    int room_index = -1;
    int free_index = -1;

    /*
     * Find existing room and first free room slot.
     */
    for (int i = 0;
         i < MAX_ROOMS;
         i++)
    {
        if (rooms[i].active)
        {
            if (strcmp(rooms[i].name,
                       room_name) == 0)
            {
                room_index = i;
                break;
            }
        }
        else if (free_index == -1)
        {
            free_index = i;
        }
    }

    /*
     * Room does not exist.
     * Create it.
     */
    if (room_index == -1)
    {
        if (free_index == -1)
        {
            pthread_mutex_unlock(&clients_mutex);

            send_response(client->socket_fd,
                          "ERR 010 ROOM_LIMIT");

            return;
        }

        room_index = free_index;

        rooms[room_index].active = 1;

        strncpy(rooms[room_index].name,
                room_name,
                MAX_ROOM_NAME - 1);

        rooms[room_index].name[MAX_ROOM_NAME - 1] =
            '\0';
    }

    /*
     * Add client to room.
     */
    client->joined_rooms[room_index] = 1;

    pthread_mutex_unlock(&clients_mutex);

    /*
     * Response.
     */
    char response[BUFFER_SIZE];

    snprintf(response,
             sizeof(response),
             "OK JOINED %s",
             room_name);

    send_response(client->socket_fd,
                  response);

    /*
     * Log.
     */
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

    int room_index = -1;

    for (int i = 0;
         i < MAX_ROOMS;
         i++)
    {
        if (rooms[i].active &&
            strcmp(rooms[i].name,
                   room_name) == 0)
        {
            room_index = i;
            break;
        }
    }

    /*
     * Room does not exist.
     */
    if (room_index == -1)
    {
        pthread_mutex_unlock(&clients_mutex);

        send_response(client->socket_fd,
                      "ERR 003 ROOM_NOT_FOUND");

        return;
    }

    /*
     * Client is not a member.
     */
    if (client->joined_rooms[room_index] == 0)
    {
        pthread_mutex_unlock(&clients_mutex);

        send_response(client->socket_fd,
                      "ERR 009 NOT_IN_ROOM");

        return;
    }

    /*
     * Remove membership.
     */
    client->joined_rooms[room_index] = 0;

    pthread_mutex_unlock(&clients_mutex);

    /*
     * Response.
     */
    char response[BUFFER_SIZE];

    snprintf(response,
             sizeof(response),
             "OK LEFT %s",
             room_name);

    send_response(client->socket_fd,
                  response);

    /*
     * Log.
     */
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

    char response[BUFFER_SIZE];

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
   REMOVE CLIENT
   Cleans user and room membership.
   ========================================================= */

void remove_client(Client *client)
{
    char username[MAX_USERNAME];

    username[0] = '\0';

    int was_registered = 0;

    int disconnected_fd = -1;

    pthread_mutex_lock(&clients_mutex);

    was_registered = client->registered;

    disconnected_fd = client->socket_fd;

    if (was_registered)
    {
        strncpy(username,
                client->username,
                MAX_USERNAME - 1);

        username[MAX_USERNAME - 1] = '\0';
    }

    /*
     * Remove all room memberships.
     */
    for (int i = 0;
         i < MAX_ROOMS;
         i++)
    {
        client->joined_rooms[i] = 0;
    }

    client->registered = 0;
    client->username[0] = '\0';
    client->recv_len = 0;

    /*
     * Mark slot as free.
     */
    client->socket_fd = -1;

    pthread_mutex_unlock(&clients_mutex);

    if (!was_registered)
    {
        return;
    }

    /*
     * Log disconnection.
     */
    char log_message[BUFFER_SIZE];

    snprintf(log_message,
             sizeof(log_message),
             "User disconnected: %s",
             username);

    log_event(log_message);

    /*
     * Notify remaining clients.
     */
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
            clients[i].socket_fd != disconnected_fd)
        {
            if (send_all(clients[i].socket_fd,
                         message,
                         (size_t)written) < 0)
            {
                perror("[SERVER] leave notification");
            }
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

    char client_address[INET_ADDRSTRLEN];

    snprintf(client_address,
             sizeof(client_address),
             "unknown");

    /*
     * We don't have the sockaddr structure here,
     * so socket number is sufficient for this log.
     */
    char connect_log[BUFFER_SIZE];

    snprintf(connect_log,
             sizeof(connect_log),
             "Client connected: socket=%d",
             client_fd);

    log_event(connect_log);

    printf("[SERVER] Client connected: socket=%d\n",
           client_fd);

    while (1)
    {
        char line[BUFFER_SIZE];

        int result =
            receive_line(client,
                         line,
                         sizeof(line));

        /*
         * Client disconnected.
         */
        if (result == 0)
        {
            printf("[SERVER] Client disconnected: socket=%d\n",
                   client_fd);

            break;
        }

        /*
         * Receive error.
         */
        if (result == -1)
        {
            perror("[SERVER] recv");
            break;
        }

        /*
         * Line too long.
         */
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
                const char *username =
                    line + 9;

                if (register_user(client,
                                  username) < 0)
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
           LIST USERS
           ================================================= */

        if (strcmp(line,
                   "LIST") == 0)
        {
            list_users(client);

            continue;
        }


        /* =================================================
           BROADCAST
           ================================================= */

        if (strncmp(line,
                    "BCAST ",
                    6) == 0)
        {
            const char *message =
                line + 6;

            if (strlen(message) == 0)
            {
                send_response(client->socket_fd,
                              "ERR 008 INVALID_COMMAND");

                continue;
            }

            broadcast_message(client,
                              message);

            continue;
        }


        /* =================================================
           PRIVATE MESSAGE
           ================================================= */

        if (strncmp(line,
                    "PMSG ",
                    5) == 0)
        {
            char *command_data =
                line + 5;

            /*
             * Find space between username and message.
             */
            char *separator =
                strchr(command_data,
                       ' ');

            if (separator == NULL ||
                separator == command_data ||
                *(separator + 1) == '\0')
            {
                send_response(client->socket_fd,
                              "ERR 008 INVALID_COMMAND");

                continue;
            }

            /*
             * Determine username length.
             */
            size_t username_length =
                (size_t)(separator -
                         command_data);

            if (username_length >=
                MAX_USERNAME)
            {
                send_response(client->socket_fd,
                              "ERR 006 INVALID_USERNAME");

                continue;
            }

            char target_username[MAX_USERNAME];

            memcpy(target_username,
                   command_data,
                   username_length);

            target_username[username_length] =
                '\0';

            const char *message =
                separator + 1;

            private_message(client,
                            target_username,
                            message);

            continue;
        }


        /* =================================================
           JOIN ROOM
           ================================================= */

        if (strncmp(line,
                    "JOIN ",
                    5) == 0)
        {
            const char *room_name =
                line + 5;

            join_room(client,
                      room_name);

            continue;
        }


        /* =================================================
           LEAVE ROOM
           ================================================= */

        if (strncmp(line,
                    "LEAVE ",
                    6) == 0)
        {
            const char *room_name =
                line + 6;

            leave_room(client,
                       room_name);

            continue;
        }


        /* =================================================
           LIST ROOMS
           ================================================= */

        if (strcmp(line,
                   "ROOMS") == 0)
        {
            list_rooms(client);

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

            log_event("Client requested QUIT");

            break;
        }


        /* =================================================
           UNKNOWN COMMAND
           ================================================= */

        send_response(client->socket_fd,
                      "ERR 008 UNKNOWN_COMMAND");
    }


    /*
     * Save descriptor before remove_client()
     * sets socket_fd = -1.
     */
    int close_fd =
        client->socket_fd;

    remove_client(client);

    /*
     * Close the real socket.
     */
    if (close_fd >= 0)
    {
        close(close_fd);
    }

    return NULL;
}


/* =========================================================
   MAIN SERVER
   ========================================================= */

int main(void)
{
    int server_fd;

    struct sockaddr_in server_addr;


    /* =====================================================
       INITIALIZE CLIENT TABLE
       ===================================================== */

    for (int i = 0;
         i < MAX_CLIENTS;
         i++)
    {
        clients[i].socket_fd = -1;
        clients[i].registered = 0;
        clients[i].username[0] = '\0';
        clients[i].recv_len = 0;

        for (int j = 0;
             j < MAX_ROOMS;
             j++)
        {
            clients[i].joined_rooms[j] = 0;
        }
    }


    /* =====================================================
       INITIALIZE ROOM TABLE
       ===================================================== */

    for (int i = 0;
         i < MAX_ROOMS;
         i++)
    {
        rooms[i].active = 0;
        rooms[i].name[0] = '\0';
    }


    /* =====================================================
       CREATE TCP SOCKET
       ===================================================== */

    server_fd =
        socket(AF_INET,
               SOCK_STREAM,
               0);

    if (server_fd < 0)
    {
        perror("socket");
        exit(EXIT_FAILURE);
    }


    /* =====================================================
       ALLOW QUICK RESTART
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

        exit(EXIT_FAILURE);
    }


    /* =====================================================
       SERVER ADDRESS
       ===================================================== */

    memset(&server_addr,
           0,
           sizeof(server_addr));

    server_addr.sin_family =
        AF_INET;

    server_addr.sin_addr.s_addr =
        INADDR_ANY;

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

        exit(EXIT_FAILURE);
    }


    /* =====================================================
       LISTEN
       ===================================================== */

    if (listen(server_fd,
               BACKLOG) < 0)
    {
        perror("listen");

        close(server_fd);

        exit(EXIT_FAILURE);
    }


    /* =====================================================
       SERVER START MESSAGE
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
    printf("========================================\n");
    printf("[SERVER] Listening for connections...\n");


    log_event("NetMessenger server started");


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
         *
         * Only the main accept thread modifies the
         * socket assignment, so this is safe here.
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
         * Server full.
         */
        if (client == NULL)
        {
            int temp_fd =
                accept(server_fd,
                       (struct sockaddr *)&client_addr,
                       &client_len);

            if (temp_fd >= 0)
            {
                const char *full_message =
                    "ERR 010 SERVER_FULL NID:7520\n";

                send_all(temp_fd,
                         full_message,
                         strlen(full_message));

                close(temp_fd);
            }

            continue;
        }


        /*
         * Accept new TCP connection.
         */
        int accepted_fd =
            accept(server_fd,
                   (struct sockaddr *)&client_addr,
                   &client_len);

        if (accepted_fd < 0)
        {
            perror("accept");
            continue;
        }


        /*
         * Initialize the selected client slot.
         */
        pthread_mutex_lock(&clients_mutex);

        client->socket_fd = accepted_fd;

        client->registered = 0;

        client->username[0] = '\0';

        client->recv_len = 0;

        for (int j = 0;
             j < MAX_ROOMS;
             j++)
        {
            client->joined_rooms[j] = 0;
        }

        pthread_mutex_unlock(&clients_mutex);


        /*
         * Create client thread.
         */
        pthread_t thread_id;

        if (pthread_create(&thread_id,
                           NULL,
                           handle_client,
                           client) != 0)
        {
            perror("pthread_create");

            pthread_mutex_lock(&clients_mutex);

            client->socket_fd = -1;
            client->registered = 0;
            client->username[0] = '\0';
            client->recv_len = 0;

            for (int j = 0;
                 j < MAX_ROOMS;
                 j++)
            {
                client->joined_rooms[j] = 0;
            }

            pthread_mutex_unlock(&clients_mutex);

            close(accepted_fd);

            continue;
        }

        /*
         * We don't need to join the thread because
         * each client thread cleans itself up.
         */
        pthread_detach(thread_id);
    }


    close(server_fd);

    return 0;
}
