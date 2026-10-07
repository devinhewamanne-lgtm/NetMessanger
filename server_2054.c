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

#define PORT 8054
#define BACKLOG 10
#define MAX_CLIENTS 20
#define MAX_USERNAME 32
#define BUFFER_SIZE 4096

#define LOG_FILE "netmsg_IT23752054.log"

typedef struct
{
    int socket_fd;
    int registered;
    char username[MAX_USERNAME];

    char recv_buffer[BUFFER_SIZE];
    size_t recv_len;

} Client;

Client clients[MAX_CLIENTS];

pthread_mutex_t clients_mutex = PTHREAD_MUTEX_INITIALIZER;


/* =========================================================
   LOGGING
   ========================================================= */

void log_event(const char *event)
{
    FILE *fp = fopen(LOG_FILE, "a");

    if (fp == NULL)
    {
        perror("fopen log");
        return;
    }

    time_t now = time(NULL);
    struct tm *tm_info = localtime(&now);

    char timestamp[64];

    strftime(timestamp,
             sizeof(timestamp),
             "%Y-%m-%d %H:%M:%S",
             tm_info);

    fprintf(fp, "[%s] %s\n", timestamp, event);

    fclose(fp);
}


/* =========================================================
   SEND ALL
   Ensures the complete message is transmitted.
   ========================================================= */

int send_all(int socket_fd, const char *data, size_t length)
{
    size_t total_sent = 0;

    while (total_sent < length)
    {
        ssize_t sent = send(socket_fd,
                            data + total_sent,
                            length - total_sent,
                            0);

        if (sent <= 0)
        {
            return -1;
        }

        total_sent += (size_t)sent;
    }

    return 0;
}


/* =========================================================
   SEND PERSONALIZED SERVER RESPONSE
   ========================================================= */

int send_response(int socket_fd, const char *response)
{
    char line[BUFFER_SIZE];

    snprintf(line,
             sizeof(line),
             "%s NID:7520\n",
             response);

    return send_all(socket_fd,
                    line,
                    strlen(line));
}


/* =========================================================
   RECEIVE ONE LINE
   Handles:
   - partial recv()
   - multiple lines in one recv()
   ========================================================= */

int receive_line(Client *client, char *line, size_t line_size)
{
    while (1)
    {
        for (size_t i = 0; i < client->recv_len; i++)
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

                size_t remaining =
                    client->recv_len - line_length;

                memmove(client->recv_buffer,
                        client->recv_buffer + line_length,
                        remaining);

                client->recv_len = remaining;

                return 1;
            }
        }

        if (client->recv_len == sizeof(client->recv_buffer))
        {
            return -2;
        }

        ssize_t received =
            recv(client->socket_fd,
                 client->recv_buffer + client->recv_len,
                 sizeof(client->recv_buffer) - client->recv_len,
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

        client->recv_len += (size_t)received;
    }
}


/* =========================================================
   REMOVE CLIENT
   ========================================================= */

void remove_client(Client *client)
{
    char username[MAX_USERNAME];
    int was_registered;

    pthread_mutex_lock(&clients_mutex);

    was_registered = client->registered;

    strncpy(username,
            client->username,
            sizeof(username) - 1);

    username[sizeof(username) - 1] = '\0';

    client->registered = 0;
    client->username[0] = '\0';
    client->recv_len = 0;

    int disconnected_fd = client->socket_fd;

    client->socket_fd = -1;

    pthread_mutex_unlock(&clients_mutex);

    if (!was_registered)
    {
        return;
    }

    char event[256];

    snprintf(event,
             sizeof(event),
             "User disconnected: %s",
             username);

    log_event(event);

    char message[256];

    snprintf(message,
             sizeof(message),
             "MSG BCAST SERVER %s left\n",
             username);

    pthread_mutex_lock(&clients_mutex);

    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        if (clients[i].registered &&
            clients[i].socket_fd != disconnected_fd)
        {
            send_all(clients[i].socket_fd,
                     message,
                     strlen(message));
        }
    }

    pthread_mutex_unlock(&clients_mutex);
}



/* =========================================================
   REGISTER USER
   ========================================================= */

int register_user(Client *client, const char *username)
{
    if (username == NULL ||
        strlen(username) == 0 ||
        strlen(username) >= MAX_USERNAME)
    {
        send_response(client->socket_fd,
                      "ERR 006 INVALID_USERNAME");

        return -1;
    }

    for (size_t i = 0; i < strlen(username); i++)
    {
        if (username[i] == ' ' ||
            username[i] == '\t' ||
            username[i] == '\n' ||
            username[i] == '\r')
        {
            send_response(client->socket_fd,
                          "ERR 006 INVALID_USERNAME");

            return -1;
        }
    }

    pthread_mutex_lock(&clients_mutex);

    /*
     * Check duplicate usernames.
     */
    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        if (clients[i].registered &&
            strcmp(clients[i].username,
                   username) == 0)
        {
            pthread_mutex_unlock(&clients_mutex);

            send_response(client->socket_fd,
                          "ERR 001 USERNAME_TAKEN");

            return -1;
        }
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

    char event[256];

    snprintf(event,
             sizeof(event),
             "User registered: %s",
             username);

    log_event(event);

    /*
     * Send registration response to the new client.
     */
    char response[256];

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
     * Notify all OTHER registered clients.
     */
    char presence[256];

    snprintf(presence,
             sizeof(presence),
             "MSG BCAST SERVER %s joined\n",
             username);

    pthread_mutex_lock(&clients_mutex);

    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        if (clients[i].registered &&
            clients[i].socket_fd != client->socket_fd)
        {
            send_all(clients[i].socket_fd,
                     presence,
                     strlen(presence));
        }
    }

    pthread_mutex_unlock(&clients_mutex);

    return 0;
}



/* =========================================================
   LIST USERS
   ========================================================= */

void list_users(Client *client)
{
    char users[MAX_CLIENTS * MAX_USERNAME];

    users[0] = '\0';

    pthread_mutex_lock(&clients_mutex);

    int first = 1;

    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        if (clients[i].registered)
        {
            if (!first)
            {
                strncat(users,
                        ",",
                        sizeof(users) - strlen(users) - 1);
            }

            strncat(users,
                    clients[i].username,
                    sizeof(users) - strlen(users) - 1);

            first = 0;
        }
    }

    pthread_mutex_unlock(&clients_mutex);

    char response[BUFFER_SIZE];

    snprintf(response,
             sizeof(response),
             "OK USERS %s",
             users);

    send_response(client->socket_fd,
                  response);

    log_event("LIST command executed");
}

/* =========================================================
   BROADCAST MESSAGE
   Sends the message to all other registered clients.
   ========================================================= */

void broadcast_message(Client *sender, const char *message)
{
    if (message == NULL || strlen(message) == 0)
    {
        send_response(sender->socket_fd,
                      "ERR 008 INVALID_COMMAND");

        return;
    }

    char outgoing[BUFFER_SIZE];

    snprintf(outgoing,
             sizeof(outgoing),
             "MSG BCAST %s %s\n",
             sender->username,
             message);

    pthread_mutex_lock(&clients_mutex);

    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        if (clients[i].registered &&
            clients[i].socket_fd != sender->socket_fd)
        {
            if (send_all(clients[i].socket_fd,
                         outgoing,
                         strlen(outgoing)) < 0)
            {
                perror("[SERVER] broadcast send");
            }
        }
    }

    pthread_mutex_unlock(&clients_mutex);

    char log_message[BUFFER_SIZE];

    snprintf(log_message,
             sizeof(log_message),
             "Broadcast from %s: %s",
             sender->username,
             message);

    log_event(log_message);

    send_response(sender->socket_fd,
                  "OK SENT");
}


/* =========================================================
   CLIENT THREAD
   ========================================================= */

void *handle_client(void *arg)
{
    Client *client = (Client *)arg;

    printf("[SERVER] Client connected: socket=%d\n",
           client->socket_fd);

    char line[BUFFER_SIZE];

    while (1)
    {
        int result =
            receive_line(client,
                         line,
                         sizeof(line));

        if (result == 0)
        {
            printf("[SERVER] Client disconnected: socket=%d\n",
                   client->socket_fd);

            break;
        }

        if (result == -1)
        {
            perror("[SERVER] recv");

            break;
        }

        if (result == -2)
        {
            send_response(client->socket_fd,
                          "ERR 005 LINE_TOO_LONG");

            break;
        }

        /*
         * Remove CR/LF.
         */

        line[strcspn(line, "\r\n")] = '\0';

        printf("[SERVER] Received: %s\n",
               line);

        /*
         * REGISTER must be the first command.
         */

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

        if (strncmp(line, "REGISTER", 8) == 0)
        {
            send_response(client->socket_fd,
                          "ERR 007 ALREADY_REGISTERED");

            continue;
        }


        /* =================================================
           LIST
           ================================================= */

        if (strcmp(line, "LIST") == 0)
        {
            list_users(client);
            continue;
        }

	/* =================================================
  		 BCAST
  	 ================================================= */

	if (strncmp(line, "BCAST ", 6) == 0)
	{
    		const char *message = line + 6;

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
           QUIT
           ================================================= */

        if (strcmp(line, "QUIT") == 0)
        {
            send_response(client->socket_fd,
                          "OK BYE");

            log_event("Client requested QUIT");

            break;
        }


        /* =================================================
           COMMANDS NOT IMPLEMENTED YET
           ================================================= */

        send_response(client->socket_fd,
                      "ERR 008 UNKNOWN_COMMAND");
    }

   int client_fd = client->socket_fd;

    remove_client(client);

    close(client_fd);

    return NULL;
}


/* =========================================================
   MAIN SERVER
   ========================================================= */

int main(void)
{
    int server_fd;

    struct sockaddr_in server_addr;

    /*
     * Initialize client table.
     */

    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        clients[i].socket_fd = -1;
        clients[i].registered = 0;
        clients[i].username[0] = '\0';
        clients[i].recv_len = 0;
    }


    server_fd =
        socket(AF_INET,
               SOCK_STREAM,
               0);

    if (server_fd < 0)
    {
        perror("socket");
        exit(EXIT_FAILURE);
    }


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


    memset(&server_addr,
           0,
           sizeof(server_addr));

    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);


    if (bind(server_fd,
             (struct sockaddr *)&server_addr,
             sizeof(server_addr)) < 0)
    {
        perror("bind");

        close(server_fd);

        exit(EXIT_FAILURE);
    }


    if (listen(server_fd,
               BACKLOG) < 0)
    {
        perror("listen");

        close(server_fd);

        exit(EXIT_FAILURE);
    }


    printf("========================================\n");
    printf("       NetMessenger Server\n");
    printf("========================================\n");
    printf("Registration : IT23752054\n");
    printf("Port         : 8054\n");
    printf("NID          : NID:7520\n");
    printf("========================================\n");
    printf("[SERVER] Listening...\n");


while (1)
{
    struct sockaddr_in client_addr;

    socklen_t client_len =
        sizeof(client_addr);

    /*
     * Find an unused slot in the global client table.
     */
    pthread_mutex_lock(&clients_mutex);

    Client *client = NULL;

    for (int i = 0; i < MAX_CLIENTS; i++)
    {
        if (clients[i].socket_fd == -1)
        {
            client = &clients[i];
            break;
        }
    }

    pthread_mutex_unlock(&clients_mutex);

    if (client == NULL)
    {
        fprintf(stderr,
                "[SERVER] Maximum client limit reached.\n");

        /*
         * Accept and immediately reject the connection.
         */
        int temp_fd =
            accept(server_fd,
                   (struct sockaddr *)&client_addr,
                   &client_len);

        if (temp_fd >= 0)
        {
            send_all(temp_fd,
                     "ERR 005 SERVER_FULL NID:7520\n",
                     strlen("ERR 005 SERVER_FULL NID:7520\n"));

            close(temp_fd);
        }

        continue;
    }

    /*
     * Accept the client into the selected slot.
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
     * Initialize the slot.
     */
    pthread_mutex_lock(&clients_mutex);

    client->socket_fd = accepted_fd;
    client->registered = 0;
    client->username[0] = '\0';
    client->recv_len = 0;

    pthread_mutex_unlock(&clients_mutex);

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

        pthread_mutex_unlock(&clients_mutex);

        close(accepted_fd);

        continue;
    }

    pthread_detach(thread_id);
}



    close(server_fd);

    return 0;
}
