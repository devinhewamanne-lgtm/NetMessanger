#include <arpa/inet.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#define PORT 8054
#define BUFFER_SIZE 4096
#define MAX_USERNAME 32

volatile int running = 1;


/* =========================================================
   RECEIVER CONTEXT
   ========================================================= */

typedef struct
{
    int socket_fd;
    char buffer[BUFFER_SIZE];
    size_t buffer_len;

} ReceiverContext;


/* =========================================================
   SEND ALL
   Ensures the complete message is sent.
   ========================================================= */

int send_all(int socket_fd, const char *data, size_t length)
{
    size_t total_sent = 0;

    while (total_sent < length)
    {
        ssize_t sent =
            send(socket_fd,
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
   RECEIVE ONE COMPLETE LINE
   Used during the initial REGISTER response.
   ========================================================= */

int receive_line_blocking(int socket_fd,
                          char *buffer,
                          size_t buffer_size)
{
    size_t received_total = 0;

    while (received_total < buffer_size - 1)
    {
        char ch;

        ssize_t received =
            recv(socket_fd,
                 &ch,
                 1,
                 0);

        if (received == 0)
        {
            return 0;
        }

        if (received < 0)
        {
            return -1;
        }

        buffer[received_total++] = ch;

        if (ch == '\n')
        {
            buffer[received_total] = '\0';
            return 1;
        }
    }

    buffer[buffer_size - 1] = '\0';

    return -2;
}


/* =========================================================
   RECEIVE THREAD
   Correctly handles:
   - partial lines
   - multiple lines in one recv()
   ========================================================= */

void *receive_messages(void *arg)
{
    ReceiverContext *ctx =
        (ReceiverContext *)arg;

    char line[BUFFER_SIZE];

    while (running)
    {
        /*
         * Search for a complete line already in the buffer.
         */
        size_t newline_pos = (size_t)-1;

        for (size_t i = 0; i < ctx->buffer_len; i++)
        {
            if (ctx->buffer[i] == '\n')
            {
                newline_pos = i;
                break;
            }
        }

        /*
         * A complete line is available.
         */
        if (newline_pos != (size_t)-1)
        {
            size_t line_length =
                newline_pos + 1;

            if (line_length >= sizeof(line))
            {
                printf("\n[CLIENT] Received line too long.\n");

                running = 0;
                break;
            }

            memcpy(line,
                   ctx->buffer,
                   line_length);

            line[line_length] = '\0';

            /*
             * Remove the processed line.
             */
            size_t remaining =
                ctx->buffer_len - line_length;

            memmove(ctx->buffer,
                    ctx->buffer + line_length,
                    remaining);

            ctx->buffer_len = remaining;

            printf("\n%s", line);

            printf("> ");
            fflush(stdout);

            continue;
        }

        /*
         * Buffer is full but there is no newline.
         */
        if (ctx->buffer_len >= sizeof(ctx->buffer) - 1)
        {
            printf("\n[CLIENT] Input buffer full.\n");

            running = 0;
            break;
        }

        /*
         * Receive more TCP data.
         */
        ssize_t received =
            recv(ctx->socket_fd,
                 ctx->buffer + ctx->buffer_len,
                 sizeof(ctx->buffer) -
                     ctx->buffer_len - 1,
                 0);

        if (received == 0)
        {
            printf("\n[CLIENT] Server disconnected.\n");

            running = 0;
            break;
        }

        if (received < 0)
        {
            if (running)
            {
                perror("[CLIENT] recv");
            }

            running = 0;
            break;
        }

        ctx->buffer_len +=
            (size_t)received;

        ctx->buffer[ctx->buffer_len] = '\0';
    }

    return NULL;
}


/* =========================================================
   MAIN
   ========================================================= */

int main(int argc, char *argv[])
{
    int sockfd;

    struct sockaddr_in server_addr;

    const char *server_ip = "127.0.0.1";

    if (argc >= 2)
    {
        server_ip = argv[1];
    }


    /* =====================================================
       CREATE SOCKET
       ===================================================== */

    sockfd =
        socket(AF_INET,
               SOCK_STREAM,
               0);

    if (sockfd < 0)
    {
        perror("socket");

        exit(EXIT_FAILURE);
    }


    memset(&server_addr,
           0,
           sizeof(server_addr));

    server_addr.sin_family =
        AF_INET;

    server_addr.sin_port =
        htons(PORT);


    if (inet_pton(AF_INET,
                  server_ip,
                  &server_addr.sin_addr) <= 0)
    {
        perror("inet_pton");

        close(sockfd);

        exit(EXIT_FAILURE);
    }


    /* =====================================================
       CONNECT TO SERVER
       ===================================================== */

    if (connect(sockfd,
                (struct sockaddr *)&server_addr,
                sizeof(server_addr)) < 0)
    {
        perror("connect");

        close(sockfd);

        exit(EXIT_FAILURE);
    }


    printf("========================================\n");
    printf("       NetMessenger Client\n");
    printf("========================================\n");
    printf("Registration : IT23752054\n");
    printf("Server IP    : %s\n", server_ip);
    printf("Port         : 8054\n");
    printf("NID          : NID:7520\n");
    printf("========================================\n");


    /* =====================================================
       USERNAME
       ===================================================== */

    char username[MAX_USERNAME];

    printf("Enter username: ");
    fflush(stdout);

    if (fgets(username,
              sizeof(username),
              stdin) == NULL)
    {
        close(sockfd);

        return 0;
    }

    username[strcspn(username,
                     "\r\n")] = '\0';


    if (strlen(username) == 0)
    {
        printf("Username cannot be empty.\n");

        close(sockfd);

        return 0;
    }


    /* =====================================================
       REGISTER
       ===================================================== */

    char register_command[BUFFER_SIZE];

    snprintf(register_command,
             sizeof(register_command),
             "REGISTER %s\n",
             username);


    if (send_all(sockfd,
                 register_command,
                 strlen(register_command)) < 0)
    {
        perror("send");

        close(sockfd);

        return 0;
    }


    /* =====================================================
       RECEIVE REGISTRATION RESPONSE
       ===================================================== */

    char response[BUFFER_SIZE];

    int response_result =
        receive_line_blocking(sockfd,
                              response,
                              sizeof(response));

    if (response_result == 0)
    {
        printf("Server disconnected.\n");

        close(sockfd);

        return 0;
    }

    if (response_result < 0)
    {
        printf("Failed to receive registration response.\n");

        close(sockfd);

        return 0;
    }

    printf("%s", response);


    /* =====================================================
       REGISTRATION FAILURE
       ===================================================== */

    if (strncmp(response,
                "OK REGISTERED",
                13) != 0)
    {
        close(sockfd);

        return 0;
    }


    /* =====================================================
       CREATE RECEIVER CONTEXT
       ===================================================== */

    ReceiverContext receiver_context;

    memset(&receiver_context,
           0,
           sizeof(receiver_context));

    receiver_context.socket_fd =
        sockfd;


    /* =====================================================
       START RECEIVER THREAD
       ===================================================== */

    pthread_t receiver_thread;

    if (pthread_create(&receiver_thread,
                       NULL,
                       receive_messages,
                       &receiver_context) != 0)
    {
        perror("pthread_create");

        close(sockfd);

        return 0;
    }


    printf("\nAvailable commands:\n");
    printf("LIST\n");
    printf("QUIT\n");
    printf("\n");


    /* =====================================================
       COMMAND LOOP
       ===================================================== */

    char command[BUFFER_SIZE];

    while (running)
    {
        printf("> ");
        fflush(stdout);

        if (fgets(command,
                  sizeof(command),
                  stdin) == NULL)
        {
            break;
        }

        if (send_all(sockfd,
                     command,
                     strlen(command)) < 0)
        {
            perror("send");

            break;
        }

        if (strncmp(command,
                    "QUIT",
                    4) == 0)
        {
            break;
        }
    }


    /* =====================================================
       SHUTDOWN
       ===================================================== */

    running = 0;

    shutdown(sockfd,
             SHUT_RDWR);

    close(sockfd);

    pthread_join(receiver_thread,
                 NULL);

    printf("\nClient closed.\n");

    return 0;
}
