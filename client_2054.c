#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

/* =========================================================
   PERSONALIZATION
   ========================================================= */

#define REGISTRATION_NUMBER "IT23752054"
#define SERVER_PORT 8054
#define NID_TAG "NID:7520"

#define BUFFER_SIZE 4096
#define MAX_USERNAME 32


/* =========================================================
   GLOBAL STATE
   ========================================================= */

volatile int running = 1;


/* =========================================================
   RECEIVER CONTEXT
   ========================================================= */

typedef struct
{
    int socket_fd;

    /*
     * TCP receive buffer.
     *
     * Since TCP is a byte stream, one recv() may contain:
     * - a partial line
     * - one complete line
     * - multiple complete lines
     */
    char buffer[BUFFER_SIZE];

    size_t buffer_len;

} ReceiverContext;


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

        total_sent += (size_t)sent;
    }

    return 0;
}


/* =========================================================
   RECEIVE ONE COMPLETE LINE
   Used for the REGISTER response.
   ========================================================= */

int receive_line_blocking(int socket_fd,
                          char *buffer,
                          size_t buffer_size)
{
    size_t total_received = 0;

    while (total_received < buffer_size - 1)
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
            if (errno == EINTR)
            {
                continue;
            }

            return -1;
        }

        buffer[total_received] = ch;
        total_received++;

        if (ch == '\n')
        {
            buffer[total_received] = '\0';

            return 1;
        }
    }

    buffer[buffer_size - 1] = '\0';

    return -2;
}


/* =========================================================
   DISPLAY RECEIVED SERVER MESSAGE
   ========================================================= */

void display_server_line(const char *line)
{
    printf("\n%s", line);

    if (running)
    {
        printf("> ");
        fflush(stdout);
    }
}


/* =========================================================
   RECEIVER THREAD
   ========================================================= */

void *receive_messages(void *arg)
{
    ReceiverContext *context =
        (ReceiverContext *)arg;

    char line[BUFFER_SIZE];

    while (running)
    {
        /*
         * Look for a complete line in the buffer.
         */
        size_t newline_pos = (size_t)-1;

        for (size_t i = 0;
             i < context->buffer_len;
             i++)
        {
            if (context->buffer[i] == '\n')
            {
                newline_pos = i;
                break;
            }
        }

        /*
         * Complete line found.
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
                   context->buffer,
                   line_length);

            line[line_length] = '\0';

            /*
             * Remove the processed line.
             */
            size_t remaining =
                context->buffer_len - line_length;

            memmove(context->buffer,
                    context->buffer + line_length,
                    remaining);

            context->buffer_len = remaining;

            display_server_line(line);

            continue;
        }

        /*
         * Protect receive buffer from overflow.
         */
        if (context->buffer_len >=
            sizeof(context->buffer) - 1)
        {
            printf("\n[CLIENT] Receive buffer full.\n");

            running = 0;
            break;
        }

        /*
         * Receive more TCP data.
         */
        ssize_t received =
            recv(context->socket_fd,
                 context->buffer + context->buffer_len,
                 sizeof(context->buffer) -
                     context->buffer_len - 1,
                 0);

        /*
         * Server disconnected.
         */
        if (received == 0)
        {
            printf("\n[CLIENT] Server disconnected.\n");

            running = 0;
            break;
        }

        /*
         * Receive error.
         */
        if (received < 0)
        {
            if (errno == EINTR)
            {
                continue;
            }

            if (running)
            {
                perror("[CLIENT] recv");
            }

            running = 0;
            break;
        }

        context->buffer_len +=
            (size_t)received;

        context->buffer[context->buffer_len] = '\0';
    }

    return NULL;
}


/* =========================================================
   DISPLAY COMMANDS
   ========================================================= */

void print_commands(void)
{
    printf("\nAvailable commands:\n");
    printf("LIST\n");
    printf("BCAST <message>\n");
    printf("PMSG <username> <message>\n");
    printf("JOIN <room>\n");
    printf("LEAVE <room>\n");
    printf("ROOMS\n");
    printf("RMSG <room> <message>\n");
    printf("QUIT\n");
    printf("\n");
}


/* =========================================================
   MAIN
   ========================================================= */

int main(int argc, char *argv[])
{
    int sockfd;

    struct sockaddr_in server_addr;

    const char *server_ip = "127.0.0.1";


    /* =====================================================
       OPTIONAL SERVER IP
       ===================================================== */

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


    /* =====================================================
       SERVER ADDRESS
       ===================================================== */

    memset(&server_addr,
           0,
           sizeof(server_addr));

    server_addr.sin_family =
        AF_INET;

    server_addr.sin_port =
        htons(SERVER_PORT);


    if (inet_pton(AF_INET,
                  server_ip,
                  &server_addr.sin_addr) <= 0)
    {
        perror("inet_pton");

        close(sockfd);

        return EXIT_FAILURE;
    }


    /* =====================================================
       CONNECT
       ===================================================== */

    if (connect(sockfd,
                (struct sockaddr *)&server_addr,
                sizeof(server_addr)) < 0)
    {
        perror("connect");

        close(sockfd);

        return EXIT_FAILURE;
    }


    /* =====================================================
       CLIENT INFORMATION
       ===================================================== */

    printf("========================================\n");
    printf("       NetMessenger Client\n");
    printf("========================================\n");
    printf("Registration : %s\n",
           REGISTRATION_NUMBER);
    printf("Server IP    : %s\n",
           server_ip);
    printf("Port         : %d\n",
           SERVER_PORT);
    printf("NID          : %s\n",
           NID_TAG);
    printf("========================================\n");


    /* =====================================================
       GET USERNAME
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
       CREATE REGISTER COMMAND
       ===================================================== */

    char register_command[BUFFER_SIZE];

    int written =
        snprintf(register_command,
                 sizeof(register_command),
                 "REGISTER %s\n",
                 username);

    if (written < 0 ||
        (size_t)written >= sizeof(register_command))
    {
        printf("Username is too long.\n");

        close(sockfd);

        return 0;
    }


    /* =====================================================
       SEND REGISTER
       ===================================================== */

    if (send_all(sockfd,
                 register_command,
                 strlen(register_command)) < 0)
    {
        perror("[CLIENT] send");

        close(sockfd);

        return 0;
    }


    /* =====================================================
       RECEIVE REGISTER RESPONSE
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

    if (response_result == -1)
    {
        perror("[CLIENT] recv");

        close(sockfd);

        return 0;
    }

    if (response_result == -2)
    {
        printf("Registration response too long.\n");

        close(sockfd);

        return 0;
    }

    printf("%s", response);


    /* =====================================================
       CHECK REGISTRATION RESULT
       ===================================================== */

    if (strncmp(response,
                "OK REGISTERED",
                13) != 0)
    {
        /*
         * Example:
         * ERR 001 USERNAME_TAKEN NID:7520
         */
        close(sockfd);

        return 0;
    }


    /* =====================================================
       RECEIVER CONTEXT
       ===================================================== */

    ReceiverContext receiver_context;

    memset(&receiver_context,
           0,
           sizeof(receiver_context));

    receiver_context.socket_fd = sockfd;


    /* =====================================================
       START RECEIVER THREAD
       ===================================================== */

    pthread_t receiver_thread;

    if (pthread_create(&receiver_thread,
                       NULL,
                       receive_messages,
                       &receiver_context) != 0)
    {
        perror("[CLIENT] pthread_create");

        close(sockfd);

        return 0;
    }


    /* =====================================================
       DISPLAY COMMANDS
       ===================================================== */

    print_commands();


    /* =====================================================
       MAIN COMMAND LOOP
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


        /*
         * Ignore empty input.
         */
        if (strcmp(command, "\n") == 0 ||
            strcmp(command, "\r\n") == 0)
        {
            continue;
        }


        /*
         * Send command to server.
         */
        if (send_all(sockfd,
                     command,
                     strlen(command)) < 0)
        {
            perror("[CLIENT] send");

            running = 0;
            break;
        }


        /*
         * QUIT:
         * The server should still send:
         *
         * OK BYE NID:7520
         */
        if (strncmp(command,
                    "QUIT",
                    4) == 0)
        {
            shutdown(sockfd,
                     SHUT_WR);

            break;
        }
    }


    /* =====================================================
       SHUTDOWN
       ===================================================== */

    if (running)
    {
        shutdown(sockfd,
                 SHUT_WR);
    }

    pthread_join(receiver_thread,
                 NULL);


    /* =====================================================
       CLOSE SOCKET
       ===================================================== */

    shutdown(sockfd,
             SHUT_RDWR);

    close(sockfd);

    running = 0;

    printf("\nClient closed.\n");

    return 0;
}
