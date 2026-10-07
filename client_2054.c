#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <unistd.h>

#define REGISTRATION_NUMBER "IT23752054"
#define SERVER_PORT 8054
#define NID_TAG "NID:7520"

#define BUFFER_SIZE 4096
#define MAX_USERNAME 32
#define MAX_FILENAME 256
#define MAX_FILE_SIZE (10ULL * 1024ULL * 1024ULL)

volatile int running = 1;

typedef struct
{
    int socket_fd;
    char buffer[BUFFER_SIZE];
    size_t buffer_len;
} ReceiverContext;


/* =========================================================
   FUNCTION PROTOTYPES
   ========================================================= */

int send_all(int socket_fd,
             const char *data,
             size_t length);

int receive_line_blocking(int socket_fd,
                          char *buffer,
                          size_t buffer_size);

int send_file_command(int socket_fd,
                      const char *command);

int receive_exact_local(ReceiverContext *context,
                        unsigned char *data,
                        size_t length);

void handle_incoming_file(ReceiverContext *context,
                          const char *header);

void *receive_messages(void *arg);

void print_commands(void);


/* =========================================================
   SEND ALL
   ========================================================= */

int send_all(int socket_fd,
             const char *data,
             size_t length)
{
    size_t total = 0;

    while (total < length)
    {
        ssize_t sent =
            send(socket_fd,
                 data + total,
                 length - total,
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

        total += (size_t)sent;
    }

    return 0;
}


/* =========================================================
   RECEIVE ONE LINE
   ========================================================= */

int receive_line_blocking(int socket_fd,
                          char *buffer,
                          size_t buffer_size)
{
    size_t total = 0;

    while (total < buffer_size - 1)
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

        buffer[total++] = ch;

        if (ch == '\n')
        {
            buffer[total] = '\0';
            return 1;
        }
    }

    buffer[buffer_size - 1] = '\0';

    return -2;
}


/* =========================================================
   SEND FILE
   IMPORTANT: this function is OUTSIDE main()
   ========================================================= */

int send_file_command(int socket_fd,
                      const char *command)
{
    char target[64];
    char filename[MAX_FILENAME];

    unsigned long long filesize;

    int parsed =
        sscanf(command,
               "SENDFILE %63s %255s %llu",
               target,
               filename,
               &filesize);

    if (parsed != 3)
    {
        printf("[CLIENT] Usage:\n");
        printf("SENDFILE <target> <filename> <filesize>\n");
        return -1;
    }

    if (filesize > MAX_FILE_SIZE)
    {
        printf("[CLIENT] File exceeds 10 MiB limit.\n");
        return -1;
    }

    struct stat st;

    if (stat(filename, &st) < 0)
    {
        perror("[CLIENT] stat");
        return -1;
    }

    if (!S_ISREG(st.st_mode))
    {
        printf("[CLIENT] Not a regular file.\n");
        return -1;
    }

    unsigned long long actual_size =
        (unsigned long long)st.st_size;

    if (actual_size != filesize)
    {
        printf("[CLIENT] File size mismatch.\n");
        printf("[CLIENT] Declared: %llu\n", filesize);
        printf("[CLIENT] Actual  : %llu\n", actual_size);
        return -1;
    }

    const char *basename =
        strrchr(filename, '/');

    if (basename != NULL)
    {
        basename++;
    }
    else
    {
        basename = filename;
    }

    char header[BUFFER_SIZE];

    int written =
        snprintf(header,
                 sizeof(header),
                 "SENDFILE %s %s %llu\n",
                 target,
                 basename,
                 filesize);

    if (written < 0 ||
        (size_t)written >= sizeof(header))
    {
        printf("[CLIENT] SENDFILE header too long.\n");
        return -1;
    }

    if (send_all(socket_fd,
                 header,
                 (size_t)written) < 0)
    {
        perror("[CLIENT] send header");
        return -1;
    }

    FILE *fp =
        fopen(filename, "rb");

    if (fp == NULL)
    {
        perror("[CLIENT] fopen");
        return -1;
    }

    unsigned char buffer[8192];

    unsigned long long total_sent = 0;

    while (total_sent < filesize)
    {
        unsigned long long remaining =
            filesize - total_sent;

        size_t to_read =
            remaining < sizeof(buffer)
                ? (size_t)remaining
                : sizeof(buffer);

        size_t read_bytes =
            fread(buffer,
                  1,
                  to_read,
                  fp);

        if (read_bytes == 0)
        {
            if (ferror(fp))
            {
                perror("[CLIENT] fread");
            }

            fclose(fp);
            return -1;
        }

        if (send_all(socket_fd,
                     (const char *)buffer,
                     read_bytes) < 0)
        {
            perror("[CLIENT] send file");
            fclose(fp);
            return -1;
        }

        total_sent +=
            (unsigned long long)read_bytes;
    }

    fclose(fp);

    printf("[CLIENT] Sent %s (%llu bytes)\n",
           basename,
           filesize);

    return 0;
}


/* =========================================================
   RECEIVE EXACTLY N FILE BYTES
   IMPORTANT: defined BEFORE handle_incoming_file()
   ========================================================= */

int receive_exact_local(ReceiverContext *context,
                        unsigned char *data,
                        size_t length)
{
    size_t total_received = 0;

    /*
     * First consume any bytes already buffered after
     * the SENDFILE header.
     */
    while (total_received < length &&
           context->buffer_len > 0)
    {
        size_t available =
            context->buffer_len;

        size_t needed =
            length - total_received;

        size_t to_copy =
            available < needed
                ? available
                : needed;

        memcpy(data + total_received,
               context->buffer,
               to_copy);

        total_received += to_copy;

        size_t remaining =
            context->buffer_len - to_copy;

        memmove(context->buffer,
                context->buffer + to_copy,
                remaining);

        context->buffer_len =
            remaining;
    }

    /*
     * Receive remaining raw bytes.
     */
    while (total_received < length)
    {
        ssize_t received =
            recv(context->socket_fd,
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
   HANDLE INCOMING FILE
   ========================================================= */

void handle_incoming_file(ReceiverContext *context,
                          const char *header)
{
    char target[64];
    char filename[MAX_FILENAME];

    unsigned long long filesize_ull;

    int parsed =
        sscanf(header,
               "SENDFILE %63s %255s %llu",
               target,
               filename,
               &filesize_ull);

    if (parsed != 3)
    {
        printf("\n[CLIENT] Invalid SENDFILE header.\n");
        return;
    }

    if (filesize_ull > MAX_FILE_SIZE)
    {
        printf("\n[CLIENT] Incoming file too large.\n");
        running = 0;
        return;
    }

    size_t filesize =
        (size_t)filesize_ull;

    const char *basename =
        strrchr(filename, '/');

    if (basename != NULL)
    {
        basename++;
    }
    else
    {
        basename = filename;
    }

    if (strstr(basename, "..") != NULL ||
        strchr(basename, '\\') != NULL)
    {
        printf("\n[CLIENT] Invalid filename.\n");
        running = 0;
        return;
    }

    if (mkdir("received_files", 0755) < 0 &&
        errno != EEXIST)
    {
        perror("[CLIENT] mkdir");
        return;
    }

    char output_path[512];

    int written =
        snprintf(output_path,
                 sizeof(output_path),
                 "received_files/%s",
                 basename);

    if (written < 0 ||
        (size_t)written >= sizeof(output_path))
    {
        printf("\n[CLIENT] Output path too long.\n");
        return;
    }

    unsigned char *data =
        malloc(filesize > 0 ? filesize : 1);

    if (data == NULL)
    {
        printf("\n[CLIENT] Memory allocation failed.\n");
        return;
    }

    printf("\n[CLIENT] Receiving %s (%llu bytes)\n",
           basename,
           filesize_ull);

    int result =
        receive_exact_local(context,
                             data,
                             filesize);

    if (result != 1)
    {
        free(data);

        printf("\n[CLIENT] Incomplete file transfer.\n");

        running = 0;
        return;
    }

    FILE *fp =
        fopen(output_path, "wb");

    if (fp == NULL)
    {
        perror("[CLIENT] fopen");
        free(data);
        return;
    }

    if (filesize > 0)
    {
        size_t written_bytes =
            fwrite(data,
                   1,
                   filesize,
                   fp);

        if (written_bytes != filesize)
        {
            perror("[CLIENT] fwrite");
            fclose(fp);
            free(data);
            return;
        }
    }

    fclose(fp);

    free(data);

    printf("\n[CLIENT] File received: %s\n",
           output_path);

    printf("> ");
    fflush(stdout);
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
        size_t newline_pos =
            (size_t)-1;

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

            size_t remaining =
                context->buffer_len -
                line_length;

            memmove(context->buffer,
                    context->buffer +
                        line_length,
                    remaining);

            context->buffer_len =
                remaining;

            /*
             * SENDFILE header is special:
             * immediately after this line,
             * raw bytes follow.
             */
            if (strncmp(line,
                        "SENDFILE ",
                        9) == 0)
            {
                handle_incoming_file(context,
                                     line);

                continue;
            }

            printf("\n%s", line);

            printf("> ");
            fflush(stdout);

            continue;
        }

        if (context->buffer_len >=
            sizeof(context->buffer) - 1)
        {
            printf("\n[CLIENT] Receive buffer full.\n");
            running = 0;
            break;
        }

        ssize_t received =
            recv(context->socket_fd,
                 context->buffer +
                     context->buffer_len,
                 sizeof(context->buffer) -
                     context->buffer_len - 1,
                 0);

        if (received == 0)
        {
            printf("\n[CLIENT] Server disconnected.\n");
            running = 0;
            break;
        }

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

        context->buffer[
            context->buffer_len] = '\0';
    }

    return NULL;
}


/* =========================================================
   COMMAND LIST
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
    printf("SENDFILE <target> <filename> <filesize>\n");
    printf("QUIT\n");
    printf("\n");
}


/* =========================================================
   MAIN
   ========================================================= */

int main(int argc, char *argv[])
{
    const char *server_ip =
        "127.0.0.1";

    if (argc >= 2)
    {
        server_ip = argv[1];
    }

    int sockfd =
        socket(AF_INET,
               SOCK_STREAM,
               0);

    if (sockfd < 0)
    {
        perror("socket");
        return EXIT_FAILURE;
    }

    struct sockaddr_in server_addr;

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

    if (connect(sockfd,
                (struct sockaddr *)&server_addr,
                sizeof(server_addr)) < 0)
    {
        perror("connect");
        close(sockfd);
        return EXIT_FAILURE;
    }

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

    char register_command[BUFFER_SIZE];

    int written =
        snprintf(register_command,
                 sizeof(register_command),
                 "REGISTER %s\n",
                 username);

    if (written < 0 ||
        (size_t)written >= sizeof(register_command))
    {
        printf("Username too long.\n");
        close(sockfd);
        return 0;
    }

    if (send_all(sockfd,
                 register_command,
                 strlen(register_command)) < 0)
    {
        perror("[CLIENT] send");
        close(sockfd);
        return 0;
    }

    char response[BUFFER_SIZE];

    int result =
        receive_line_blocking(sockfd,
                              response,
                              sizeof(response));

    if (result <= 0)
    {
        printf("No registration response from server.\n");
        close(sockfd);
        return 0;
    }

    printf("%s", response);

    if (strncmp(response,
                "OK REGISTERED",
                13) != 0)
    {
        close(sockfd);
        return 0;
    }

    ReceiverContext context;

    memset(&context,
           0,
           sizeof(context));

    context.socket_fd =
        sockfd;

    pthread_t receiver_thread;

    if (pthread_create(&receiver_thread,
                       NULL,
                       receive_messages,
                       &context) != 0)
    {
        perror("[CLIENT] pthread_create");
        close(sockfd);
        return 0;
    }

    print_commands();

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

        if (strcmp(command, "\n") == 0 ||
            strcmp(command, "\r\n") == 0)
        {
            continue;
        }

        /*
         * SENDFILE is special:
         * header + raw bytes.
         *
         * DO NOT pass it through the normal
         * send_all(command) path.
         */
        if (strncmp(command,
                    "SENDFILE ",
                    9) == 0)
        {
            if (send_file_command(sockfd,
                                  command) < 0)
            {
                printf("[CLIENT] File transfer failed.\n");
            }

            continue;
        }

        /*
         * Normal text commands.
         */
        if (send_all(sockfd,
                     command,
                     strlen(command)) < 0)
        {
            perror("[CLIENT] send");
            running = 0;
            break;
        }

        if (strncmp(command,
                    "QUIT",
                    4) == 0)
        {
            shutdown(sockfd,
                     SHUT_WR);

            break;
        }
    }

    if (running)
    {
        shutdown(sockfd,
                 SHUT_WR);
    }

    pthread_join(receiver_thread,
                 NULL);

    shutdown(sockfd,
             SHUT_RDWR);

    close(sockfd);

    running = 0;

    printf("\nClient closed.\n");

    return 0;
}
