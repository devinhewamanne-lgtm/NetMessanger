# NetMessenger Design Diary

## Student
Registration Number: IT23752054

## Project
NetMessenger – Multi-Client Chat and File-Sharing Platform over TCP/IP

## Initial Design Decision
The system was designed as a TCP client-server application written in C using the standard BSD socket API. A single server accepts multiple client connections and maintains connected users and chat-room information.

## Concurrency Model
A concurrent client-handling approach was selected so that multiple clients can communicate with the server at the same time without blocking other users. The implementation uses the chosen concurrency mechanism in the server to handle connected clients independently.

## Protocol Implementation
The supplied line-based NetMessenger protocol was followed for registration, user listing, broadcast messaging, private messaging, room management, file transfer, and graceful disconnection. Special attention was given to newline-based command framing and the requirement to read the exact number of bytes for SENDFILE transfers.

## Personalisation
The registration number IT23752054 was used to calculate the required project values:

- Server/client port: 8054
- Response tag: NID:7520
- Server source: server_2054.c
- Client source: client_2054.c
- Makefile: Makefile_2054
- Log file: netmsg_IT23752054.log
- Storage root: storage/IT23752054/

## Development Challenges
During development, testing was carried out incrementally. Important challenges included handling multiple clients, maintaining user and room state, implementing file-transfer framing correctly, managing disconnects, and ensuring personalised protocol responses were generated consistently.

## File Transfer
The SENDFILE feature was tested using a sample text file. The sender received a successful FILE_RECEIVED response, the receiving client created the transferred file, and the server stored a copy under the personalised storage directory.

## Testing
The system was tested for registration, LIST, broadcast messaging, private messaging, room operations, logging, file transfer, and disconnect behaviour. Testing was performed in the Linux/CentOS environment.

## Reflection on Development
The implementation process improved my understanding of TCP socket communication, client-server architecture, concurrent connection handling, message framing, file transfer, and debugging network applications. Testing each feature incrementally helped identify problems before integrating the complete system.
