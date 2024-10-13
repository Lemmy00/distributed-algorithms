#pragma once

#include <netinet/in.h>
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <unistd.h>
#include <arpa/inet.h>
#include <iostream>

class FairLossLinks
{
private:
    int socket_fd;
    struct sockaddr_in servaddr;

public:
    FairLossLinks(char *ip, unsigned short port);
    FairLossLinks(in_addr_t ip, unsigned short port);

    ~FairLossLinks();

    size_t send(char *dest_addr, unsigned short dest_port, const char *buffer, const size_t buffer_size);
    size_t send(in_addr_t dest_addr, unsigned short dest_port, const char *buffer, const size_t buffer_size);
    size_t recv(char *buffer, const size_t buffer_size, char *src_addr, unsigned short *src_port);
    size_t recv(char *buffer, const size_t buffer_size, in_addr_t *src_addr, unsigned short *src_port);
};

FairLossLinks::FairLossLinks(char *ip_addr, unsigned short port)
{
    if ((socket_fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
    {
        perror("socket creation failed");
        exit(EXIT_FAILURE);
    }

    memset(&servaddr, 0, sizeof(servaddr));

    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(port);
    servaddr.sin_addr.s_addr = inet_addr(ip_addr);

    if (bind(socket_fd, reinterpret_cast<const struct sockaddr *>(&servaddr), sizeof(servaddr)) < 0)
    {
        perror("bind failed");
        close(socket_fd);
        exit(EXIT_FAILURE);
    }
}

FairLossLinks::FairLossLinks(in_addr_t ip_addr, unsigned short port)
{
    if ((socket_fd = socket(AF_INET, SOCK_DGRAM, 0)) < 0)
    {
        perror("socket creation failed");
        exit(EXIT_FAILURE);
    }

    memset(&servaddr, 0, sizeof(servaddr));

    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(port);
    servaddr.sin_addr.s_addr = ip_addr;

    if (bind(socket_fd, reinterpret_cast<const struct sockaddr *>(&servaddr), sizeof(servaddr)) < 0)
    {
        perror("bind failed");
        close(socket_fd);
        exit(EXIT_FAILURE);
    }
}

FairLossLinks::~FairLossLinks()
{
    close(socket_fd);
}

size_t FairLossLinks::send(char *dest_addr, unsigned short dest_port, const char *buffer, const size_t buffer_size)
{
    struct sockaddr_in destaddr;
    memset(&destaddr, 0, sizeof(destaddr));

    destaddr.sin_family = AF_INET;
    destaddr.sin_port = htons(dest_port);
    destaddr.sin_addr.s_addr = inet_addr(dest_addr);

    ssize_t sent_bytes = sendto(socket_fd, buffer, buffer_size, 0, reinterpret_cast<const struct sockaddr *>(&destaddr), sizeof(destaddr));
    if (sent_bytes < 0)
    {
        perror("sendto failed");
    }
    return sent_bytes;
}

size_t FairLossLinks::send(in_addr_t dest_addr, unsigned short dest_port, const char *buffer, const size_t buffer_size)
{
    struct sockaddr_in destaddr;
    memset(&destaddr, 0, sizeof(destaddr));

    destaddr.sin_family = AF_INET;
    destaddr.sin_port = htons(dest_port);
    destaddr.sin_addr.s_addr = dest_addr;

    ssize_t sent_bytes = sendto(socket_fd, buffer, buffer_size, 0, reinterpret_cast<const struct sockaddr *>(&destaddr), sizeof(destaddr));
    if (sent_bytes < 0)
    {
        perror("sendto failed");
    }
    return sent_bytes;
}

size_t FairLossLinks::recv(char *buffer, const size_t buffer_size, char *src_addr, unsigned short *src_port)
{
    struct sockaddr_in srcaddr;
    socklen_t len = sizeof(srcaddr);

    ssize_t n = recvfrom(socket_fd, buffer, buffer_size, 0, reinterpret_cast<struct sockaddr *>(&srcaddr), &len);
    if (n < 0)
    {
        perror("recvfrom failed");
    }

    *src_port = srcaddr.sin_port;
    strcpy(src_addr, inet_ntoa(srcaddr.sin_addr));

    return n;
}

size_t FairLossLinks::recv(char *buffer, const size_t buffer_size, in_addr_t *src_addr, unsigned short *src_port)
{
    struct sockaddr_in srcaddr;
    socklen_t len = sizeof(srcaddr);

    ssize_t n = recvfrom(socket_fd, buffer, buffer_size, 0, reinterpret_cast<struct sockaddr *>(&srcaddr), &len);
    if (n < 0)
    {
        perror("recvfrom failed");
    }

    *src_port = ntohs(srcaddr.sin_port);
    *src_addr = srcaddr.sin_addr.s_addr;

    return n;
}