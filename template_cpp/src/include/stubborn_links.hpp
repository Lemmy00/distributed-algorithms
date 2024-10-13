#pragma once

#include <netinet/in.h>
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <unistd.h>
#include <thread>
#include <chrono>

#include "fair_loss_links.hpp"

class StubbornLinks
{
private:
    FairLossLinks fairLossLinks;

public:
    StubbornLinks(char *ip, unsigned short port);
    StubbornLinks(in_addr_t ip, unsigned short port);

    ~StubbornLinks();

    void send(char *dest_addr, unsigned short dest_port, const char *buffer, const size_t buffer_size);
    void send(in_addr_t dest_addr, unsigned short dest_port, const char *buffer, const size_t buffer_size);
    size_t recv(char *buffer, const size_t buffer_size, char *src_addr, unsigned short *src_port);
    size_t recv(char *buffer, const size_t buffer_size, in_addr_t *src_addr, unsigned short *src_port);
};

StubbornLinks::StubbornLinks(char *ip, unsigned short port) : fairLossLinks(ip, port) {}

StubbornLinks::StubbornLinks(in_addr_t ip, unsigned short port) : fairLossLinks(ip, port) {}

StubbornLinks::~StubbornLinks() {}

void StubbornLinks::send(char *dest_addr, unsigned short dest_port, const char *buffer, const size_t buffer_size)
{
    while (true)
    {
        fairLossLinks.send(dest_addr, dest_port, buffer, buffer_size);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

void StubbornLinks::send(in_addr_t dest_addr, unsigned short dest_port, const char *buffer, const size_t buffer_size)
{
    while (true)
    {
        fairLossLinks.send(dest_addr, dest_port, buffer, buffer_size);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

size_t StubbornLinks::recv(char *buffer, const size_t buffer_size, char *src_addr, unsigned short *src_port)
{
    return fairLossLinks.recv(buffer, buffer_size, src_addr, src_port);
}

size_t StubbornLinks::recv(char *buffer, const size_t buffer_size, in_addr_t *src_addr, unsigned short *src_port)
{
    return fairLossLinks.recv(buffer, buffer_size, src_addr, src_port);
}
