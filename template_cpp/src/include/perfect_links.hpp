#pragma once

#include <netinet/in.h>
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <unistd.h>
#include <unordered_map>
#include <unordered_set>
#include <iostream>
#include <memory>
#include <mutex>
#include <utility>
#include <functional>
#include <arpa/inet.h>

#include "stubborn_links.hpp"
#include "message.hpp"

#define BUFFER_SIZE 128

class PerfectLinks
{
private:
    StubbornLinks stubbornLinks;
    std::unordered_map<unsigned long, std::unordered_set<uint32_t>> deliveredMessages;
    std::mutex deliveredMessagesMutex;

public:
    PerfectLinks(char *ip, unsigned short port);
    PerfectLinks(in_addr_t ip, unsigned short port);
    ~PerfectLinks();

    void send(char *dest_addr, unsigned short dest_port, Message &msg);
    void send(in_addr_t dest_addr, unsigned short dest_port, Message &msg);
    size_t recv(char *src_addr, unsigned short *src_port);
    size_t recv(in_addr_t *src_addr, unsigned short *src_port);
};

PerfectLinks::PerfectLinks(char *ip, unsigned short port) : stubbornLinks(ip, port) {}
PerfectLinks::PerfectLinks(in_addr_t ip, unsigned short port) : stubbornLinks(ip, port) {}
PerfectLinks::~PerfectLinks() {}

void PerfectLinks::send(char *dest_addr, unsigned short dest_port, Message &msg)
{
    size_t buffer_size;
    std::unique_ptr<char[]> buffer(Message::serialize(msg, buffer_size));
    stubbornLinks.send(dest_addr, dest_port, buffer.get(), buffer_size);
}

void PerfectLinks::send(in_addr_t dest_addr, unsigned short dest_port, Message &msg)
{
    size_t buffer_size;
    std::unique_ptr<char[]> buffer(Message::serialize(msg, buffer_size));
    stubbornLinks.send(dest_addr, dest_port, buffer.get(), buffer_size);
}

size_t PerfectLinks::recv(char *src_addr, unsigned short *src_port)
{
    char buffer[BUFFER_SIZE];
    size_t buffer_size = BUFFER_SIZE;
    size_t recv_size = stubbornLinks.recv(buffer, buffer_size, src_addr, src_port);

    if (recv_size > 0)
    {
        Message msg = Message::deserialize(buffer, recv_size);

        std::lock_guard<std::mutex> lock(deliveredMessagesMutex);
        auto it = deliveredMessages.find(msg.get_sender_id());
        if (it != deliveredMessages.end() && it->second.find(msg.get_msg_id()) != it->second.end())
        {
            return 0;
        }

        deliveredMessages[msg.get_sender_id()].insert(msg.get_msg_id());
        std::cout << "Delivered message with ID: " << msg.get_msg_id()
                  << " from sender: " << msg.get_sender_id()
                  << ", content: " << msg.get_msg()
                  << ", from address: " << src_addr << ":" << *src_port << "\n";

        return msg.get_msg_size();
    }

    return 0;
}

size_t PerfectLinks::recv(in_addr_t *src_addr, unsigned short *src_port)
{
    char buffer[BUFFER_SIZE];
    size_t buffer_size = BUFFER_SIZE;
    size_t recv_size = stubbornLinks.recv(buffer, buffer_size, src_addr, src_port);

    if (recv_size > 0)
    {
        Message msg = Message::deserialize(buffer, recv_size);

        std::lock_guard<std::mutex> lock(deliveredMessagesMutex);
        auto it = deliveredMessages.find(msg.get_sender_id());
        if (it != deliveredMessages.end() && it->second.find(msg.get_msg_id()) != it->second.end())
        {
            return 0;
        }

        deliveredMessages[msg.get_sender_id()].insert(msg.get_msg_id());
        std::cout << "Delivered message with ID: " << msg.get_msg_id()
                  << " from sender: " << msg.get_sender_id()
                  << ", content: " << msg.get_msg()
                  << ", from address: " << inet_ntoa(*reinterpret_cast<in_addr *>(src_addr))
                  << ":" << ntohs(*src_port) << "\n";

        return msg.get_msg_size();
    }

    return 0;
}