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

#include "fair_loss_links.hpp"
#include "message.hpp"

#define BUFFER_SIZE 128

class PerfectLinks
{
private:
    FairLossLinks fairLossLinks;
    std::unordered_map<unsigned long, std::unordered_set<uint32_t>> deliveredMessages;
    std::mutex deliveredMessagesMutex;

public:
    PerfectLinks(char *ip, unsigned short port);
    PerfectLinks(in_addr_t ip, unsigned short port);
    ~PerfectLinks();

    void send(char *dest_addr, unsigned short dest_port, Message &msg);
    void send(in_addr_t dest_addr, unsigned short dest_port, Message &msg);
    void recv();
};

PerfectLinks::PerfectLinks(char *ip, unsigned short port) : fairLossLinks(ip, port) {}
PerfectLinks::PerfectLinks(in_addr_t ip, unsigned short port) : fairLossLinks(ip, port) {}
PerfectLinks::~PerfectLinks() {}

void PerfectLinks::send(char *dest_addr, unsigned short dest_port, Message &msg)
{
    size_t buffer_size;
    std::unique_ptr<char[]> buffer(Message::serialize(msg, buffer_size));
    while (true)
    {
        fairLossLinks.send(dest_addr, dest_port, buffer.get(), buffer_size);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

void PerfectLinks::send(in_addr_t dest_addr, unsigned short dest_port, Message &msg)
{
    size_t buffer_size;
    std::unique_ptr<char[]> buffer(Message::serialize(msg, buffer_size));
    while (true)
    {
        fairLossLinks.send(dest_addr, dest_port, buffer.get(), buffer_size);
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
}

void PerfectLinks::recv()
{
    while (true)
    {
        in_addr_t src_addr[BUFFER_SIZE];
        unsigned short src_port;

        char buffer[BUFFER_SIZE];
        size_t buffer_size = BUFFER_SIZE;
        size_t recv_size = fairLossLinks.recv(buffer, buffer_size, src_addr, &src_port);

        if (recv_size > 0)
        {
            Message msg = Message::deserialize(buffer, recv_size);

            std::lock_guard<std::mutex> lock(deliveredMessagesMutex);
            auto it = deliveredMessages.find(msg.get_sender_id());
            if (it != deliveredMessages.end() && it->second.find(msg.get_msg_id()) != it->second.end())
            {
                continue;
            }

            deliveredMessages[msg.get_sender_id()].insert(msg.get_msg_id());
            std::cout << "Delivered message with ID: " << msg.get_msg_id()
                      << " from sender: " << msg.get_sender_id()
                      << ", content: " << msg.get_msg()
                      << ", from address: " << inet_ntoa(*reinterpret_cast<in_addr *>(src_addr))
                      << ":" << ntohs(src_port) << "\n";
        }
    }
}
