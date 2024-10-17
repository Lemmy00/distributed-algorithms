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
#include <thread>
#include <vector>
#include <condition_variable>
#include <string>

#include "fair_loss_links.hpp"
#include "message.hpp"
#include "logger.hpp"
#include "ts_queue.hpp"

#define BUFFER_SIZE 64

class PerfectLinks
{
private:
    FairLossLinks fairLossLinks;
    std::unordered_map<unsigned long, std::unordered_set<uint32_t>> deliveredMessages;
    std::unordered_set<uint32_t> ackedMessages;
    std::mutex ackedMessagesMutex;
    std::mutex deliveredMessagesMutex;

    TSQueue<Message> messageQueue;
    std::atomic<bool> stopThreads{false};

    std::shared_ptr<Logger> logger;

public:
    PerfectLinks(in_addr_t ip, unsigned short port, std::shared_ptr<Logger> loggerInstance, size_t queueSize = 100000);
    ~PerfectLinks();

    void send(const Message &msg);

    std::vector<std::thread> startReceivers(size_t n);
    std::thread startReciever();
    std::thread startSender();
    void stop();

private:
    void sendWorker();
    void receiverWorker();
    void sendAck(Message &ack_msg);
};

PerfectLinks::PerfectLinks(in_addr_t ip, unsigned short port, std::shared_ptr<Logger> loggerInstance, size_t queueSize)
    : fairLossLinks(ip, port), messageQueue(queueSize), logger(std::move(loggerInstance)) {}

PerfectLinks::~PerfectLinks()
{
    stop();
}

std::vector<std::thread> PerfectLinks::startReceivers(size_t n)
{
    std::vector<std::thread> threads;
    for (size_t i = 0; i < n; ++i)
    {
        threads.emplace_back(&PerfectLinks::receiverWorker, this);
    }
    return threads;
}

std::thread PerfectLinks::startReciever()
{
    return std::thread(&PerfectLinks::receiverWorker, this);
}

std::thread PerfectLinks::startSender()
{
    return std::thread(&PerfectLinks::sendWorker, this);
}

void PerfectLinks::stop()
{
    stopThreads = true;
    messageQueue.shutdown();
}

void PerfectLinks::send(const Message &msg)
{
    if (stopThreads)
    {
        return;
    }

    // std::cout << "Sending message with ID: " << msg.get_msg_id() << " with content: " << msg.get_msg() << "\n";
    logger->log("b " + msg.get_msg());

    while (messageQueue.full())
    {
    }

    messageQueue.push(msg);
}

void PerfectLinks::sendWorker()
{
    while (!stopThreads)
    {
        auto msgOpt = messageQueue.pop();
        if (!msgOpt.has_value())
        {
            continue;
        }

        Message msg = msgOpt.value();
        {
            std::lock_guard<std::mutex> lock(ackedMessagesMutex);
            if (ackedMessages.find(msg.get_msg_id()) != ackedMessages.end())
            {
                continue;
            }
        }
        // std::cout << "Sending message with ID: " << msg.get_msg_id() << " with content: " << msg.get_msg() << "\n";

        size_t buffer_size;
        std::unique_ptr<char[]> buffer(Message::serialize(msg, buffer_size));

        fairLossLinks.send(msg.get_dest_addr(), msg.get_dest_port(), buffer.get(), buffer_size);
        messageQueue.push(msg);
    }
}

void PerfectLinks::receiverWorker()
{
    while (!stopThreads)
    {
        in_addr src_addr;
        unsigned short src_port;

        char buffer[BUFFER_SIZE];
        size_t buffer_size = BUFFER_SIZE;
        size_t recv_size = fairLossLinks.recv(buffer, buffer_size, &src_addr, &src_port);

        if (recv_size > 0)
        {
            Message msg = Message::deserialize(buffer, recv_size, fairLossLinks.get_ip(), fairLossLinks.get_port());
            if (msg.get_is_ack())
            {
                std::lock_guard<std::mutex> lock(ackedMessagesMutex);
                ackedMessages.insert(msg.get_msg_id());
                // std::cout << "Received ACK for message with ID: " << msg.get_msg_id() << "\n";
                continue;
            }

            Message ack_msg(msg.get_msg_id(), msg.get_sender_id(), src_addr.s_addr, src_port);
            sendAck(ack_msg);

            {
                std::lock_guard<std::mutex> lock(deliveredMessagesMutex);
                auto it = deliveredMessages.find(msg.get_sender_id());
                if (it != deliveredMessages.end() && it->second.find(msg.get_msg_id()) != it->second.end())
                {
                    continue;
                }

                deliveredMessages[msg.get_sender_id()].insert(msg.get_msg_id());
            }

            logger->log("d " + std::to_string(msg.get_sender_id()) + " " + msg.get_msg());
            /*std::cout << "Delivered message with ID: " << msg.get_msg_id()
                      << " from sender: " << msg.get_sender_id()
                      << ", content: " << msg.get_msg()
                      << ", from address: " << inet_ntoa(src_addr)
                      << ":" << ntohs(src_port) << "\n";*/
        }
    }
}

void PerfectLinks::sendAck(Message &ack_msg)
{
    size_t buffer_size;
    std::unique_ptr<char[]> buffer(Message::serialize(ack_msg, buffer_size));

    fairLossLinks.send(ack_msg.get_dest_addr(), ack_msg.get_dest_port(), buffer.get(), buffer_size);
}
