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
#include <queue>
#include <condition_variable>
#include <string>

#include "fair_loss_links.hpp"
#include "message.hpp"
#include "logger.hpp"

#define BUFFER_SIZE 64

class PerfectLinks
{
private:
    FairLossLinks fairLossLinks;
    std::unordered_map<unsigned long, std::unordered_set<uint32_t>> deliveredMessages;
    std::unordered_set<uint32_t> ackedMessages;
    std::mutex deliveredMessagesMutex;
    std::mutex ackedMessagesMutex;

    std::vector<std::thread> senderThreads;
    std::queue<std::function<void()>> sendTasks;
    std::mutex queueMutex;
    std::condition_variable queueCV;
    bool stopThreads = false;

    std::thread receiverThread;
    size_t numThreads;

    std::shared_ptr<Logger> logger;

public:
    PerfectLinks(const char *ip, unsigned short port, size_t num_threads, std::shared_ptr<Logger> loggerInstance);
    PerfectLinks(in_addr_t ip, unsigned short port, size_t num_threads, std::shared_ptr<Logger> loggerInstance);
    ~PerfectLinks();

    void send(const char *dest_addr, unsigned short dest_port, const Message &msg);
    void send(in_addr_t dest_addr, unsigned short dest_port, const Message &msg);

    void start();
    void stop();
    void shutdown();

private:
    void sendWorker();
    void receiverWorker();
    void sendAck(in_addr_t dest_addr, unsigned short dest_port, uint32_t msg_id, unsigned long sender_id);
};

PerfectLinks::PerfectLinks(const char *ip, unsigned short port, size_t num_threads, std::shared_ptr<Logger> loggerInstance)
    : fairLossLinks(ip, port), numThreads(num_threads), logger(std::move(loggerInstance)) {}

PerfectLinks::PerfectLinks(in_addr_t ip, unsigned short port, size_t num_threads, std::shared_ptr<Logger> loggerInstance)
    : fairLossLinks(ip, port), numThreads(num_threads), logger(std::move(loggerInstance)) {}

PerfectLinks::~PerfectLinks()
{
    shutdown();
}

void PerfectLinks::start()
{
    std::cout << "Starting PerfectLinks...\n";
    for (size_t i = 0; i < numThreads; ++i)
    {
        senderThreads.emplace_back(&PerfectLinks::sendWorker, this);
    }

    receiverThread = std::thread(&PerfectLinks::receiverWorker, this);
}

void PerfectLinks::stop()
{
    {
        std::lock_guard<std::mutex> lock(queueMutex);
        stopThreads = true;
    }
    queueCV.notify_all();

    for (auto &t : senderThreads)
    {
        if (t.joinable())
            t.join();
    }

    if (receiverThread.joinable())
    {
        receiverThread.join();
    }
}

void PerfectLinks::shutdown()
{
    stop();
    std::cout << "Shutting down PerfectLinks...\n";
    {
        std::lock_guard<std::mutex> lock(queueMutex);
        while (!sendTasks.empty())
        {
            sendTasks.pop();
        }
    }
}

void PerfectLinks::send(const char *dest_addr, unsigned short dest_port, const Message &msg)
{
    auto msg_ptr = std::make_shared<Message>(msg);

    auto task = [this, dest_addr, dest_port, msg_ptr]()
    {
        size_t buffer_size;
        std::unique_ptr<char[]> buffer(Message::serialize(*msg_ptr, buffer_size));

        while (true)
        {
            {
                std::lock_guard<std::mutex> lock(ackedMessagesMutex);
                if (ackedMessages.find(msg_ptr->get_msg_id()) != ackedMessages.end())
                {
                    break;
                }
            }

            fairLossLinks.send(dest_addr, dest_port, buffer.get(), buffer_size);
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    };

    {
        std::lock_guard<std::mutex> lock(queueMutex);
        sendTasks.push(task);
    }
    queueCV.notify_one();
}

void PerfectLinks::send(in_addr_t dest_addr, unsigned short dest_port, const Message &msg)
{
    auto msg_ptr = std::make_shared<Message>(msg);

    auto task = [this, dest_addr, dest_port, msg_ptr]()
    {
        size_t buffer_size;
        std::unique_ptr<char[]> buffer(Message::serialize(*msg_ptr, buffer_size));
        logger->log("b " + msg_ptr->get_msg());

        while (true)
        {
            {
                std::lock_guard<std::mutex> lock(ackedMessagesMutex);
                if (ackedMessages.find(msg_ptr->get_msg_id()) != ackedMessages.end())
                {
                    break;
                }
            }

            fairLossLinks.send(dest_addr, dest_port, buffer.get(), buffer_size);
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }
    };

    {
        std::lock_guard<std::mutex> lock(queueMutex);
        sendTasks.push(task);
    }
    queueCV.notify_one();
}

void PerfectLinks::sendWorker()
{
    while (true)
    {
        std::function<void()> task;
        {
            std::unique_lock<std::mutex> lock(queueMutex);
            queueCV.wait(lock, [this]()
                         { return !sendTasks.empty() || stopThreads; });

            if (stopThreads && sendTasks.empty())
                return;

            task = std::move(sendTasks.front());
            sendTasks.pop();
        }
        task();
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
            Message msg = Message::deserialize(buffer, recv_size);
            if (msg.get_is_ack())
            {
                std::lock_guard<std::mutex> lock(ackedMessagesMutex);
                ackedMessages.insert(msg.get_msg_id());
                std::cout << "Received ACK for message with ID: " << msg.get_msg_id() << "\n";
                continue;
            }

            {
                std::lock_guard<std::mutex> lock(deliveredMessagesMutex);
                auto it = deliveredMessages.find(msg.get_sender_id());
                if (it != deliveredMessages.end() && it->second.find(msg.get_msg_id()) != it->second.end())
                {
                    continue;
                }

                deliveredMessages[msg.get_sender_id()].insert(msg.get_msg_id());
                logger->log("d " + std::to_string(msg.get_sender_id()) + " " + msg.get_msg());
            }

            sendAck(src_addr.s_addr, src_port, msg.get_msg_id(), msg.get_sender_id());

            std::cout << "Delivered message with ID: " << msg.get_msg_id()
                      << " from sender: " << msg.get_sender_id()
                      << ", content: " << msg.get_msg()
                      << ", from address: " << inet_ntoa(src_addr)
                      << ":" << ntohs(src_port) << "\n";
        }
    }
}

void PerfectLinks::sendAck(in_addr_t dest_addr, unsigned short dest_port, uint32_t msg_id, unsigned long sender_id)
{
    Message ack_msg(msg_id, sender_id, true);

    size_t buffer_size;
    std::unique_ptr<char[]> buffer(Message::serialize(ack_msg, buffer_size));

    fairLossLinks.send(dest_addr, dest_port, buffer.get(), buffer_size);
}