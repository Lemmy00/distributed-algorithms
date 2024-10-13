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

#include "fair_loss_links.hpp"
#include "message.hpp"

#define BUFFER_SIZE 128

class PerfectLinks
{
private:
    FairLossLinks fairLossLinks;
    std::unordered_map<unsigned long, std::unordered_set<uint32_t>> deliveredMessages;
    std::mutex deliveredMessagesMutex;

    // Thread pool and task queue for sender threads
    std::vector<std::thread> senderThreads;
    std::queue<std::function<void()>> sendTasks;
    std::mutex queueMutex;
    std::condition_variable queueCV;
    bool stopThreads = false;

    // Receiver thread
    std::thread receiverThread;
    size_t numThreads;

public:
    PerfectLinks(char *ip, unsigned short port, size_t num_threads);
    PerfectLinks(in_addr_t ip, unsigned short port, size_t num_threads);
    ~PerfectLinks();

    void send(char *dest_addr, unsigned short dest_port, Message &msg);
    void send(in_addr_t dest_addr, unsigned short dest_port, Message &msg);

    void start();
    void stop();

private:
    void sendWorker();
    void receiverWorker();
};

PerfectLinks::PerfectLinks(char *ip, unsigned short port, size_t num_threads) : fairLossLinks(ip, port), numThreads(num_threads) {}
PerfectLinks::PerfectLinks(in_addr_t ip, unsigned short port, size_t num_threads) : fairLossLinks(ip, port), numThreads(num_threads) {}

PerfectLinks::~PerfectLinks()
{
    stop();
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
        receiverThread.join();
}

void PerfectLinks::send(char *dest_addr, unsigned short dest_port, Message &msg)
{
    auto msg_ptr = std::make_shared<Message>(msg);

    auto task = [this, dest_addr, dest_port, msg_ptr]()
    {
        size_t buffer_size;
        std::unique_ptr<char[]> buffer(Message::serialize(*msg_ptr, buffer_size));

        // Stubborn sending logic
        while (true)
        {
            fairLossLinks.send(dest_addr, dest_port, buffer.get(), buffer_size);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            // You can add a stopping condition if required
        }
    };

    {
        std::lock_guard<std::mutex> lock(queueMutex);
        sendTasks.push(task);
    }
    queueCV.notify_one();
}

void PerfectLinks::send(in_addr_t dest_addr, unsigned short dest_port, Message &msg)
{
    auto msg_ptr = std::make_shared<Message>(msg);

    auto task = [this, dest_addr, dest_port, msg_ptr]()
    {
        size_t buffer_size;
        std::unique_ptr<char[]> buffer(Message::serialize(*msg_ptr, buffer_size));

        // Stubborn sending logic
        while (true)
        {
            fairLossLinks.send(dest_addr, dest_port, buffer.get(), buffer_size);
            std::this_thread::sleep_for(std::chrono::milliseconds(100));

            // You can add a stopping condition if required
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
        in_addr_t src_addr[BUFFER_SIZE];
        unsigned short src_port;

        char buffer[BUFFER_SIZE];
        size_t buffer_size = BUFFER_SIZE;
        size_t recv_size = fairLossLinks.recv(buffer, buffer_size, src_addr, &src_port);

        if (recv_size > 0)
        {
            try
            {
                Message msg = Message::deserialize(buffer, recv_size);

                {
                    std::lock_guard<std::mutex> lock(deliveredMessagesMutex);
                    auto it = deliveredMessages.find(msg.get_sender_id());
                    if (it != deliveredMessages.end() && it->second.find(msg.get_msg_id()) != it->second.end())
                    {
                        continue;
                    }

                    deliveredMessages[msg.get_sender_id()].insert(msg.get_msg_id());
                }

                std::cout << "Delivered message with ID: " << msg.get_msg_id()
                          << " from sender: " << msg.get_sender_id()
                          << ", content: " << msg.get_msg()
                          << ", from address: " << inet_ntoa(*reinterpret_cast<in_addr *>(src_addr))
                          << ":" << ntohs(src_port) << "\n";
            }
            catch (const std::exception &e)
            {
                std::cerr << "Failed to deserialize message: " << e.what() << "\n";
            }
        }
    }
}
