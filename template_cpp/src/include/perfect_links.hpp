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
#include "message_batch.hpp"
#include "ts_queue.hpp"

#define BUFFER_SIZE 512

class PerfectLinks
{
private:
    FairLossLinks fairLossLinks;
    std::unordered_map<uint8_t, std::unordered_set<uint64_t>> receivedMessages;
    std::unordered_set<uint64_t> ackedMessages;
    std::mutex ackedMessagesMutex;
    std::mutex receivedMessagesMutex;

    TSQueue<MessageBatch> messageQueue;
    std::atomic<bool> stopThreads{false};
    std::function<void(const MessageBatch &)> deliverCallback;

public:
    PerfectLinks(in_addr_t ip, unsigned short port, std::function<void(const MessageBatch &)> deliverCallback);
    ~PerfectLinks();

    void send(const MessageBatch &msg);

    std::vector<std::thread> startReceivers(size_t n);
    std::thread startSender();
    std::thread startReceiver();

    void stop();
    std::atomic<bool> &getStopThreads() { return stopThreads; }
    size_t getQueueSize() { return messageQueue.size(); }

private:
    void sendWorker();
    void receiverWorker();
    void sendAck(const MessageBatch &ack_msg);
};

PerfectLinks::PerfectLinks(in_addr_t ip, unsigned short port, std::function<void(const MessageBatch &)> deliverCallback)
    : fairLossLinks(ip, port), deliverCallback(std::move(deliverCallback)) {}

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

std::thread PerfectLinks::startReceiver()
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

void PerfectLinks::send(const MessageBatch &msgBatch)
{
    if (stopThreads)
    {
        return;
    }

    /*while (messageQueue.full())
    {
    }*/

    // std::cout << "Sending message with ID: " << msgBatch.get_messages().front().get_msg_id() << " with content: " << msgBatch.get_messages().front().get_msg() << "\n";
    size_t buffer_size;
    std::unique_ptr<char[]> buffer(MessageBatch::serialize(msgBatch, buffer_size));

    fairLossLinks.send(msgBatch.get_dest_addr(), msgBatch.get_dest_port(), buffer.get(), buffer_size);
    messageQueue.push(msgBatch);
}

void PerfectLinks::sendWorker()
{
    while (!stopThreads)
    {
        auto msgBatchOpt = messageQueue.pop();
        if (!msgBatchOpt.has_value())
        {
            continue;
        }

        const MessageBatch &msgBatch = msgBatchOpt.value();
        Message front = msgBatch.get_messages().front();

        {
            std::lock_guard<std::mutex> lock(ackedMessagesMutex);
            if (ackedMessages.find(front.get_msg_id()) != ackedMessages.end())
            {
                ackedMessages.erase(front.get_msg_id());
                continue;
            }
        }

        size_t buffer_size;
        std::unique_ptr<char[]> buffer(MessageBatch::serialize(msgBatch, buffer_size));
        std::cout << buffer_size << "\n";

        fairLossLinks.send(msgBatch.get_dest_addr(), msgBatch.get_dest_port(), buffer.get(), buffer_size);
        messageQueue.push(msgBatch);
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

        if (stopThreads)
        {
            break;
        }

        if (recv_size > 0)
        {
            MessageBatch msgBatch = MessageBatch::deserialize(buffer, recv_size, fairLossLinks.get_ip(), fairLossLinks.get_port());
            if (msgBatch.get_messages().empty())
            {
                continue;
            }

            Message front = msgBatch.get_messages().front();
            if (front.get_is_ack())
            {
                std::lock_guard<std::mutex> lock(ackedMessagesMutex);
                /*for (const auto &msg : msgBatch.get_messages())
                {
                    if (!msg.get_is_ack())
                    {
                        std::runtime_error("Received a message in an ACK batch");
                    }
                }*/
                // std::cout << "Received ACK for message with ID: " << front.get_msg_id() << "\n";
                ackedMessages.insert(front.get_msg_id());
                continue;
            }

            MessageBatch ack_msgs(src_addr.s_addr, src_port);
            ack_msgs.add_message(Message(front.get_msg_id(), front.get_sender_id(), true));
            sendAck(ack_msgs);

            {
                std::lock_guard<std::mutex> lock(receivedMessagesMutex);
                auto it = receivedMessages.find(front.get_sender_id());
                if (it != receivedMessages.end() && it->second.find(front.get_msg_id()) != it->second.end())
                {
                    continue;
                }

                receivedMessages[front.get_sender_id()].insert(front.get_msg_id());
            }

            if (stopThreads)
            {
                break;
            }

            deliverCallback(msgBatch);

            /*for (const auto &msg : msgBatch.get_messages())
            {
                logger->log("d " + std::to_string(msg.get_sender_id()) + " " + msg.get_msg());
            }*/

            /*std::cout << "Delivered message with ID: " << front.get_msg_id()
                      << " from sender: " << front.get_sender_id()
                      << ", content: " << front.get_msg()
                      << ", from address: " << inet_ntoa(src_addr)
                      << ":" << ntohs(src_port) << "\n";*/
        }
    }
}

void PerfectLinks::sendAck(const MessageBatch &ack_msgs)
{
    size_t buffer_size;
    std::unique_ptr<char[]> buffer(MessageBatch::serialize(ack_msgs, buffer_size));

    fairLossLinks.send(ack_msgs.get_dest_addr(), ack_msgs.get_dest_port(), buffer.get(), buffer_size);
}
