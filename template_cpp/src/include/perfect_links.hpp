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
#include "logger.hpp"
#include "ts_queue.hpp"

#define BUFFER_SIZE 512

class PerfectLinks
{
private:
    FairLossLinks fairLossLinks;
    std::unordered_map<unsigned long, std::unordered_set<uint32_t>> deliveredMessages;
    std::unordered_set<uint32_t> ackedMessages;
    std::mutex ackedMessagesMutex;
    std::mutex deliveredMessagesMutex;

    TSQueue<MessageBatch> messageQueue;
    std::atomic<bool> stopThreads{false};

    std::shared_ptr<Logger> logger;

public:
    PerfectLinks(in_addr_t ip, unsigned short port, std::shared_ptr<Logger> loggerInstance, size_t queueSize = 100000);
    ~PerfectLinks();

    void send(const MessageBatch &msg);

    std::vector<std::thread> startReceivers(size_t n);
    std::thread startReciever();
    std::thread startSender();
    void stop();

private:
    void sendWorker();
    void receiverWorker();
    void sendAck(const MessageBatch &ack_msg);
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

void PerfectLinks::send(const MessageBatch &msgBatch)
{
    if (stopThreads)
    {
        return;
    }

    while (messageQueue.full())
    {
    }

    for (const auto &message : msgBatch.get_messages())
    {
        logger->log("b " + message.get_msg());
        // std::cout << "Sending message with ID: " << msg.get_msg_id() << " with content: " << msg.get_msg() << "\n";
    }

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

        MessageBatch msgBatch = msgBatchOpt.value();
        std::vector<Message> toSend;
        {
            std::lock_guard<std::mutex> lock(ackedMessagesMutex);
            for (const auto &msg : msgBatch.get_messages())
            {
                if (ackedMessages.find(msg.get_msg_id()) != ackedMessages.end())
                {
                    continue;
                }
                toSend.push_back(msg);
            }
        }
        // std::cout << "Sending message with ID: " << msg.get_msg_id() << " with content: " << msg.get_msg() << "\n";

        MessageBatch msgBatchToSend(toSend, msgBatch.get_dest_addr(), msgBatch.get_dest_port());
        size_t buffer_size;
        std::unique_ptr<char[]> buffer(MessageBatch::serialize(msgBatchToSend, buffer_size));

        fairLossLinks.send(msgBatchToSend.get_dest_addr(), msgBatchToSend.get_dest_port(), buffer.get(), buffer_size);
        messageQueue.push(msgBatchToSend);
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
            MessageBatch msgBatch = MessageBatch::deserialize(buffer, recv_size, fairLossLinks.get_ip(), fairLossLinks.get_port());
            if (msgBatch.get_messages().empty())
            {
                continue;
            }

            if (msgBatch.get_messages().front().get_is_ack())
            {
                std::lock_guard<std::mutex> lock(ackedMessagesMutex);
                for (const auto &msg : msgBatch.get_messages())
                {
                    if (!msg.get_is_ack())
                    {
                        std::runtime_error("Received a message in an ACK batch");
                    }

                    ackedMessages.insert(msg.get_msg_id());
                    // std::cout << "Received ACK for message with ID: " << msg.get_msg_id() << "\n";
                }
                continue;
            }

            MessageBatch ack_msgs(msgBatch.get_dest_addr(), msgBatch.get_dest_port());
            for (const auto &msg : msgBatch.get_messages())
            {
                ack_msgs.add_message(Message(msg.get_msg_id(), msg.get_sender_id(), true));
            }

            sendAck(ack_msgs);

            std::vector<Message> newDeliveredMessages;
            {
                std::lock_guard<std::mutex> lock(deliveredMessagesMutex);
                for (const auto &msg : msgBatch.get_messages())
                {
                    auto it = deliveredMessages.find(msg.get_sender_id());
                    if (it != deliveredMessages.end() && it->second.find(msg.get_msg_id()) != it->second.end())
                    {
                        continue;
                    }

                    deliveredMessages[msg.get_sender_id()].insert(msg.get_msg_id());
                    newDeliveredMessages.push_back(msg);
                }
            }

            for (const auto &msg : newDeliveredMessages)
            {
                logger->log("d " + std::to_string(msg.get_sender_id()) + " " + msg.get_msg());
                /*std::cout << "Delivered message with ID: " << msg.get_msg_id()
                      << " from sender: " << msg.get_sender_id()
                      << ", content: " << msg.get_msg()
                      << ", from address: " << inet_ntoa(src_addr)
                      << ":" << ntohs(src_port) << "\n";*/
            }
        }
    }
}

void PerfectLinks::sendAck(const MessageBatch &ack_msgs)
{
    size_t buffer_size;
    std::unique_ptr<char[]> buffer(MessageBatch::serialize(ack_msgs, buffer_size));

    fairLossLinks.send(ack_msgs.get_dest_addr(), ack_msgs.get_dest_port(), buffer.get(), buffer_size);
}
