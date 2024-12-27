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
#include "message_batch.hpp"
#include "ts_queue.hpp"
#include "message.hpp"
#include "proposal_message.hpp"

#define BUFFER_SIZE 16384

// hash solution from https://stackoverflow.com/questions/20590656/how-to-solve-error-for-hash-function-of-pair-of-ints-in-unordered-map
struct pairhash
{
public:
    template <typename T, typename U>
    std::size_t operator()(const std::pair<T, U> &x) const
    {
        return std::hash<T>()(x.first) ^ (std::hash<U>()(x.second) << 1);
    }
};

class PerfectLinks
{
private:
    uint8_t sender_id;

    FairLossLinks fairLossLinks;
    // std::unordered_map<std::pair<uint8_t, uint32_t>, std::chrono::steady_clock::time_point, pairhash>
    std::unordered_map<uint8_t, std::unordered_set<uint64_t>> receivedMessages;
    std::unordered_map<uint8_t, std::unordered_set<uint64_t>> ackedMessages;
    std::mutex ackedMessagesMutex;
    std::mutex receivedMessagesMutex;

    TSQueue<Message> messageQueue;
    std::atomic<bool> stopThreads{false};
    std::function<void(const ProposalMessage &)> deliverCallback;

public:
    PerfectLinks(uint8_t sender_id, in_addr_t ip, unsigned short port, std::function<void(const ProposalMessage &)> deliverCallback);
    ~PerfectLinks();

    void send(const Message &msg);

    std::vector<std::thread> startReceivers(size_t n);
    std::thread startSender();
    std::thread startReceiver();

    void stop();
    std::atomic<bool> &getStopThreads() { return stopThreads; }
    size_t getQueueSize() { return messageQueue.size(); }

private:
    void sendWorker();
    void receiverWorker();
    void sendAck(const Message &ack_msg);
};

PerfectLinks::PerfectLinks(uint8_t sender_id, in_addr_t ip, unsigned short port, std::function<void(const ProposalMessage &)> deliverCallback)
    : sender_id(sender_id), fairLossLinks(ip, port), deliverCallback(std::move(deliverCallback)) {}

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

void PerfectLinks::send(const Message &msg)
{
    if (stopThreads)
    {
        return;
    }

    // std::cout << "Sending message with ID: " << msgBatch.get_messages().front().get_msg_id() << " with content: " << msgBatch.get_messages().front().get_msg() << "\n";
    size_t buffer_size;
    std::unique_ptr<char[]> buffer(Message::serialize(msg, buffer_size));

    fairLossLinks.send(msg.get_dest_addr(), msg.get_dest_port(), buffer.get(), buffer_size);
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

        const Message &msg = msgOpt.value();
        {
            std::lock_guard<std::mutex> lock(ackedMessagesMutex);
            auto it = ackedMessages.find(msg.get_dest_id());
            if (it != ackedMessages.end() && it->second.find(msg.get_message_key()) != it->second.end())
            {
                ackedMessages[msg.get_dest_id()].erase(msg.get_message_key());
                continue;
            }
        }

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

        if (stopThreads)
        {
            break;
        }

        if (recv_size > 0)
        {
            Message msg = Message::deserialize(buffer, recv_size, fairLossLinks.get_ip(), fairLossLinks.get_port());
            if (msg.get_is_ack())
            {
                std::lock_guard<std::mutex> lock(ackedMessagesMutex);
                // std::cout << "Received ACK for message with ID: " << msgBatch.get_sender_id() << ":" << msgBatch.get_message_key().first << ":" << msgBatch.get_message_key().second << "\n";
                ackedMessages[msg.get_sender_id()].insert(msg.get_message_key());
                continue;
            }

            Message ack_msgs(msg.get_message_key(), this->sender_id, msg.get_sender_id(), src_addr.s_addr, src_port, true);
            sendAck(ack_msgs);

            {
                std::lock_guard<std::mutex> lock(receivedMessagesMutex);
                auto it = receivedMessages.find(msg.get_sender_id());
                if (it != receivedMessages.end() && it->second.find(msg.get_message_key()) != it->second.end())
                {
                    continue;
                }

                receivedMessages[msg.get_sender_id()].insert(msg.get_message_key());
            }

            if (stopThreads)
            {
                break;
            }

            deliverCallback(ProposalMessage::fromMessage(msg));

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

void PerfectLinks::sendAck(const Message &ack_msgs)
{
    size_t buffer_size;
    std::unique_ptr<char[]> buffer(Message::serialize(ack_msgs, buffer_size));

    fairLossLinks.send(ack_msgs.get_dest_addr(), ack_msgs.get_dest_port(), buffer.get(), buffer_size);
}
