#pragma once

#include <vector>
#include <thread>
#include <atomic>
#include <utility>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <condition_variable>
#include <set>
#include <mutex>
#include <string>

#include "parser.hpp"
#include "message.hpp"
#include "message_batch.hpp"
#include "uniform_reliable_broadcast.hpp"

#define SEQUENCE_BUFFER_SIZE 100

// solution for comparator from https://stackoverflow.com/questions/2620862/using-custom-stdset-comparator
struct PendingMessageComparator
{
    bool operator()(const MessageBatch &a, const MessageBatch &b) const
    {
        return a.get_seq_num() < b.get_seq_num();
    }
};

class FIFOReliableBroadcast
{
private:
    unsigned long sender_id;
    UniformReliableBroadcast uniformReliableBroadcast;
    std::unordered_map<unsigned long, Parser::Host> processes;

    std::atomic<uint32_t> lsn{0};
    std::unordered_map<unsigned long, std::set<MessageBatch, PendingMessageComparator>> pendingMessages;
    std::unordered_map<unsigned long, uint32_t> nextSequenceNumber;

    std::mutex delevringMutex;
    std::unordered_map<unsigned long, std::shared_ptr<std::mutex>> pendingMessagesLocks;
    std::condition_variable cv;

public:
    FIFOReliableBroadcast(unsigned long sender_id, in_addr_t ip, unsigned short port, const std::unordered_map<unsigned long, Parser::Host> &processes, size_t queueSize, std::function<void(const MessageBatch &)> deliverCallback);
    ~FIFOReliableBroadcast();

    void broadcast(const std::vector<std::string> &msgs);
    void startBroadcaster(size_t numReceivers = 3);
    void stop();

    std::atomic<bool> &getStopThreads() { return uniformReliableBroadcast.getStopThreads(); }

private:
    void handleDeliver(const MessageBatch &msgBatch, const std::function<void(const MessageBatch &)> &deliverCallback);
};

FIFOReliableBroadcast::FIFOReliableBroadcast(unsigned long sender_id, in_addr_t ip, unsigned short port, const std::unordered_map<unsigned long, Parser::Host> &processes, size_t queueSize, std::function<void(const MessageBatch &)> deliverCallback)
    : sender_id(sender_id), uniformReliableBroadcast(sender_id, ip, port, processes, queueSize, [this, deliverCallback](const MessageBatch &msgBatch)
                                                     { this->handleDeliver(msgBatch, deliverCallback); }),
      processes(processes)
{
    for (const auto &process : processes)
    {
        nextSequenceNumber[process.first] = 1;
        pendingMessagesLocks[process.first] = std::make_shared<std::mutex>();
    }
    nextSequenceNumber.reserve(processes.size());
}

FIFOReliableBroadcast::~FIFOReliableBroadcast()
{
    stop();
}

void FIFOReliableBroadcast::stop()
{
    uniformReliableBroadcast.stop();
    for (const auto &process : processes)
    {
        std::shared_ptr<std::mutex> lockPtr = pendingMessagesLocks[process.first];
        std::lock_guard<std::mutex> lock(*lockPtr);
        if (pendingMessages[process.first].empty())
        {
            continue;
        }

        std::cout << "----------------------------------\n";
        std::cout << "Pending messages for process " << process.first << ":\n";
        for (const auto &msgBatch : pendingMessages[process.first])
        {
            std::cout << "  Sequence number: " << msgBatch.get_sender_id() << " " << msgBatch.get_seq_num() << ", content: " << msgBatch.get_messages().front().get_msg() << "\n";
        }
        std::cout << "----------------------------------\n";
    }
}

void FIFOReliableBroadcast::startBroadcaster(size_t numReceivers)
{
    uniformReliableBroadcast.startBroadcaster(numReceivers);
}

void FIFOReliableBroadcast::broadcast(const std::vector<std::string> &msgs)
{
    {
        unsigned long process_id = sender_id;
        uint32_t min_seq_num = nextSequenceNumber[sender_id];
        for (const auto &process : processes)
        {
            if (nextSequenceNumber[process.first] < min_seq_num)
            {
                min_seq_num = nextSequenceNumber[process.first];
                process_id = process.first;
            }
        }

        {
            std::shared_ptr<std::mutex> lockPtr = pendingMessagesLocks[process_id];
            std::unique_lock<std::mutex> lock(*lockPtr);
            cv.wait(lock, [this, process_id]()
                    { return nextSequenceNumber[process_id] + SEQUENCE_BUFFER_SIZE > lsn.load(); });
        }
    }

    lsn++;
    uniformReliableBroadcast.broadcast({sender_id, lsn.load()}, msgs);
}

void FIFOReliableBroadcast::handleDeliver(const MessageBatch &msgBatch, const std::function<void(const MessageBatch &)> &deliverCallback)
{
    const std::pair<unsigned long, uint32_t> &batch_key = msgBatch.get_batch_key();
    const unsigned long sender_process = batch_key.first;
    std::vector<MessageBatch> toDeliver;

    {
        std::shared_ptr<std::mutex> lockPtr = pendingMessagesLocks[sender_process];
        std::lock_guard<std::mutex> lock(*lockPtr);
        pendingMessages[sender_process].insert(msgBatch);

        auto it = pendingMessages[sender_process].begin();
        while (it != pendingMessages[sender_process].end() && it->get_seq_num() == nextSequenceNumber[sender_process])
        {
            if (this->getStopThreads())
            {
                break;
            }

            MessageBatch front = *it;
            pendingMessages[sender_process].erase(it);
            nextSequenceNumber[sender_process]++;

            toDeliver.push_back(front);

            it = pendingMessages[sender_process].begin();
        }
        cv.notify_all();
    }

    {
        std::lock_guard<std::mutex> lock(delevringMutex);
        for (const auto &msgBatch : toDeliver)
        {
            deliverCallback(msgBatch);
        }
    }
}
