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
#include <queue>

#include "parser.hpp"
#include "message.hpp"
#include "message_batch.hpp"
#include "uniform_reliable_broadcast.hpp"

// solution for comparator from https://stackoverflow.com/questions/2620862/using-custom-stdset-comparator
struct PendingMessageComparator
{
    bool operator()(const MessageBatch &a, const MessageBatch &b) const
    {
        return a.get_seq_num() > b.get_seq_num();
    }
};

class FIFOReliableBroadcast
{
private:
    const size_t SEQUENCE_BUFFER_SIZE = 100;

    unsigned long sender_id;
    UniformReliableBroadcast uniformReliableBroadcast;
    std::unordered_map<unsigned long, Parser::Host> processes;

    std::atomic<uint32_t> lsn{0};
    std::unordered_map<unsigned long, std::priority_queue<MessageBatch, std::vector<MessageBatch>, PendingMessageComparator>> pendingMessages;
    std::unordered_map<unsigned long, std::atomic<uint32_t>> nextSequenceNumber;

    std::mutex delevringMutex;
    std::unordered_map<unsigned long, std::shared_ptr<std::mutex>> pendingMessagesLocks;
    std::condition_variable cv;

public:
    FIFOReliableBroadcast(unsigned long sender_id, in_addr_t ip, unsigned short port, const std::unordered_map<unsigned long, Parser::Host> &processes, std::function<void(const MessageBatch &)> deliverCallback);
    ~FIFOReliableBroadcast();

    void broadcast(const std::vector<std::string> &msgs);
    void startBroadcaster(size_t numReceivers = 3);
    void stop();

    std::atomic<bool> &getStopThreads() { return uniformReliableBroadcast.getStopThreads(); }

private:
    void handleDeliver(const MessageBatch &msgBatch, const std::function<void(const MessageBatch &)> &deliverCallback);
};

FIFOReliableBroadcast::FIFOReliableBroadcast(unsigned long sender_id, in_addr_t ip, unsigned short port, const std::unordered_map<unsigned long, Parser::Host> &processes, std::function<void(const MessageBatch &)> deliverCallback)
    : sender_id(sender_id), uniformReliableBroadcast(sender_id, ip, port, processes, [this, deliverCallback](const MessageBatch &msgBatch)
                                                     { this->handleDeliver(msgBatch, deliverCallback); }),
      processes(processes)
{
    for (const auto &process : processes)
    {
        nextSequenceNumber[process.first] = 1;
        pendingMessagesLocks[process.first] = std::make_shared<std::mutex>();
    }
    pendingMessagesLocks.reserve(processes.size());
}

FIFOReliableBroadcast::~FIFOReliableBroadcast()
{
    stop();
}

void FIFOReliableBroadcast::stop()
{
    uniformReliableBroadcast.stop();
    cv.notify_all();
}

void FIFOReliableBroadcast::startBroadcaster(size_t numReceivers)
{
    uniformReliableBroadcast.startBroadcaster(numReceivers);
}

void FIFOReliableBroadcast::broadcast(const std::vector<std::string> &msgs)
{
    {
        std::shared_ptr<std::mutex> lockPtr = pendingMessagesLocks[sender_id];
        std::unique_lock<std::mutex> lock(*lockPtr);

        cv.wait(lock, [this]()
                { return getStopThreads() || (nextSequenceNumber[sender_id].load() + SEQUENCE_BUFFER_SIZE > lsn.load()); });
    }

    lsn++;
    uniformReliableBroadcast.broadcast({sender_id, lsn.load()}, msgs);
}

void FIFOReliableBroadcast::handleDeliver(const MessageBatch &msgBatch, const std::function<void(const MessageBatch &)> &deliverCallback)
{
    const std::pair<unsigned long, uint32_t> &batch_key = msgBatch.get_batch_key();
    const unsigned long sender_process = batch_key.first;

    std::shared_ptr<std::mutex> lockPtr = pendingMessagesLocks[sender_process];
    std::lock_guard<std::mutex> lock(*lockPtr);
    pendingMessages[sender_process].push(msgBatch);

    while (!pendingMessages[sender_process].empty() &&
           pendingMessages[sender_process].top().get_seq_num() == nextSequenceNumber[sender_process].load())
    {
        if (this->getStopThreads())
        {
            break;
        }

        MessageBatch front = pendingMessages[sender_process].top();
        pendingMessages[sender_process].pop();
        nextSequenceNumber[sender_process]++;

        deliverCallback(front);
    }
    cv.notify_all();
}
