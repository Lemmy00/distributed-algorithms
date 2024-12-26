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
#include <cmath>

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
    size_t sequence_buffer_max_latency = 100;
    size_t queue_buffer_size = 1000;

    uint8_t sender_id;
    UniformReliableBroadcast uniformReliableBroadcast;
    std::unordered_map<uint8_t, Parser::Host> processes;

    std::atomic<uint32_t> lsn{0};
    std::unordered_map<uint8_t, std::priority_queue<MessageBatch, std::vector<MessageBatch>, PendingMessageComparator>> pendingMessages;
    std::unordered_map<uint8_t, std::atomic<uint32_t>> nextSequenceNumber;

    std::mutex delevringMutex;
    std::unordered_map<uint8_t, std::shared_ptr<std::mutex>> pendingMessagesLocks;
    std::condition_variable cv;

public:
    FIFOReliableBroadcast(uint8_t sender_id, in_addr_t ip, unsigned short port, const std::unordered_map<uint8_t, Parser::Host> &processes, std::function<void(const MessageBatch &)> deliverCallback);
    ~FIFOReliableBroadcast();

    void broadcast(const std::vector<std::string> &msgs);
    void startBroadcaster(size_t numReceivers = 3);
    void stop();

    std::atomic<bool> &getStopThreads() { return uniformReliableBroadcast.getStopThreads(); }

private:
    void handleDeliver(const MessageBatch &msgBatch, const std::function<void(const MessageBatch &)> &deliverCallback);
    static size_t calculate_buffer_size(size_t max_buffer, double decay_rate, double number_processes, size_t min_size);
};

FIFOReliableBroadcast::FIFOReliableBroadcast(uint8_t sender_id, in_addr_t ip, unsigned short port, const std::unordered_map<uint8_t, Parser::Host> &processes, std::function<void(const MessageBatch &)> deliverCallback)
    : sender_id(sender_id), uniformReliableBroadcast(sender_id, ip, port, processes, [this, deliverCallback](const MessageBatch &msgBatch)
                                                     { this->handleDeliver(msgBatch, deliverCallback); }),
      processes(processes)
{
    for (const auto &process : processes)
    {
        nextSequenceNumber[process.first] = 1;
        pendingMessagesLocks[process.first] = std::make_shared<std::mutex>();
    }
    pendingMessages.reserve(processes.size());

    // Calculate buffer sizes based on number of processes
    double number_processes = static_cast<double>(processes.size());

    queue_buffer_size = calculate_buffer_size(300000, 0.08, number_processes, 1000);
    sequence_buffer_max_latency = calculate_buffer_size(200, 0.03, number_processes, 20);

    std::cout << "Queue buffer size: " << queue_buffer_size << "\n";
    std::cout << "Sequence buffer max latency: " << sequence_buffer_max_latency << "\n";
}

size_t FIFOReliableBroadcast::calculate_buffer_size(size_t max_buffer, double decay_rate, double number_processes, size_t min_size)
{
    double buffer_size_double = static_cast<double>(max_buffer) * std::exp(-decay_rate * number_processes);
    size_t buffer_size = static_cast<size_t>(buffer_size_double);
    return std::max(buffer_size, min_size);
}

FIFOReliableBroadcast::~FIFOReliableBroadcast()
{
    stop();
}

void FIFOReliableBroadcast::stop()
{
    uniformReliableBroadcast.stop();
    cv.notify_all();

    // std::cout << "End Queue size: " << uniformReliableBroadcast.getQueueSize() << "\n";
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
                { return getStopThreads() || (nextSequenceNumber[sender_id].load() + sequence_buffer_max_latency > lsn.load()); });
    }

    // std::cout << "Queue size: " << uniformReliableBroadcast.getQueueSize() << "\n";
    while (uniformReliableBroadcast.getQueueSize() > queue_buffer_size)
    {
    }

    lsn++;
    uniformReliableBroadcast.broadcast({sender_id, lsn.load()}, msgs);
}

void FIFOReliableBroadcast::handleDeliver(const MessageBatch &msgBatch, const std::function<void(const MessageBatch &)> &deliverCallback)
{
    const std::pair<uint8_t, uint32_t> &batch_key = msgBatch.get_batch_key();
    const uint8_t sender_process = batch_key.first;

    std::shared_ptr<std::mutex> lockPtr = pendingMessagesLocks[sender_process];
    std::lock_guard<std::mutex> lock(*lockPtr);
    pendingMessages[sender_process].push(msgBatch);

    if (pendingMessages[sender_process].top().get_seq_num() < nextSequenceNumber[sender_process].load())
    {
        throw std::runtime_error("Received message with sequence number less than expected");
    }

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
