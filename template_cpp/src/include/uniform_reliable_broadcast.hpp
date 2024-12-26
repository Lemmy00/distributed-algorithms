#pragma once

#include <vector>
#include <thread>
#include <atomic>
#include <utility>
#include <functional>
#include <unordered_map>
#include <unordered_set>
#include <mutex>
#include <string>

#include "parser.hpp"
#include "message.hpp"
#include "message_batch.hpp"
#include "best_effort_broadcast.hpp"

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

template <typename Set, typename Mutex>
bool safeInsertIfAbsent(Set &set, const typename Set::value_type &value, Mutex &mutex)
{
    std::lock_guard<std::mutex> lock(mutex);
    bool absent = set.find(value) == set.end();
    if (absent)
    {
        set.insert(value);
    }
    return absent;
}

class UniformReliableBroadcast
{
private:
    uint8_t sender_id;

    BestEffortBroadcast bestEffortBroadcast;
    std::unordered_map<uint8_t, Parser::Host> processes;

    std::unordered_set<std::pair<uint8_t, uint32_t>, pairhash> deliveredMessages;
    std::unordered_set<std::pair<uint8_t, uint32_t>, pairhash> pendingMessages;
    std::unordered_map<std::pair<uint8_t, uint32_t>, std::unordered_set<uint8_t>, pairhash> messageAcknowledgements;

    std::mutex pendingMessagesMutex;
    std::mutex deliveredMessagesMutex;
    std::mutex messageAcknowledgementsMutex;

public:
    UniformReliableBroadcast(uint8_t sender_id, in_addr_t ip, unsigned short port, const std::unordered_map<uint8_t, Parser::Host> &processes, const std::function<void(const MessageBatch &)> &deliverCallback);
    ~UniformReliableBroadcast();

    void broadcast(const std::pair<uint8_t, uint32_t> &batch_key, const std::vector<std::string> &msgs);

    void startBroadcaster(size_t numReceivers = 3);
    void stop();

    std::atomic<bool> &getStopThreads() { return bestEffortBroadcast.getStopThreads(); }
    size_t getQueueSize() { return bestEffortBroadcast.getQueueSize(); }

private:
    bool canDeliver(const std::pair<uint8_t, uint32_t> &batch_key);
    bool isNotPending(const std::pair<uint8_t, uint32_t> &batch_key);
    bool isNotDelivered(const std::pair<uint8_t, uint32_t> &batch_key);

    void handleDeliver(const MessageBatch &msgBatch, const std::function<void(const MessageBatch &)> &deliverCallback);
};

UniformReliableBroadcast::UniformReliableBroadcast(uint8_t sender_id, in_addr_t ip, unsigned short port, const std::unordered_map<uint8_t, Parser::Host> &processes, const std::function<void(const MessageBatch &)> &deliverCallback)
    : sender_id(sender_id), bestEffortBroadcast(sender_id, ip, port, processes, [this, deliverCallback](const MessageBatch &msgBatch)
                                                { this->handleDeliver(msgBatch, deliverCallback); }),
      processes(processes) {}

UniformReliableBroadcast::~UniformReliableBroadcast()
{
    stop();
}

void UniformReliableBroadcast::stop()
{
    bestEffortBroadcast.stop();
}

void UniformReliableBroadcast::startBroadcaster(size_t numReceivers)
{
    bestEffortBroadcast.startBroadcaster(numReceivers);
}

void UniformReliableBroadcast::broadcast(const std::pair<uint8_t, uint32_t> &batch_key, const std::vector<std::string> &msgs)
{
    safeInsertIfAbsent(pendingMessages, batch_key, pendingMessagesMutex);

    {
        std::lock_guard<std::mutex> lock(messageAcknowledgementsMutex);
        messageAcknowledgements[batch_key].insert(sender_id);
    }
    bestEffortBroadcast.broadcast(batch_key, msgs);
}

bool UniformReliableBroadcast::canDeliver(const std::pair<uint8_t, uint32_t> &batch_key)
{
    std::lock_guard<std::mutex> lock(messageAcknowledgementsMutex);
    return messageAcknowledgements.at(batch_key).size() > processes.size() / 2;
}

bool UniformReliableBroadcast::isNotPending(const std::pair<uint8_t, uint32_t> &batch_key)
{
    return safeInsertIfAbsent(pendingMessages, batch_key, pendingMessagesMutex);
}

bool UniformReliableBroadcast::isNotDelivered(const std::pair<uint8_t, uint32_t> &batch_key)
{
    return safeInsertIfAbsent(deliveredMessages, batch_key, deliveredMessagesMutex);
}

void UniformReliableBroadcast::handleDeliver(const MessageBatch &msgBatch, const std::function<void(const MessageBatch &)> &deliverCallback)
{
    const std::pair<uint8_t, uint32_t> &batch_key = msgBatch.get_batch_key();
    const uint8_t process_id = msgBatch.get_messages().front().get_sender_id();
    {
        std::lock_guard<std::mutex> lock(messageAcknowledgementsMutex);
        messageAcknowledgements[batch_key].insert(process_id);
    }
    if (isNotPending(batch_key))
    {
        {
            std::lock_guard<std::mutex> lock(messageAcknowledgementsMutex);
            messageAcknowledgements[batch_key].insert(sender_id);
        }
        bestEffortBroadcast.broadcast(batch_key, msgBatch.get_messages_str());
    }

    if (canDeliver(batch_key) && isNotDelivered(batch_key))
    {
        deliverCallback(msgBatch);
    }
}
