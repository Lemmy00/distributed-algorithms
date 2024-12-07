#pragma once

#include <unordered_map>
#include <vector>
#include <thread>
#include <atomic>
#include <functional>
#include <utility>
#include <string>

#include "perfect_links.hpp"
#include "parser.hpp"
#include "message.hpp"
#include "message_batch.hpp"

class BestEffortBroadcast
{
private:
    unsigned long sender_id;

    PerfectLinks perfectLinks;
    std::unordered_map<unsigned long, Parser::Host> processes;

    std::thread broadcasterThread;
    std::vector<std::thread> receiverThreads;

public:
    BestEffortBroadcast(unsigned long sender_id, in_addr_t ip, unsigned short port, const std::unordered_map<unsigned long, Parser::Host> &processes, size_t queueSize, std::function<void(const MessageBatch &)> deliverCallback);
    ~BestEffortBroadcast();

    void broadcast(const std::pair<unsigned long, uint32_t> &batch_key, const std::vector<std::string> &msgs);

    void startBroadcaster(size_t numReceivers = 3);
    void stop();

    std::atomic<bool> &getStopThreads() { return perfectLinks.getStopThreads(); }
};

BestEffortBroadcast::BestEffortBroadcast(unsigned long sender_id, in_addr_t ip, unsigned short port, const std::unordered_map<unsigned long, Parser::Host> &processes, size_t queueSize, std::function<void(const MessageBatch &)> deliverCallback)
    : sender_id(sender_id), perfectLinks(ip, port, queueSize, deliverCallback), processes(processes)
{
}

BestEffortBroadcast::~BestEffortBroadcast()
{
    stop();
}

void BestEffortBroadcast::startBroadcaster(size_t numReceivers)
{
    broadcasterThread = perfectLinks.startSender();
    broadcasterThread.detach();

    receiverThreads = perfectLinks.startReceivers(numReceivers);
    for (size_t i = 0; i < numReceivers; i++)
    {
        receiverThreads[i].detach();
    }
}

void BestEffortBroadcast::stop()
{
    perfectLinks.stop();
}

void BestEffortBroadcast::broadcast(const std::pair<unsigned long, uint32_t> &batch_key, const std::vector<std::string> &msgs)
{
    for (auto &[id, process] : processes)
    {
        if (id == sender_id)
        {
            continue;
        }

        MessageBatch batch(batch_key, sender_id, msgs, process.ip, process.port);

        if (perfectLinks.getStopThreads())
        {
            break;
        }

        perfectLinks.send(batch);
    }
}