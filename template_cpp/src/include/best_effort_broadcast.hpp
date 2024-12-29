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
#include "proposal_message.hpp"

class BestEffortBroadcast
{
private:
    const uint8_t sender_id;

    PerfectLinks perfectLinks;
    std::unordered_map<uint8_t, Parser::Host> processes;

    std::thread broadcasterThread;
    std::vector<std::thread> receiverThreads;

public:
    BestEffortBroadcast(uint8_t sender_id, in_addr_t ip, unsigned short port, const std::unordered_map<uint8_t, Parser::Host> &processes, std::function<void(const ProposalMessage &)> deliverCallback);
    ~BestEffortBroadcast();

    void broadcast(const ProposalMessage &proposal);
    void send(const ProposalMessage &proposal, uint8_t id);

    void startBroadcaster(size_t numReceivers = 3);
    void stop();

    std::atomic<bool> &getStopThreads() { return perfectLinks.getStopThreads(); }
    size_t getQueueSize() { return perfectLinks.getQueueSize(); }
};

BestEffortBroadcast::BestEffortBroadcast(uint8_t sender_id, in_addr_t ip, unsigned short port, const std::unordered_map<uint8_t, Parser::Host> &processes, std::function<void(const ProposalMessage &)> deliverCallback)
    : sender_id(sender_id), perfectLinks(sender_id, ip, port, deliverCallback), processes(processes)
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

void BestEffortBroadcast::broadcast(const ProposalMessage &proposal)
{
    for (auto &[id, process] : processes)
    {
        if (getStopThreads())
        {
            break;
        }

        if (id == sender_id)
        {
            continue;
        }

        perfectLinks.send(proposal.toMessage(id, process.ip, process.port));
    }
}

void BestEffortBroadcast::send(const ProposalMessage &proposal, uint8_t id)
{
    perfectLinks.send(proposal.toMessage(id, processes[id].ip, processes[id].port));
}