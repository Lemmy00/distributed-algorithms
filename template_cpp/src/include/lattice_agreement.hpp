#pragma once

#include "best_effort_broadcast.hpp"
#include "message_batch.hpp"
#include "parser.hpp"
#include "proposal_message.hpp"

#include <map>
#include <unordered_set>
#include <unordered_map>
#include <functional>
#include <atomic>
#include <mutex>
#include <queue>
#include <utility>
#include <string>
#include <condition_variable>
#include <cmath>
#include <stdexcept>
#include <chrono>

class LatticeAgreement
{
private:
    const uint8_t sender_id;
    std::function<void(const std::string &decodedProposal)> deliverCallback;
    BestEffortBroadcast beb;

    std::unordered_map<uint32_t, std::unordered_set<int32_t>> proposals;
    std::unordered_map<uint32_t, std::unordered_set<int32_t>> accepted_values;
    std::unordered_map<uint32_t, bool> active;
    std::unordered_map<uint32_t, uint32_t> active_proposal_number;
    std::unordered_map<uint32_t, uint8_t> num_acks;
    std::mutex lattice_mutex;
    std::mutex accpeted_values_mutex;

    std::uint8_t threshold_acks;
    std::map<uint32_t, std::string> pendintgDecisions;

    size_t queue_buffer_size;
    size_t sequence_buffer_max_latency;
    uint32_t next_to_deliver = 1;
    std::condition_variable cv;

public:
    LatticeAgreement(uint8_t sender_id, in_addr_t ip, unsigned short port,
                     const std::unordered_map<uint8_t, Parser::Host> &processes,
                     std::function<void(const std::string &)> deliverCallback);
    ~LatticeAgreement();

    void startBroadcaster(size_t numReceivers = 3) { beb.startBroadcaster(numReceivers); }
    void stop() { beb.stop(); }
    std::atomic<bool> &getStopThreads() { return beb.getStopThreads(); }

    void propose(std::unordered_set<int32_t> &proposal, uint32_t seq_num);

private:
    void handleDeliver(const ProposalMessage &proposal);
    void handleAck(const ProposalMessage &proposal);
    void handleNack(const ProposalMessage &proposal);
    void handlePropose(const ProposalMessage &proposal);

    static void set_union(std::unordered_set<int32_t> &a, const std::unordered_set<int32_t> &b);
    static bool is_subset_of(const std::unordered_set<int32_t> &a, const std::unordered_set<int32_t> &b);
    static size_t calculate_buffer_size(size_t max_buffer, double decay_rate, double number_processes, size_t min_size);
};

LatticeAgreement::LatticeAgreement(uint8_t sender_id, in_addr_t ip, unsigned short port,
                                   const std::unordered_map<uint8_t, Parser::Host> &processes,
                                   std::function<void(const std::string &)> deliverCallback)
    : sender_id(sender_id),
      deliverCallback(std::move(deliverCallback)),
      beb(sender_id, ip, port, processes, [this](const ProposalMessage &proposal)
          { this->handleDeliver(proposal); }),
      threshold_acks(static_cast<uint8_t>(processes.size() / 2))
{
    double number_processes = static_cast<double>(processes.size());
    sequence_buffer_max_latency = calculate_buffer_size(300, 0.03, number_processes, 20);
    queue_buffer_size = 5; // calculate_buffer_size(800000, 0.1, number_processes, 5000);

    std::cout << "Queue buffer size: " << queue_buffer_size << "\n";
    std::cout << "Sequence buffer max latency: " << sequence_buffer_max_latency << "\n\n";
}

LatticeAgreement::~LatticeAgreement()
{
    stop();
    cv.notify_all();
}

void LatticeAgreement::propose(std::unordered_set<int32_t> &proposal, uint32_t seq_num)
{
    {
        std::lock_guard<std::mutex> lock(accpeted_values_mutex);
        if (accepted_values.find(seq_num) != accepted_values.end())
        {
            set_union(proposal, accepted_values[seq_num]);
        }
    }

    {
        std::unique_lock<std::mutex> lock(lattice_mutex);
        cv.wait(lock, [this, seq_num]()
                { return getStopThreads() || (seq_num < next_to_deliver + sequence_buffer_max_latency); });

        proposals[seq_num] = proposal;
        active[seq_num] = true;
        active_proposal_number[seq_num] = 1;
        num_acks[seq_num] = 0;
    }

    ProposalMessage propose_msg(sender_id, seq_num, active_proposal_number[seq_num], proposal, PROPOSAL_TYPE_PROPOSE);
    handleDeliver(propose_msg);
    beb.broadcast(propose_msg);
}

void LatticeAgreement::handleDeliver(const ProposalMessage &proposal)
{
    if (proposal.getType() == PROPOSAL_TYPE_PROPOSE)
    {
        handlePropose(proposal);
    }
    else if (proposal.getType() == PROPOSAL_TYPE_ACK)
    {
        handleAck(proposal);
    }
    else if (proposal.getType() == PROPOSAL_TYPE_NACK)
    {
        handleNack(proposal);
    }
    else
    {
        throw std::runtime_error("Invalid proposal type.");
    }
}

void LatticeAgreement::handlePropose(const ProposalMessage &proposal)
{
    bool is_subset;
    {
        std::lock_guard<std::mutex> lock(accpeted_values_mutex);
        is_subset = is_subset_of(accepted_values[proposal.getSeqNum()], proposal.getProposal());
    }

    if (is_subset)
    {
        {
            std::lock_guard<std::mutex> lock(accpeted_values_mutex);
            accepted_values[proposal.getSeqNum()] = proposal.getProposal();
        }
        ProposalMessage ack_msg(sender_id, proposal.getSeqNum(), proposal.getActiveProposalNumber(), PROPOSAL_TYPE_ACK);

        if (proposal.getSenderId() == sender_id)
        {
            handleAck(ack_msg);
        }
        else
        {
            beb.send(ack_msg, proposal.getSenderId());
        }
    }
    else
    {
        ProposalMessage nack_msg(sender_id, proposal.getSeqNum(), proposal.getActiveProposalNumber(), PROPOSAL_TYPE_NACK);
        {
            std::lock_guard<std::mutex> lock(accpeted_values_mutex);
            set_union(accepted_values[proposal.getSeqNum()], proposal.getProposal());
            nack_msg.setAcceptedValues(accepted_values[proposal.getSeqNum()]);
        }

        if (proposal.getSenderId() == sender_id)
        {
            handleNack(nack_msg);
        }
        else
        {
            beb.send(nack_msg, proposal.getSenderId());
        }
    }
}

void LatticeAgreement::handleAck(const ProposalMessage &proposal)
{
    std::unique_lock<std::mutex> lock(lattice_mutex);

    if (!active[proposal.getSeqNum()])
    {
        return;
    }

    if (proposal.getActiveProposalNumber() != active_proposal_number[proposal.getSeqNum()])
    {
        return;
    }

    num_acks[proposal.getSeqNum()]++;
    if (num_acks[proposal.getSeqNum()] > threshold_acks)
    {
        // save proposal
        std::string decodedProposal = ProposalMessage::decodeProposal(proposals[proposal.getSeqNum()]);

        // turn off active
        active[proposal.getSeqNum()] = false;

        // erase proposal
        proposals.erase(proposal.getSeqNum());
        active_proposal_number.erase(proposal.getSeqNum());
        num_acks.erase(proposal.getSeqNum());

        // place decision to wait for its turn
        pendintgDecisions[proposal.getSeqNum()] = decodedProposal;

        // try to deliver
        auto it = pendintgDecisions.cbegin();
        while (it != pendintgDecisions.end() && it->first == next_to_deliver)
        {
            // std::cout << "Delivering, round " << it->first << " proposal: " << it->second << std::endl;

            deliverCallback(it->second);
            next_to_deliver++;
            pendintgDecisions.erase(it);
            it = pendintgDecisions.cbegin();
        }
        cv.notify_all();
    }
}

void LatticeAgreement::handleNack(const ProposalMessage &proposal)
{
    {
        std::unique_lock<std::mutex> lock(lattice_mutex);
        if (!active[proposal.getSeqNum()])
        {
            return;
        }

        if (proposal.getActiveProposalNumber() != active_proposal_number[proposal.getSeqNum()])
        {
            return;
        }

        set_union(proposals[proposal.getSeqNum()], proposal.getProposal());

        active_proposal_number[proposal.getSeqNum()]++;
        num_acks[proposal.getSeqNum()] = 0;
    }

    ProposalMessage propose_msg(sender_id, proposal.getSeqNum(), active_proposal_number[proposal.getSeqNum()], proposals[proposal.getSeqNum()], PROPOSAL_TYPE_PROPOSE);
    handleDeliver(propose_msg);
    beb.broadcast(propose_msg);
}

void LatticeAgreement::set_union(std::unordered_set<int32_t> &a, const std::unordered_set<int32_t> &b)
{
    a.insert(b.begin(), b.end());
}

// adapted from https://stackoverflow.com/questions/48299390/check-if-unordered-set-contains-all-elements-in-other-unordered-set-c
bool LatticeAgreement::is_subset_of(const std::unordered_set<int32_t> &a, const std::unordered_set<int32_t> &b)
{
    // return true if all members of a are also in b
    if (a.size() > b.size())
    {
        return false;
    }

    auto const not_found = b.end();
    for (auto const &element : a)
    {
        if (b.find(element) == not_found)
        {
            return false;
        }
    }

    return true;
}

size_t LatticeAgreement::calculate_buffer_size(size_t max_buffer, double decay_rate, double number_processes, size_t min_size)
{
    double buffer_size_double = static_cast<double>(max_buffer) * std::exp(-decay_rate * number_processes);
    size_t buffer_size = static_cast<size_t>(buffer_size_double);
    return std::max(buffer_size, min_size);
}
