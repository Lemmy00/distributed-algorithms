#pragma once

#include <cstdint>
#include <unordered_set>
#include <sstream>
#include <stdexcept>
#include <string>

#include "message.hpp"

enum ProposalType
{
    PROPOSAL_TYPE_PROPOSE = 0,
    PROPOSAL_TYPE_ACK = 1,
    PROPOSAL_TYPE_NACK = 2
};

class ProposalMessage
{
private:
    uint8_t sender_id;
    uint32_t seq_num;
    uint32_t active_proposal_number;
    std::unordered_set<int32_t> proposal;
    ProposalType type;

public:
    ProposalMessage(uint8_t sender_id, uint32_t seq_num, uint32_t active_proposal_number, const std::unordered_set<int32_t> &proposal, ProposalType type)
        : sender_id(sender_id), seq_num(seq_num), active_proposal_number(active_proposal_number), proposal(proposal), type(type) {}

    ProposalMessage(uint8_t sender_id, uint32_t seq_num, uint32_t active_proposal_number, ProposalType type)
        : sender_id(sender_id), seq_num(seq_num), active_proposal_number(active_proposal_number), type(type) {}

    ~ProposalMessage() = default;

    uint8_t getSenderId() const { return sender_id; }
    uint32_t getSeqNum() const { return seq_num; }
    uint32_t getActiveProposalNumber() const { return active_proposal_number; }
    const std::unordered_set<int32_t> &getProposal() const { return proposal; }
    ProposalType getType() const { return type; }

    std::string toString() const
    {
        std::string proposal_str;
        for (auto it = proposal.begin(); it != proposal.end(); ++it)
        {
            if (it != proposal.begin())
            {
                proposal_str += " ";
            }
            proposal_str += std::to_string(static_cast<unsigned int>(*it));
        }

        return std::to_string(seq_num) + " " +
               std::to_string(active_proposal_number) + " " +
               std::to_string(static_cast<unsigned int>(type)) + " " +
               proposal_str;
    }

    Message toMessage(
        uint8_t dest_id, in_addr_t dest_addr, unsigned short dest_port) const
    {
        std::string proposal_str;
        for (auto it = proposal.begin(); it != proposal.end(); ++it)
        {
            if (it != proposal.begin())
                proposal_str += " ";
            proposal_str += std::to_string(static_cast<unsigned int>(*it));
        }

        return Message(
            sender_id,
            std::to_string(seq_num) + " " +
                std::to_string(active_proposal_number) + " " +
                std::to_string(static_cast<unsigned int>(type)) + " " +
                proposal_str,
            dest_id, dest_addr, dest_port, false);
    }

    static ProposalMessage fromMessage(const Message &msg)
    {
        std::istringstream iss(msg.get_message());
        uint8_t sender_id = msg.get_sender_id();
        uint32_t seq_num, active_proposal_number;
        int type_int;

        iss >> seq_num >> active_proposal_number >> type_int;
        if (type_int < 0 || type_int > 2)
        {
            throw std::runtime_error("Invalid ProposalType value in message");
        }
        ProposalType type = static_cast<ProposalType>(type_int);

        std::unordered_set<int32_t> proposal;
        int32_t element;
        while (iss >> element)
        {
            proposal.insert(element);
        }

        return ProposalMessage(sender_id, seq_num, active_proposal_number, proposal, type);
    }

    static std::string decodeProposal(const std::unordered_set<int32_t> &proposal)
    {
        std::string decodedProposal;
        size_t i = 0;
        for (auto element : proposal)
        {
            decodedProposal += std::to_string(static_cast<unsigned int>(element));
            if (i < proposal.size() - 1)
            {
                decodedProposal += " ";
            }
            i++;
        }

        return decodedProposal;
    }
};