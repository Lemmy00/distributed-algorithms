#pragma once

#include <vector>
#include <utility>
#include <string>
#include <atomic>
#include <cassert>

class Message
{
private:
    uint64_t message_key;
    const bool is_ack;

    const uint8_t sender_id;
    const uint32_t seq_num;
    const uint32_t active_proposal_number;
    const uint8_t type;
    std::string message;

    const uint8_t dest_id;

    static std::atomic<uint64_t> msg_counter;

public:
    Message(const uint8_t sender_id, const uint32_t seq_num, const uint32_t active_proposal_number, const uint8_t type, const std::string &message, const uint8_t dest_id, const bool is_ack) : is_ack(is_ack), sender_id(sender_id), seq_num(seq_num), active_proposal_number(active_proposal_number), type(type), message(message), dest_id(dest_id)
    {
        message_key = msg_counter.fetch_add(1);
    }

    Message(const uint64_t message_key, const uint8_t sender_id, const uint8_t dest_id, const bool is_ack) : message_key(message_key), is_ack(is_ack), sender_id(sender_id), seq_num(0), active_proposal_number(0), type(0), dest_id(dest_id) {}
    Message(const uint64_t message_key, const uint8_t sender_id, const uint32_t seq_num, const uint32_t active_proposal_number, const uint8_t type, const std::string &message, const uint8_t dest_id, const bool is_ack) : message_key(message_key), is_ack(is_ack), sender_id(sender_id), seq_num(seq_num), active_proposal_number(active_proposal_number), type(type), message(message), dest_id(dest_id) {}

    ~Message() = default;

    std::pair<uint8_t, uint64_t> get_sender_message_key() const { return std::make_pair(sender_id, message_key); }
    std::pair<uint8_t, uint64_t> get_dest_message_key() const { return std::make_pair(dest_id, message_key); }
    uint64_t get_message_key() const { return message_key; }
    bool get_is_ack() const { return is_ack; }
    uint8_t get_sender_id() const { return sender_id; }
    uint32_t get_seq_num() const { return seq_num; }
    uint32_t get_active_proposal_number() const { return active_proposal_number; }
    uint8_t get_type() const { return type; }
    uint8_t get_dest_id() const { return dest_id; }
    const std::string &get_message() const { return message; }

    static char *serialize(const Message &msg, size_t &buffer_size)
    {
        std::vector<char> buffer;
        buffer.reserve(sizeof(msg.message_key) + sizeof(msg.is_ack) +
                       sizeof(msg.sender_id) + sizeof(msg.seq_num) +
                       sizeof(msg.active_proposal_number) + sizeof(msg.type) +
                       sizeof(msg.dest_id) + sizeof(size_t) + msg.message.size());

        buffer.insert(buffer.end(),
                      reinterpret_cast<const char *>(&msg.message_key),
                      reinterpret_cast<const char *>(&msg.message_key) + sizeof(msg.message_key));

        buffer.insert(buffer.end(),
                      reinterpret_cast<const char *>(&msg.is_ack),
                      reinterpret_cast<const char *>(&msg.is_ack) + sizeof(msg.is_ack));

        buffer.insert(buffer.end(),
                      reinterpret_cast<const char *>(&msg.sender_id),
                      reinterpret_cast<const char *>(&msg.sender_id) + sizeof(msg.sender_id));

        buffer.insert(buffer.end(),
                      reinterpret_cast<const char *>(&msg.seq_num),
                      reinterpret_cast<const char *>(&msg.seq_num) + sizeof(msg.seq_num));

        buffer.insert(buffer.end(),
                      reinterpret_cast<const char *>(&msg.active_proposal_number),
                      reinterpret_cast<const char *>(&msg.active_proposal_number) + sizeof(msg.active_proposal_number));

        buffer.insert(buffer.end(),
                      reinterpret_cast<const char *>(&msg.type),
                      reinterpret_cast<const char *>(&msg.type) + sizeof(msg.type));

        buffer.insert(buffer.end(),
                      reinterpret_cast<const char *>(&msg.dest_id),
                      reinterpret_cast<const char *>(&msg.dest_id) + sizeof(msg.dest_id));

        buffer.insert(buffer.end(), msg.message.begin(), msg.message.end());

        buffer_size = buffer.size();
        char *final_buffer = new char[buffer_size];
        std::memcpy(final_buffer, buffer.data(), buffer_size);

        return final_buffer;
    }

    static Message deserialize(const char *buffer, size_t received_size, uint8_t expected_dest_id)
    {
        size_t offset = 0;

        uint64_t message_key;
        std::memcpy(&message_key, buffer + offset, sizeof(message_key));
        offset += sizeof(message_key);

        bool is_ack;
        std::memcpy(&is_ack, buffer + offset, sizeof(is_ack));
        offset += sizeof(is_ack);

        uint8_t sender_id;
        std::memcpy(&sender_id, buffer + offset, sizeof(sender_id));
        offset += sizeof(sender_id);

        uint32_t seq_num;
        std::memcpy(&seq_num, buffer + offset, sizeof(seq_num));
        offset += sizeof(seq_num);

        uint32_t active_proposal_number;
        std::memcpy(&active_proposal_number, buffer + offset, sizeof(active_proposal_number));
        offset += sizeof(active_proposal_number);

        uint8_t type;
        std::memcpy(&type, buffer + offset, sizeof(type));
        offset += sizeof(type);

        uint8_t dest_id;
        std::memcpy(&dest_id, buffer + offset, sizeof(dest_id));
        offset += sizeof(dest_id);
        assert(dest_id == expected_dest_id && "Destination ID does not match the expected value");

        std::string msg(buffer + offset, buffer + received_size);
        return Message(message_key, sender_id, seq_num, active_proposal_number, type, msg, dest_id, is_ack);
    }
};

std::atomic<uint64_t> Message::msg_counter{0};