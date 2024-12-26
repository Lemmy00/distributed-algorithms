#pragma once

#include <cstring>
#include <cstdint>
#include <cstdlib>
#include <atomic>
#include <iostream>
#include <string>

class Message
{
private:
    uint64_t msg_id;
    uint8_t sender_id;
    std::string msg;
    size_t msg_size;
    bool is_ack;

    static std::atomic<uint64_t> msg_counter;

public:
    Message(uint8_t sender_id, const std::string &msg)
        : sender_id(sender_id), msg(msg), msg_size(msg.length()), is_ack(false)
    {
        msg_id = msg_counter++;
    }

    Message(uint64_t msg_id, uint8_t sender_id, const std::string &msg)
        : msg_id(msg_id), sender_id(sender_id), msg(msg), msg_size(msg.length()), is_ack(false)
    {
    }

    Message(uint64_t msg_id, uint8_t sender_id, const std::string &msg, bool is_ack)
        : msg_id(msg_id), sender_id(sender_id), msg(msg), msg_size(msg.length()), is_ack(is_ack)
    {
    }

    Message(uint64_t msg_id, uint8_t sender_id, bool is_ack)
        : msg_id(msg_id), sender_id(sender_id), msg(""), msg_size(0), is_ack(is_ack)
    {
    }

    ~Message() = default;

    uint64_t get_msg_id() const { return msg_id; }
    uint8_t get_sender_id() const { return sender_id; }
    const std::string &get_msg() const { return msg; }
    size_t get_msg_size() const { return msg_size; }
    bool get_is_ack() const { return is_ack; }

    static char *serialize(const Message &message, size_t &buffer_size)
    {
        buffer_size = sizeof(message.msg_id) + sizeof(message.sender_id) + sizeof(message.msg_size) + sizeof(message.is_ack) + message.msg_size;
        char *buffer = new char[buffer_size];

        size_t offset = 0;
        std::memcpy(buffer + offset, &message.msg_id, sizeof(message.msg_id));
        offset += sizeof(message.msg_id);

        std::memcpy(buffer + offset, &message.sender_id, sizeof(message.sender_id));
        offset += sizeof(message.sender_id);

        std::memcpy(buffer + offset, &message.msg_size, sizeof(message.msg_size));
        offset += sizeof(message.msg_size);

        std::memcpy(buffer + offset, &message.is_ack, sizeof(message.is_ack));
        offset += sizeof(message.is_ack);

        std::memcpy(buffer + offset, message.msg.c_str(), message.msg_size);

        return buffer;
    }

    static Message deserialize(const char *buffer, size_t received_size)
    {
        size_t offset = 0;

        uint64_t msg_id;
        std::memcpy(&msg_id, buffer + offset, sizeof(msg_id));
        offset += sizeof(msg_id);

        uint8_t sender_id;
        std::memcpy(&sender_id, buffer + offset, sizeof(sender_id));
        offset += sizeof(sender_id);

        size_t msg_size;
        std::memcpy(&msg_size, buffer + offset, sizeof(msg_size));
        offset += sizeof(msg_size);

        bool is_ack;
        std::memcpy(&is_ack, buffer + offset, sizeof(is_ack));
        offset += sizeof(is_ack);

        if (offset + msg_size > received_size)
        {
            throw std::runtime_error("Deserialization error: received buffer size is smaller than expected message size.");
        }

        return Message(msg_id, sender_id, std::string(buffer + offset, msg_size), is_ack);
    }
};

std::atomic<uint64_t> Message::msg_counter{0};
