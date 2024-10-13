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
    uint32_t msg_id;
    unsigned long sender_id;
    std::string msg;
    size_t msg_size;

    static std::atomic<uint32_t> msg_counter;

public:
    Message(unsigned long sender_id, const char *msg, size_t msg_size)
        : sender_id(sender_id), msg(msg, msg_size), msg_size(msg_size)
    {
        msg_id = msg_counter++;
    }

    Message(unsigned long sender_id, const std::string &msg)
        : sender_id(sender_id), msg(msg), msg_size(msg.length())
    {
        msg_id = msg_counter++;
    }

    Message(uint32_t msg_id, unsigned long sender_id, const char *msg, size_t msg_size)
        : msg_id(msg_id), sender_id(sender_id), msg(msg, msg_size), msg_size(msg_size)
    {
    }

    ~Message() = default;

    uint32_t get_msg_id() const { return msg_id; }
    unsigned long get_sender_id() const { return sender_id; }
    const char *get_msg() const { return msg.c_str(); }
    size_t get_msg_size() const { return msg_size; }

    static char *serialize(const Message &message, size_t &buffer_size)
    {
        buffer_size = sizeof(message.msg_id) + sizeof(message.sender_id) + sizeof(message.msg_size) + message.msg_size;
        char *buffer = new char[buffer_size];

        size_t offset = 0;
        std::memcpy(buffer + offset, &message.msg_id, sizeof(message.msg_id));
        offset += sizeof(message.msg_id);

        std::memcpy(buffer + offset, &message.sender_id, sizeof(message.sender_id));
        offset += sizeof(message.sender_id);

        std::memcpy(buffer + offset, &message.msg_size, sizeof(message.msg_size));
        offset += sizeof(message.msg_size);

        std::memcpy(buffer + offset, message.msg.c_str(), message.msg_size);

        return buffer;
    }

    static Message deserialize(const char *buffer, size_t received_size)
    {
        size_t offset = 0;

        uint32_t msg_id;
        std::memcpy(&msg_id, buffer + offset, sizeof(msg_id));
        offset += sizeof(msg_id);

        unsigned long sender_id;
        std::memcpy(&sender_id, buffer + offset, sizeof(sender_id));
        offset += sizeof(sender_id);

        size_t msg_size;
        std::memcpy(&msg_size, buffer + offset, sizeof(msg_size));
        offset += sizeof(msg_size);

        if (offset + msg_size > received_size)
        {
            throw std::runtime_error("Deserialization error: received buffer size is smaller than expected message size.");
        }

        return Message(msg_id, sender_id, buffer + offset, msg_size);
    }
};

std::atomic<uint32_t> Message::msg_counter{0};
