#pragma once

#include <vector>
#include "message.hpp"

class MessageBatch
{
private:
    std::vector<Message> messages;
    in_addr_t dest_addr;
    unsigned short dest_port;

public:
    MessageBatch(in_addr_t dest_addr, unsigned short dest_port) : dest_addr(dest_addr), dest_port(dest_port) {}
    MessageBatch(const std::vector<Message> &msgs, in_addr_t dest_addr, unsigned short dest_port) : messages(msgs), dest_addr(dest_addr), dest_port(dest_port) {}

    void add_message(const Message &msg)
    {
        messages.push_back(msg);
    }

    const std::vector<Message> &get_messages() const
    {
        return messages;
    }

    in_addr_t get_dest_addr() const
    {
        return dest_addr;
    }

    unsigned short get_dest_port() const
    {
        return dest_port;
    }

    static char *serialize(const MessageBatch &batch, size_t &buffer_size)
    {
        std::vector<char> buffer;

        size_t num_messages = batch.messages.size();
        buffer.insert(buffer.end(), reinterpret_cast<const char *>(&num_messages), reinterpret_cast<const char *>(&num_messages) + sizeof(num_messages));

        for (const auto &msg : batch.messages)
        {
            size_t msg_size;
            char *msg_buffer = Message::serialize(msg, msg_size);

            buffer.insert(buffer.end(), reinterpret_cast<const char *>(&msg_size), reinterpret_cast<const char *>(&msg_size) + sizeof(msg_size));

            buffer.insert(buffer.end(), msg_buffer, msg_buffer + msg_size);

            delete[] msg_buffer;
        }

        buffer_size = buffer.size();
        char *final_buffer = new char[buffer_size];
        std::memcpy(final_buffer, buffer.data(), buffer_size);

        return final_buffer;
    }

    static MessageBatch deserialize(const char *buffer, size_t received_size, in_addr_t dest_addr, unsigned short dest_port)
    {
        size_t offset = 0;

        size_t num_messages;
        std::memcpy(&num_messages, buffer + offset, sizeof(num_messages));
        offset += sizeof(num_messages);

        std::vector<Message> messages;
        messages.reserve(num_messages);

        for (size_t i = 0; i < num_messages; ++i)
        {
            size_t msg_size;
            std::memcpy(&msg_size, buffer + offset, sizeof(size_t));
            offset += sizeof(size_t);

            if (offset + msg_size > received_size)
            {
                throw std::runtime_error("Deserialization error: received buffer size is smaller than expected message size.");
            }

            Message msg = Message::deserialize(buffer + offset, msg_size);
            offset += msg_size;

            messages.push_back(msg);
        }

        return MessageBatch(messages, dest_addr, dest_port);
    }
};
