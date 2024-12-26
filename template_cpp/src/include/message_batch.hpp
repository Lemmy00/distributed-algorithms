#pragma once

#include <vector>
#include <utility>
#include <string>

#include "message.hpp"

class MessageBatch
{
private:
    std::pair<uint8_t, uint32_t> batch_key;
    std::vector<Message> messages;
    in_addr_t dest_addr;
    unsigned short dest_port;

public:
    MessageBatch(in_addr_t dest_addr, unsigned short dest_port) : batch_key(std::make_pair(0, 0)), dest_addr(dest_addr), dest_port(dest_port) {}
    MessageBatch(const std::pair<uint8_t, uint32_t> &batch_key, in_addr_t dest_addr, unsigned short dest_port) : batch_key(batch_key), dest_addr(dest_addr), dest_port(dest_port) {}
    MessageBatch(const std::pair<uint8_t, uint32_t> &batch_key, const std::vector<Message> &msgs, in_addr_t dest_addr, unsigned short dest_port) : batch_key(batch_key), messages(msgs), dest_addr(dest_addr), dest_port(dest_port) {}
    MessageBatch(const std::pair<uint8_t, uint32_t> &batch_key, uint8_t sender_id, const std::vector<std::string> &msgs, in_addr_t dest_addr, unsigned short dest_port) : batch_key(batch_key), dest_addr(dest_addr), dest_port(dest_port)
    {
        for (const auto &msg : msgs)
        {
            messages.push_back(Message(sender_id, msg));
        }
    }

    uint8_t get_sender_id() const
    {
        return batch_key.first;
    }

    uint32_t get_seq_num() const
    {
        return batch_key.second;
    }

    const std::pair<uint8_t, uint32_t> &get_batch_key() const
    {
        return batch_key;
    }

    void add_message(const Message &msg)
    {
        messages.push_back(msg);
    }

    const std::vector<Message> &get_messages() const
    {
        return messages;
    }

    const std::vector<std::string> get_messages_str() const
    {
        std::vector<std::string> msgs;
        for (const auto &msg : messages)
        {
            msgs.push_back(msg.get_msg());
        }

        return msgs;
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

        buffer.insert(buffer.end(), reinterpret_cast<const char *>(&batch.batch_key.first), reinterpret_cast<const char *>(&batch.batch_key.first) + sizeof(batch.batch_key.first));
        buffer.insert(buffer.end(), reinterpret_cast<const char *>(&batch.batch_key.second), reinterpret_cast<const char *>(&batch.batch_key.second) + sizeof(batch.batch_key.second));

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

        std::pair<uint8_t, uint32_t> batch_key;
        std::memcpy(&batch_key.first, buffer + offset, sizeof(batch_key.first));
        offset += sizeof(batch_key.first);

        std::memcpy(&batch_key.second, buffer + offset, sizeof(batch_key.second));
        offset += sizeof(batch_key.second);

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

        return MessageBatch(batch_key, messages, dest_addr, dest_port);
    }
};
