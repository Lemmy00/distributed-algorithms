#pragma once

#include <vector>
#include <utility>
#include <string>

class MessageBatch
{
private:
    std::pair<uint8_t, uint32_t> batch_key;
    bool is_ack;

    uint8_t sender_id;
    std::vector<std::string> messages;

    uint8_t dest_id;
    in_addr_t dest_addr;
    unsigned short dest_port;

public:
    MessageBatch(const std::pair<uint8_t, uint32_t> &batch_key, uint8_t sender_id, uint8_t dest_id, in_addr_t dest_addr, unsigned short dest_port, bool is_ack) : batch_key(batch_key), is_ack(is_ack), sender_id(sender_id), dest_id(dest_id), dest_addr(dest_addr), dest_port(dest_port) {}
    MessageBatch(const std::pair<uint8_t, uint32_t> &batch_key, uint8_t sender_id, const std::vector<std::string> &messages, uint8_t dest_id, in_addr_t dest_addr, unsigned short dest_port, bool is_ack) : batch_key(batch_key), is_ack(is_ack), sender_id(sender_id), messages(messages), dest_id(dest_id), dest_addr(dest_addr), dest_port(dest_port) {}

    const std::pair<uint8_t, uint32_t> &get_batch_key() const
    {
        return batch_key;
    }

    uint8_t get_init_sender_id() const
    {
        return batch_key.first;
    }

    uint32_t get_seq_num() const
    {
        return batch_key.second;
    }

    bool get_is_ack() const
    {
        return is_ack;
    }

    uint8_t get_sender_id() const
    {
        return sender_id;
    }

    uint8_t get_dest_id() const
    {
        return dest_id;
    }

    void add_message(const std::string &msg)
    {
        messages.push_back(msg);
    }

    const std::vector<std::string> &get_messages() const
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

        buffer.insert(buffer.end(), reinterpret_cast<const char *>(&batch.batch_key.first), reinterpret_cast<const char *>(&batch.batch_key.first) + sizeof(batch.batch_key.first));
        buffer.insert(buffer.end(), reinterpret_cast<const char *>(&batch.batch_key.second), reinterpret_cast<const char *>(&batch.batch_key.second) + sizeof(batch.batch_key.second));
        buffer.insert(buffer.end(), reinterpret_cast<const char *>(&batch.is_ack), reinterpret_cast<const char *>(&batch.is_ack) + sizeof(batch.is_ack));

        buffer.insert(buffer.end(), reinterpret_cast<const char *>(&batch.sender_id), reinterpret_cast<const char *>(&batch.sender_id) + sizeof(batch.sender_id));
        buffer.insert(buffer.end(), reinterpret_cast<const char *>(&batch.dest_id), reinterpret_cast<const char *>(&batch.dest_id) + sizeof(batch.dest_id));

        size_t num_messages = batch.messages.size();
        buffer.insert(buffer.end(), reinterpret_cast<const char *>(&num_messages), reinterpret_cast<const char *>(&num_messages) + sizeof(num_messages));

        for (const auto &msg : batch.messages)
        {
            size_t msg_size = msg.size();
            buffer.insert(buffer.end(), reinterpret_cast<const char *>(&msg_size), reinterpret_cast<const char *>(&msg_size) + sizeof(msg_size));
            buffer.insert(buffer.end(), msg.begin(), msg.end());
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

        bool is_ack;
        std::memcpy(&is_ack, buffer + offset, sizeof(is_ack));
        offset += sizeof(is_ack);

        uint8_t sender_id;
        std::memcpy(&sender_id, buffer + offset, sizeof(sender_id));
        offset += sizeof(sender_id);

        uint8_t dest_id;
        std::memcpy(&dest_id, buffer + offset, sizeof(dest_id));
        offset += sizeof(dest_id);

        size_t num_messages;
        std::memcpy(&num_messages, buffer + offset, sizeof(num_messages));
        offset += sizeof(num_messages);

        std::vector<std::string> messages;
        messages.reserve(num_messages);
        for (size_t i = 0; i < num_messages; ++i)
        {
            size_t msg_size;
            std::memcpy(&msg_size, buffer + offset, sizeof(size_t));
            offset += sizeof(msg_size);

            if (offset + msg_size > received_size)
            {
                throw std::runtime_error("Deserialization error: received buffer size is smaller than expected message size.");
            }

            std::string msg(buffer + offset, buffer + offset + msg_size);
            offset += msg_size;

            messages.push_back(msg);
        }

        return MessageBatch(batch_key, sender_id, messages, dest_id, dest_addr, dest_port, is_ack);
    }
};
