#pragma once

#include <vector>
#include <utility>
#include <string>

class Message
{
private:
    uint64_t message_key;
    bool is_ack;

    uint8_t sender_id;
    std::string message;

    uint8_t dest_id;
    in_addr_t dest_addr;
    unsigned short dest_port;

    static std::atomic<uint64_t> msg_counter;

public:
    Message(uint8_t sender_id, const std::string &message, uint8_t dest_id, in_addr_t dest_addr, unsigned short dest_port, bool is_ack) : is_ack(is_ack), sender_id(sender_id), message(message), dest_id(dest_id), dest_addr(dest_addr), dest_port(dest_port)
    {
        message_key = msg_counter.fetch_add(1);
    }

    Message(const uint64_t message_key, uint8_t sender_id, uint8_t dest_id, in_addr_t dest_addr, unsigned short dest_port, bool is_ack) : message_key(message_key), is_ack(is_ack), sender_id(sender_id), dest_id(dest_id), dest_addr(dest_addr), dest_port(dest_port) {}
    Message(const uint64_t message_key, uint8_t sender_id, const std::string &message, uint8_t dest_id, in_addr_t dest_addr, unsigned short dest_port, bool is_ack) : message_key(message_key), is_ack(is_ack), sender_id(sender_id), message(message), dest_id(dest_id), dest_addr(dest_addr), dest_port(dest_port) {}

    ~Message() = default;

    uint64_t get_message_key() const
    {
        return message_key;
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

    const std::string get_message() const
    {
        return message;
    }

    in_addr_t get_dest_addr() const
    {
        return dest_addr;
    }

    unsigned short get_dest_port() const
    {
        return dest_port;
    }

    static char *serialize(const Message &msg, size_t &buffer_size)
    {
        std::vector<char> buffer;

        buffer.insert(buffer.end(), reinterpret_cast<const char *>(&msg.message_key), reinterpret_cast<const char *>(&msg.message_key) + sizeof(msg.message_key));
        buffer.insert(buffer.end(), reinterpret_cast<const char *>(&msg.is_ack), reinterpret_cast<const char *>(&msg.is_ack) + sizeof(msg.is_ack));

        buffer.insert(buffer.end(), reinterpret_cast<const char *>(&msg.sender_id), reinterpret_cast<const char *>(&msg.sender_id) + sizeof(msg.sender_id));
        buffer.insert(buffer.end(), reinterpret_cast<const char *>(&msg.dest_id), reinterpret_cast<const char *>(&msg.dest_id) + sizeof(msg.dest_id));

        size_t msg_size = msg.message.size();
        buffer.insert(buffer.end(), reinterpret_cast<const char *>(&msg_size), reinterpret_cast<const char *>(&msg_size) + sizeof(msg_size));
        buffer.insert(buffer.end(), msg.message.begin(), msg.message.end());

        buffer_size = buffer.size();
        char *final_buffer = new char[buffer_size];
        std::memcpy(final_buffer, buffer.data(), buffer_size);

        return final_buffer;
    }

    static Message deserialize(const char *buffer, size_t received_size, in_addr_t dest_addr, unsigned short dest_port)
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

        uint8_t dest_id;
        std::memcpy(&dest_id, buffer + offset, sizeof(dest_id));
        offset += sizeof(dest_id);

        std::string message;
        size_t msg_size;
        std::memcpy(&msg_size, buffer + offset, sizeof(size_t));
        offset += sizeof(msg_size);

        std::string msg(buffer + offset, buffer + offset + msg_size);
        offset += msg_size;

        if (offset != received_size)
        {
            std::cerr << "Deserialization error: received buffer size is different than expected message size." << std::endl;
            std::cerr << "Expected size: " << received_size << ", Processed size: " << offset << std::endl;
            throw std::runtime_error("Deserialization error: received buffer size is different than expected message size.");
        }

        return Message(message_key, sender_id, message, dest_id, dest_addr, dest_port, is_ack);
    }
};

std::atomic<uint64_t> Message::msg_counter{0};