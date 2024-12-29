#pragma once

#include <unordered_map>
#include <vector>
#include <optional>
#include <memory>
#include <cstring>
#include <cstdint>
#include <mutex>
#include "message.hpp"

class MessageBatch
{
private:
    const size_t max_batch_size;
    std::unordered_map<uint8_t, std::vector<Message>> batches;
    std::mutex msg_mutex;

public:
    MessageBatch(size_t num_processes, size_t batch_size = 8)
        : max_batch_size(batch_size)
    {
        for (uint8_t i = 1; i <= num_processes; ++i)
        {
            batches.emplace(i, std::vector<Message>{});
        }
    }

    std::optional<std::unique_ptr<char[]>> serialize(const Message &msg, size_t &buffer_size)
    {
        std::lock_guard<std::mutex> lock(msg_mutex);
        auto &vec = batches[msg.get_dest_id()];
        vec.push_back(msg);

        if (vec.size() < max_batch_size)
        {
            return std::nullopt;
        }

        uint32_t message_cnt = static_cast<uint32_t>(vec.size());
        std::vector<std::unique_ptr<char[]>> message_buffers(message_cnt);
        std::vector<size_t> message_sizes(message_cnt);
        size_t total_size = sizeof(message_cnt);

        for (uint32_t i = 0; i < message_cnt; ++i)
        {
            size_t msg_size = 0;
            message_buffers[i].reset(Message::serialize(vec[i], msg_size));
            message_sizes[i] = msg_size;
            total_size += sizeof(uint32_t) + msg_size;
        }

        buffer_size = total_size;
        auto final_buffer = std::make_unique<char[]>(buffer_size);
        size_t offset = 0;

        std::memcpy(final_buffer.get(), &message_cnt, sizeof(message_cnt));
        offset += sizeof(message_cnt);

        for (uint32_t i = 0; i < message_cnt; ++i)
        {
            uint32_t msg_size_32 = static_cast<uint32_t>(message_sizes[i]);
            std::memcpy(final_buffer.get() + offset, &msg_size_32, sizeof(msg_size_32));
            offset += sizeof(msg_size_32);

            std::memcpy(final_buffer.get() + offset, message_buffers[i].get(), message_sizes[i]);
            offset += message_sizes[i];
        }

        vec.clear();
        return std::make_optional(std::move(final_buffer));
    }

    static std::vector<Message> deserialize(const char *buffer, size_t buffer_size, uint8_t expected_dest_id)
    {
        std::vector<Message> messages;
        size_t offset = 0;

        if (buffer_size < sizeof(uint32_t))
        {
            return messages;
        }

        uint32_t message_cnt = 0;
        std::memcpy(&message_cnt, buffer, sizeof(message_cnt));
        offset += sizeof(message_cnt);

        messages.reserve(message_cnt);
        for (uint32_t i = 0; i < message_cnt; ++i)
        {
            if (offset + sizeof(uint32_t) > buffer_size)
            {
                break;
            }

            uint32_t msg_size = 0;
            std::memcpy(&msg_size, buffer + offset, sizeof(msg_size));
            offset += sizeof(msg_size);

            if (offset + msg_size > buffer_size)
            {
                break;
            }

            messages.emplace_back(Message::deserialize(buffer + offset, msg_size, expected_dest_id));
            offset += msg_size;
        }

        return messages;
    }
};
