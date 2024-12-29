#pragma once

#include <unordered_map>
#include <vector>
#include <optional>
#include <memory>
#include <cstring>
#include <cstdint>
#include <cassert>

#include "message.hpp"

class MessageBatch
{
private:
    size_t maxBatchSize;
    std::unordered_map<uint8_t, std::vector<Message>> batches;
    std::mutex mutex;

public:
    explicit MessageBatch(size_t num_processes, size_t batch_size = 8)
        : maxBatchSize(batch_size)
    {
        for (uint8_t i = 1; i <= num_processes; i++)
        {
            batches[i] = {};
        }
    }

    std::optional<std::unique_ptr<char[]>> serialize(const Message &msg, size_t &buffer_size)
    {
        std::lock_guard<std::mutex> lock(mutex);
        auto &vec = batches[msg.get_dest_id()];
        vec.push_back(msg);

        if (vec.size() < maxBatchSize)
        {
            return std::nullopt;
        }

        uint32_t count = static_cast<uint32_t>(vec.size());
        std::vector<std::unique_ptr<char[]>> messageBuffers(count);
        std::vector<size_t> messageSizes(count);

        size_t totalBytes = sizeof(count);

        for (uint32_t i = 0; i < count; i++)
        {
            size_t msgSize = 0;
            messageBuffers[i].reset(Message::serialize(vec[i], msgSize));
            messageSizes[i] = msgSize;
            totalBytes += sizeof(uint64_t);
            totalBytes += msgSize;
        }

        buffer_size = totalBytes;
        std::unique_ptr<char[]> finalBuffer(new char[buffer_size]);

        size_t offset = 0;
        std::memcpy(finalBuffer.get() + offset, &count, sizeof(count));
        offset += sizeof(count);

        for (uint32_t i = 0; i < count; i++)
        {
            uint64_t msgSize64 = static_cast<uint64_t>(messageSizes[i]);
            std::memcpy(finalBuffer.get() + offset, &msgSize64, sizeof(msgSize64));
            offset += sizeof(msgSize64);
            std::memcpy(finalBuffer.get() + offset, messageBuffers[i].get(), messageSizes[i]);
            offset += messageSizes[i];
        }

        vec.clear();

        return std::make_optional(std::move(finalBuffer));
    }

    static std::vector<Message> deserialize(const char *buffer, size_t buffer_size, uint8_t expected_dest_id)
    {
        std::vector<Message> result;
        size_t offset = 0;

        if (buffer_size < sizeof(uint32_t))
        {
            return result;
        }

        uint32_t count = 0;
        std::memcpy(&count, buffer + offset, sizeof(count));
        offset += sizeof(count);

        result.reserve(count);
        for (uint32_t i = 0; i < count; i++)
        {
            if (offset + sizeof(uint64_t) > buffer_size)
            {
                break;
            }

            uint64_t msgSize = 0;
            std::memcpy(&msgSize, buffer + offset, sizeof(msgSize));
            offset += sizeof(msgSize);

            if (offset + msgSize > buffer_size)
            {
                break;
            }

            Message m = Message::deserialize(buffer + offset, static_cast<size_t>(msgSize), expected_dest_id);
            offset += msgSize;

            result.push_back(std::move(m));
        }

        return result;
    }
};
