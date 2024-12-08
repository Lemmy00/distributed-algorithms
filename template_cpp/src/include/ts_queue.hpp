// Adapted from: https://www.geeksforgeeks.org/implement-thread-safe-queue-in-c/

#pragma once

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <queue>
#include <atomic>
#include <optional>
#include <csignal>

template <typename T>
class TSQueue
{
private:
    std::queue<T> m_queue;
    std::atomic<size_t> m_size{0};
    std::mutex m_mutex;
    std::condition_variable m_cond;
    std::atomic<bool> m_shutdown{false};

public:
    TSQueue() {}

    void shutdown()
    {
        m_shutdown.store(true);
        m_cond.notify_all();
    }

    void push(T item)
    {
        if (m_shutdown.load())
        {
            return;
        }

        std::unique_lock<std::mutex> lock(m_mutex);

        m_queue.push(std::move(item));
        ++m_size;

        m_cond.notify_one();
    }

    std::optional<T> pop()
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_cond.wait(lock, [this]()
                    { return m_shutdown.load() || m_size.load() > 0; });

        if (m_shutdown.load() && m_size.load() == 0)
        {
            return std::nullopt;
        }

        T item = std::move(m_queue.front());
        m_queue.pop();
        --m_size;

        m_cond.notify_one();

        return item;
    }
};
