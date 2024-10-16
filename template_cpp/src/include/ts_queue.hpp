// Adapted from: https://www.geeksforgeeks.org/implement-thread-safe-queue-in-c/

#pragma once

#include <condition_variable>
#include <iostream>
#include <mutex>
#include <queue>
#include <atomic>

template <typename T>
class TSQueue
{
private:
    std::queue<T> m_queue;
    std::atomic<size_t> m_size{0};
    std::mutex m_mutex;
    std::condition_variable m_cond;
    size_t m_capacity;

public:
    TSQueue(size_t capacity = 50000) : m_capacity(capacity) {}

    bool is_full() const
    {
        return m_size.load() >= m_capacity;
    }

    bool is_empty() const
    {
        return m_size.load() == 0;
    }

    void push(T item)
    {
        std::unique_lock<std::mutex> lock(m_mutex);

        m_queue.push(std::move(item));
        ++m_size;

        m_cond.notify_one();
    }

    T pop()
    {
        std::unique_lock<std::mutex> lock(m_mutex);
        m_cond.wait(lock,
                    [this]()
                    { return m_size.load() > 0; });

        T item = std::move(m_queue.front());
        m_queue.pop();
        --m_size;
        lock.unlock();

        m_cond.notify_one();

        return item;
    }
};
