#pragma once

#include <fstream>
#include <iostream>
#include <sstream>
#include <mutex>
#include <queue>
#include <atomic>
#include <thread>
#include <chrono>
#include <future>

class Logger
{
private:
    std::ofstream outputFile;

    std::queue<std::string> logBufferA;
    std::queue<std::string> logBufferB;
    std::mutex bufferMutexA;
    std::mutex bufferMutexB;
    std::mutex flushMutex;

    const size_t maxBufferSize;
    std::atomic<bool> useA{true};
    std::atomic<bool> flushStop{false};

public:
    Logger(const std::string &outputPath, size_t bufferSize = 5000000)
        : maxBufferSize(bufferSize)
    {
        outputFile.open(outputPath, std::ios::out | std::ios::app);
        if (!outputFile.is_open())
        {
            throw std::runtime_error("Failed to open log file");
        }
    }

    ~Logger()
    {
        close();
    }

    void log(const std::string &entry)
    {
        if (useA.load())
        {
            std::lock_guard<std::mutex> lock(bufferMutexA);
            logBufferA.push(entry);

            if (logBufferA.size() >= maxBufferSize)
            {
                useA.store(false);
                flushBuffer(logBufferA);
            }
        }
        else
        {
            std::lock_guard<std::mutex> lock(bufferMutexB);
            logBufferB.push(entry);

            if (logBufferB.size() >= maxBufferSize)
            {
                useA.store(true);
                flushBuffer(logBufferB);
            }
        }
    }

    void close()
    {
        flushStop.store(true);
        flush();
        if (outputFile.is_open())
        {
            outputFile.close();
        }
    }

    void flush()
    {
        flushA();
        flushB();
    }

    void flushA()
    {
        flushStop.store(false);
        std::lock_guard<std::mutex> lock(bufferMutexA);
        flushBuffer(logBufferA);
    }

    void flushB()
    {
        flushStop.store(false);
        std::lock_guard<std::mutex> lock(bufferMutexB);
        flushBuffer(logBufferB);
    }

private:
    void flushBuffer(std::queue<std::string> &buffer)
    {
        std::lock_guard<std::mutex> flushLock(flushMutex);
        if (outputFile.is_open())
        {
            while (!buffer.empty())
            {
                if (flushStop.load())
                {
                    return;
                }
                outputFile << buffer.front() << "\n";
                buffer.pop();
            }
            outputFile.flush();
        }
    }
};
