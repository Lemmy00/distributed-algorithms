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

    std::atomic<bool> useA{true};
    std::queue<std::string> logBufferA;
    std::queue<std::string> logBufferB;
    std::mutex bufferMutexA;
    std::mutex bufferMutexB;
    std::mutex flushMutex;

    const size_t maxBufferSize;

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
                // useA.store(false);
                flush_lockedA();
            }
        }
        else
        {
            std::lock_guard<std::mutex> lock(bufferMutexB);
            logBufferB.push(entry);

            if (logBufferB.size() >= maxBufferSize)
            {
                useA.store(true);
                flush_lockedB();
            }
        }
    }

    void close()
    {
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
        std::lock_guard<std::mutex> lock(bufferMutexA);
        flush_lockedA();
    }

    void flushB()
    {
        std::lock_guard<std::mutex> lock(bufferMutexB);
        flush_lockedB();
    }

private:
    void flush_lockedA()
    {
        std::lock_guard<std::mutex> flushLock(flushMutex);
        if (outputFile.is_open())
        {
            while (!logBufferA.empty())
            {
                outputFile << logBufferA.front() << "\n";
                logBufferA.pop();
            }
            outputFile.flush();
        }
    }

    void flush_lockedB()
    {
        std::lock_guard<std::mutex> flushLock(flushMutex);
        if (outputFile.is_open())
        {
            while (!logBufferB.empty())
            {
                outputFile << logBufferB.front() << "\n";
                logBufferB.pop();
            }
            outputFile.flush();
        }
    }
};