#pragma once

#include <fstream>
#include <iostream>
#include <sstream>
#include <mutex>
#include <queue>
#include <atomic>
#include <chrono>

class Logger
{
private:
    std::ofstream outputFile;
    std::queue<std::string> logBuffer;
    std::mutex bufferMutex;
    const size_t maxBufferSize;

public:
    Logger(const std::string &outputPath, size_t bufferSize = 5000)
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
        close_logger();
    }

    void log(const std::string &entry)
    {
        std::lock_guard<std::mutex> lock(bufferMutex);
        logBuffer.push(entry);

        if (logBuffer.size() >= maxBufferSize)
        {
            flush_locked();
        }
    }

    void close_logger()
    {
        flush();
        if (outputFile.is_open())
        {
            outputFile.close();
        }
    }

    void flush()
    {
        std::lock_guard<std::mutex> lock(bufferMutex);
        flush_locked();
    }

private:
    void flush_locked()
    {
        if (outputFile.is_open())
        {
            while (!logBuffer.empty())
            {
                outputFile << logBuffer.front() << std::endl;
                logBuffer.pop();
            }
            outputFile.flush();
        }
    }
};