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
        logBuffer.push(entry);

        if (logBuffer.size() >= maxBufferSize)
        {
            flush();
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
        if (outputFile.is_open())
        {
            while (!logBuffer.empty())
            {
                outputFile << logBuffer.front() << "\n";
                logBuffer.pop();
            }
            outputFile.flush();
        }
    }
};