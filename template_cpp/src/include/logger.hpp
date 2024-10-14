#pragma once

#include <fstream>
#include <iostream>
#include <sstream>
#include <mutex>
#include <condition_variable>
#include <queue>
#include <thread>
#include <atomic>
#include <chrono>

class Logger
{
private:
    std::ofstream outputFile;
    std::queue<std::string> logBuffer;
    std::mutex bufferMutex;
    std::condition_variable bufferCV;
    std::atomic<bool> stopLogging{false};
    std::thread loggingThread;
    const size_t maxBufferSize;

public:
    Logger(const std::string &outputPath, size_t bufferSize = 1000000)
        : maxBufferSize(bufferSize)
    {
        outputFile.open(outputPath, std::ios::out | std::ios::app);
        if (!outputFile.is_open())
        {
            throw std::runtime_error("Failed to open log file");
        }
        loggingThread = std::thread(&Logger::loggingWorker, this);
    }

    ~Logger()
    {
        stop();
    }

    void log(const std::string &entry)
    {
        {
            std::lock_guard<std::mutex> lock(bufferMutex);
            logBuffer.push(entry);
        }
        bufferCV.notify_one();
    }

    void stop()
    {
        stopLogging = true;
        bufferCV.notify_one();
        if (loggingThread.joinable())
        {
            loggingThread.join();
        }
        flush();

        if (outputFile.is_open())
        {
            outputFile.close();
        }
    }

private:
    void loggingWorker()
    {
        while (!stopLogging)
        {
            std::unique_lock<std::mutex> lock(bufferMutex);
            bufferCV.wait_for(lock, std::chrono::seconds(1), [this]()
                              { return !logBuffer.empty() || stopLogging; });

            if (!logBuffer.empty())
            {
                flush();
            }
        }
    }

    void flush()
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
        else
        {
            std::cerr << "Log file is not open." << std::endl;
        }
    }
};
