#include <chrono>
#include <iostream>
#include <thread>
#include <unordered_map>
#include <vector>

#include "parser.hpp"
#include "hello.h"
#include "perfect_links.hpp"
#include "message.hpp"
#include "message_batch.hpp"
#include "config.hpp"
#include "logger.hpp"
#include <signal.h>

std::shared_ptr<Logger> logger;
std::unique_ptr<PerfectLinks> perfectLinks;

std::vector<std::thread> receiverThreads;
std::thread receiverThread;
std::thread senderThread;

static void stop(int)
{
  // reset signal handlers to default
  signal(SIGTERM, SIG_DFL);
  signal(SIGINT, SIG_DFL);

  // immediately stop network packet processing
  std::cout << "Immediately stopping network packet processing.\n";

  if (perfectLinks)
  {
    perfectLinks->stop();
  }

  // write/flush output file if necessary
  std::cout << "Writing output.\n";
  if (logger)
  {
    logger->close();
  }

  // exit directly from signal handler
  exit(0);
}

int main(int argc, char **argv)
{
  signal(SIGTERM, stop);
  signal(SIGINT, stop);

  // `true` means that a config file is required.
  // Call with `false` if no config file is necessary.
  bool requireConfig = true;

  Parser parser(argc, argv);
  parser.parse();

  std::cout << std::endl;

  std::cout << "My PID: " << getpid() << "\n";
  std::cout << "From a new terminal type `kill -SIGINT " << getpid() << "` or `kill -SIGTERM "
            << getpid() << "` to stop processing packets\n\n";

  std::cout << "My ID: " << parser.id() << "\n\n";

  std::cout << "List of resolved hosts is:\n";
  std::cout << "==========================\n";
  auto hosts = parser.hosts();
  std::unordered_map<unsigned long, Parser::Host> hostMap;
  for (auto &host : hosts)
  {
    std::cout << host.id << "\n";
    std::cout << "Human-readable IP: " << host.ipReadable() << "\n";
    std::cout << "Machine-readable IP: " << host.ip << "\n";
    std::cout << "Human-readbale Port: " << host.portReadable() << "\n";
    std::cout << "Machine-readbale Port: " << host.port << "\n";
    std::cout << "\n";

    hostMap[host.id] = host;
  }
  std::cout << "\n";

  std::cout << "Path to output:\n";
  std::cout << "===============\n";
  std::cout << parser.outputPath() << "\n\n";

  std::cout << "Path to config:\n";
  std::cout << "===============\n";
  std::cout << parser.configPath() << "\n\n";

  parser.configPath();
  Config config(parser.configPath());
  std::cout << "Number of messages: " << config.get_num_msgs() << "\n";
  std::cout << "Receiver index: " << config.get_receiver_index() << "\n\n";

  std::cout << "Doing some initialization...\n\n";

  std::cout << "Broadcasting and delivering messages...\n\n";

  Parser::Host currentHost = hostMap[parser.id()];

  if (parser.id() == config.get_receiver_index())
  {
    logger = std::make_shared<Logger>(parser.outputPath(), 5000000);
  }
  else
  {
    logger = std::make_shared<Logger>(parser.outputPath(), 16000);
  }

  perfectLinks = std::make_unique<PerfectLinks>(currentHost.ip, currentHost.port, logger, 55000);

  if (parser.id() == config.get_receiver_index())
  {
    receiverThreads = perfectLinks->startReceivers(7);
    for (size_t i = 0; i < 7; i++)
    {
      receiverThreads[i].detach();
    }
  }
  else
  {
    receiverThread = perfectLinks->startReceiver();
    senderThread = perfectLinks->startSender();
    receiverThread.detach();
    senderThread.detach();

    for (unsigned long i = 1; i <= config.get_num_msgs(); i += 8)
    {
      MessageBatch batch(hostMap[config.get_receiver_index()].ip, hostMap[config.get_receiver_index()].port);
      for (unsigned long j = i; j < i + 8; j++)
      {
        if (j > config.get_num_msgs())
        {
          break;
        }

        std::string message = std::to_string(j);
        Message msg(parser.id(), message);
        batch.add_message(msg);
      }

      perfectLinks->send(batch);
    }
  }

  std::cout << "Waiting for messages...\n";

  // After a process finishes broadcasting,
  // it waits forever for the delivery of messages.
  while (true)
  {
    std::this_thread::sleep_for(std::chrono::hours(1));
  }

  return 0;
}
