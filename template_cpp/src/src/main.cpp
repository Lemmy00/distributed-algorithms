#include <chrono>
#include <iostream>
#include <thread>
#include <unordered_map>

#include "parser.hpp"
#include "hello.h"
#include "perfect_links.hpp"
#include "message.hpp"
#include "config.hpp"
#include <signal.h>

static void stop(int)
{
  // reset signal handlers to default
  signal(SIGTERM, SIG_DFL);
  signal(SIGINT, SIG_DFL);

  // immediately stop network packet processing
  std::cout << "Immediately stopping network packet processing.\n";

  // write/flush output file if necessary
  std::cout << "Writing output.\n";

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
  size_t numThreads = 4;
  auto perfectLinks = new PerfectLinks(currentHost.ip, currentHost.port, numThreads);
  perfectLinks->start();
  if (config.get_receiver_index() != parser.id())
  {
    std::cout << "I am a sender.\n";
    for (unsigned long i = 0; i < config.get_num_msgs(); i++)
    {
      std::string message = "test" + std::to_string(i);
      Message msg(parser.id(), message.c_str(), message.length());
      std::cout << "Sending message with ID: " << msg.get_msg_id() << " with content: " << msg.get_msg() << "\n";
      perfectLinks->send(hostMap[config.get_receiver_index()].ip, hostMap[config.get_receiver_index()].port, msg);
    }
  }

  // After a process finishes broadcasting,
  // it waits forever for the delivery of messages.
  while (true)
  {
    std::this_thread::sleep_for(std::chrono::hours(1));
  }

  return 0;
}
