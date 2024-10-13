#include <fstream>
#include <iostream>
#include <string>

class Config
{
private:
    unsigned long num_msgs;
    unsigned long receiver_index;

public:
    Config(const char *path)
    {
        std::ifstream infile(path);
        if (!infile.is_open())
        {
            throw std::runtime_error("Unable to open config file.");
        }

        infile >> num_msgs >> receiver_index;
        if (infile.fail())
        {
            throw std::runtime_error("Error reading config values.");
        }
    }

    unsigned long get_num_msgs() const { return num_msgs; }
    unsigned long get_receiver_index() const { return receiver_index; }
};