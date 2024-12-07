#include <fstream>
#include <iostream>
#include <string>

class Config
{
private:
    unsigned long num_msgs;

public:
    Config(const char *path)
    {
        std::ifstream infile(path);
        if (!infile.is_open())
        {
            throw std::runtime_error("Unable to open config file.");
        }

        infile >> num_msgs;
        if (infile.fail())
        {
            throw std::runtime_error("Error reading config values.");
        }
    }

    unsigned long get_num_msgs() const { return num_msgs; }
};