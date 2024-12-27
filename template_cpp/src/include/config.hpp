#include <fstream>
#include <iostream>
#include <string>
#include <sstream>
#include <unordered_set>

class Config
{
private:
    std::ifstream infile;

    uint32_t num_proposals;
    uint32_t max_proposal_size;
    uint32_t num_elements;

public:
    Config(const char *path) : infile(path)
    {
        if (!infile.is_open())
        {
            throw std::runtime_error("Unable to open config file.");
        }

        infile >> num_proposals;
        if (infile.fail())
        {
            throw std::runtime_error("Error reading number of proposals from config file.");
        }

        infile >> max_proposal_size;
        if (infile.fail())
        {
            throw std::runtime_error("Error reading max proposal size from config file.");
        }

        infile >> num_elements;
        if (infile.fail())
        {
            throw std::runtime_error("Error reading number of elements from config file.");
        }

        // Skip the newline character
        infile.ignore();
    }

    std::unordered_set<uint16_t> read_next_proposal()
    {
        std::unordered_set<uint16_t> proposal;

        std::string line;
        if (!getline(infile, line))
        {
            throw std::runtime_error("Error reading proposal from config file.");
        }

        std::istringstream iss(line);
        uint16_t element;
        while (iss >> element)
        {
            proposal.insert(element);
        }

        return proposal;
    }

    uint32_t get_num_proposals() const
    {
        return num_proposals;
    }

    uint32_t get_max_proposal_size() const
    {
        return max_proposal_size;
    }

    uint32_t get_num_elements() const
    {
        return num_elements;
    }
};