import os
import sys
import re

if __name__ == "__main__":
    if len(sys.argv) < 3:
        print("Usage: python lattice_verify_time.py <input_files_dir> <output_files_dir> [optional: <term_process>, <term_process> ...]")
        sys.exit(1)
        
    input_files_dir = sys.argv[1]
    output_files_dir = sys.argv[2]
    term_processes = set(int(x) - 1 for x in sys.argv[3:])
    
    input_files = [file for file in os.listdir(input_files_dir) if file.endswith('.config')]
    output_files = [file for file in os.listdir(output_files_dir) if file.endswith('.output')]

    input_files.sort()
    output_files.sort()
    
    assert len(input_files) == len(output_files), "Number of input files and output files do not match"
    
    num_processes = len(input_files)
    proposals_process = [[] for _ in range(num_processes)]
    decisions_process = [[] for _ in range(num_processes)]
    
    n_lines = []
    for i in range(num_processes):
        input_file = input_files[i]
        output_file = output_files[i]
        
        input_filepath = os.path.join(input_files_dir, input_file)
        output_filepath = os.path.join(output_files_dir, output_file)
        
        with open(input_filepath, 'r') as file:
            lines = file.readlines()
            for line in lines[1:]:
                line = line.strip()
                if line:
                    proposals = line.split()
                    proposals_process[i].append(set(int(x) for x in proposals))
                    
        if i in term_processes:
            decisions_process[i] = [set() for _ in range(len(proposals_process[i]))]
            continue
        
        with open(output_filepath, 'r') as file:
            lines = file.readlines()
            for line in lines:
                line = line.strip()
                if line:
                    descisions = line.split()
                    decisions_process[i].append(set(int(x) for x in descisions))
                    
        #assert len(proposals_process[i]) == num_rounds, f"Number of rounds do not match for process {i + 1} in {input_file} and {output_file}"
        #assert len(proposals_process[i]) == len(decisions_process[i]), f"Number of proposals and decisions do not match for process {i + 1} in {input_file} and {output_file}"
        n_lines.append(len(decisions_process[i]))
        
        for j in range(len(decisions_process[i])):
            assert proposals_process[i][j].issubset(decisions_process[i][j]), f"Proposals are not subset of decisions for process {i + 1} in round {j + 1}"
    
    print(f"Average number of rounds: {sum(n_lines) / len(n_lines)}")
    
    num_rounds = min(n_lines)
    # print("Proposals:")
    '''for i in range(num_processes):
        print(f"Process {i + 1}: {proposals_process[i]}")'''
        
    # print("\nDecisions:")
    '''for i in range(num_processes):
        print(f"Process {i + 1}: {decisions_process[i]}")'''

    print("Termination successful!")
    
    cumulative_proposals = [set() for _ in range(num_rounds)]
    for i in range(num_processes):
        for j in range(num_rounds):
            cumulative_proposals[j] |= proposals_process[i][j]
            
    
    for i in range(num_processes):
        for j in range(num_rounds):
            if i in term_processes:
                continue
            
            assert decisions_process[i][j].issubset(cumulative_proposals[j]), f"Decisions are not subset of cumulative proposals for process {i + 1} in round {j + 1}"
    
    print("Validtion successful!")
    
    for i in range(num_rounds):
        for proc_i in range(num_processes):
            for proc_j in range(proc_i + 1, num_processes):
                subset_condition_i = decisions_process[proc_i][i].issubset(decisions_process[proc_j][i])
                subset_condition_j = decisions_process[proc_j][i].issubset(decisions_process[proc_i][i])
                
                assert subset_condition_i or subset_condition_j, f"Decisions are not subset of proposals for process {proc_i + 1} and {proc_j + 1} in round {i + 1}"

    print("Consistency successful!")
    print("Lattice agreement successful!")
                