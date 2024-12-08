import os
import sys
import re

from collections import defaultdict

def count_lines(directory):
    n_processes = 0
    total_lines = 0
    pattern_b = re.compile(r"^b \d+$")
    pattern_d = re.compile(r"^d \d+ \d+$")
    
    delivered_messages = set()
    
    for filename in os.listdir(directory):
        if filename.endswith('.output'):
            n_processes += 1
            filepath = os.path.join(directory, filename)
            
            lines_set = set()
            with open(filepath, 'r') as file:
                lines = file.readlines()
                
                line_count = 0
                for i, line in enumerate(lines, 1):
                    line = line.strip()
                    if pattern_b.match(line):
                        lines_set.add(line)
                        line_count += 1
                    elif pattern_d.match(line):
                        lines_set.add(line)
                        line_count += 1
                        delivered_messages.add(line)
                    else:
                        print(f"Invalid line format in {filename} line number {i}: {line}")
                
                assert line_count == len(lines_set), f"Duplicate lines in {filename}, line count: {line_count}, unique lines: {len(lines_set)}"
                
                total_lines += line_count
                print(f"{filename}: {line_count} valid lines")
    
    print(f"Total number of valid lines in all .output files: {total_lines}")
    return delivered_messages, n_processes

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python script.py <directory_path> [optional: <term_process>, <term_process> ...]")
        sys.exit(1)
    
    directory_path = sys.argv[1]
    term_processes = set(int(x) for x in sys.argv[2:])
    delivered_messages, n_processes = count_lines(directory_path)
    print(f"Total number of processes: {n_processes}")
    print(f"Total number of delivered messages: {len(delivered_messages)}")
        
    corr = 0
    for i in range(1, n_processes + 1):
        if i in term_processes:
            continue
        
        filename = f"proc{i:02d}.output"
        with open(os.path.join(directory_path, filename), 'r') as file:
            lines = file.readlines()
            delivered_per_process_broadcaster = defaultdict(int)
            delivered_messages_for_process = set()
            for line in lines:
                line = line.strip()
                if line.startswith('d'):
                    delivered_messages_for_process.add(line)
                    
                    delivered_msg = line.split()
                    sender = int(delivered_msg[1])
                    msg = int(delivered_msg[2])
                
                    assert delivered_per_process_broadcaster[sender] < msg, f"Process {i} delivered message {msg} from process {sender} after {delivered_per_process_broadcaster[sender]}."
                    delivered_per_process_broadcaster[sender] = msg
                    
            undelivered_messages = delivered_messages - delivered_messages_for_process
            if len(undelivered_messages) > 0:
                print(f"Process {i} did not deliver {len(undelivered_messages)} messages.")
            else:
                corr += 1
                print(f"Process {i} delivered all messages successfully.")
                
    print(f"Number of correct processes: {corr}/{n_processes - len(term_processes)} ~ {corr / (n_processes - len(term_processes)) * 100:.2f}%")