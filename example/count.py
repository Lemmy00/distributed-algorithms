import os
import sys
import re

def count_lines_in_stdout_files(directory):
    total_lines = 0
    pattern_b = re.compile(r"^b \d+$")
    pattern_d = re.compile(r"^d \d+ \d+$")
    
    for filename in os.listdir(directory):
        if filename.endswith('.output'):
            filepath = os.path.join(directory, filename)
            
            lines_set = set()
            with open(filepath, 'r') as file:
                lines = file.readlines()
                
                line_count = 0
                for line in lines:
                    line = line.strip()
                    if pattern_b.match(line) or pattern_d.match(line):
                        lines_set.add(line)
                        line_count += 1
                    else:
                        print(f"Invalid line format in {filename}: {line}")
                
                assert line_count == len(lines_set), f"Duplicate lines in {filename}, line count: {line_count}, unique lines: {len(lines_set)}"
                
                total_lines += line_count
                print(f"{filename}: {line_count} valid lines")
    
    print(f"Total number of valid lines in all .output files: {total_lines}")

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python script.py <directory_path>")
        sys.exit(1)
    
    directory_path = sys.argv[1]
    count_lines_in_stdout_files(directory_path)
