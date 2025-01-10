import sys
import argparse

## This script corrects bed file format for expected input to newer version of artic

def process_primers(input_file, output_file):
    with open(input_file, 'r') as f:
        lines = f.readlines()
    
    processed_lines = []
    for line in lines:
        columns = line.strip().split('\t')
        
        # Check if "LEFT" or "RIGHT" is in the primer name
        if "LEFT" in columns[3]:
            columns.append("+")
        elif "RIGHT" in columns[3]:
            columns.append("-")
            
        # Join the columns back together with tabs
        processed_lines.append("\t".join(columns))
    
    # Write to output file
    with open(output_file, 'w') as f:
        for line in processed_lines:
            f.write(line + '\n')

def main():
    parser = argparse.ArgumentParser(description='Process primer file to add +/- column based on LEFT/RIGHT primers')
    parser.add_argument('input', help='Input file path')
    parser.add_argument('output', help='Output file path')
    
    args = parser.parse_args()
    
    try:
        process_primers(args.input, args.output)
        print(f"Processing complete. Results written to {args.output}")
    except FileNotFoundError:
        print(f"Error: Could not find input file '{args.input}'")
        sys.exit(1)
    except Exception as e:
        print(f"An error occurred: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()