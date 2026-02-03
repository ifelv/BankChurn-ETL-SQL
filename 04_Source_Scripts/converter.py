import csv
import json
import argparse
from pathlib import Path

def csv_to_json(csv_file_path, json_file_path):
    # Convert string paths to Path objects for better cross-OS compatibility
    csv_path = Path(csv_file_path)
    json_path = Path(json_file_path)

    # Check if input file exists
    if not csv_path.exists():
        print(f"Error: The file '{csv_path}' was not found.")
        return

    data = []
    try:
        # opens the file 
        with open(csv_path, 'r', encoding='utf-8') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                data.append(row)
        
        # saves the file
        with open(json_path, 'w', encoding='utf-8') as json_file:
            json.dump(data, json_file, indent=4)
            
        print(f"Success! Converted '{csv_path.name}' to '{json_path.name}'")
        print(f"Full output path: {json_path.resolve()}")

    except Exception as e:
        print(f"An error occurred: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Convert CSV files to JSON format.')
    
    # Argument 1: Input file (default looks for 'data.csv' in the same folder)
    parser.add_argument(
        '--input', 
        '-i', 
        default='data.csv', 
        help='Path to the input CSV file (default: data.csv)'
    )
    
    # Argument 2: Output file (default looks for 'output.json' in the same folder)
    parser.add_argument(
        '--output', 
        '-o', 
        default='output.json', 
        help='Path to the output JSON file (default: output.json)'
    )

    args = parser.parse_args()
    
    csv_to_json(args.input, args.output)