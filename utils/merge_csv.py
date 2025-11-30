"""
Merge multiple CSV files into a single file
Handles duplicate headers and preserves data integrity

Author: Diwas Puri
Affiliation: Duke University
Email: diwas.puri@duke.edu
"""

import os
from pathlib import Path
import re

def merge_csv_files(data_dirs=None, output_file="organizations_merged.csv"):
    """Merge all CSV files from directories into one"""
    if data_dirs is None:
        data_dirs = ["../data/raw_chunks"]
    elif isinstance(data_dirs, str):
        data_dirs = [data_dirs]
    
    csv_files = []
    for data_dir in data_dirs:
        data_path = Path(data_dir)
        if data_path.exists():
            # Find all CSV files (excluding the merged file if it exists)
            files = [f for f in data_path.glob("*.csv") if 'merged' not in f.name and 'complete' not in f.name and 'deduped' not in f.name]
            csv_files.extend(files)
            print(f"Found {len(files)} files in {data_dir}/")
    
    if not csv_files:
        print(f"No CSV files found in any directory!")
        return
    
    # Sort files by page range for logical order
    def get_page_range(filename):
        # Try organizations_X-Y pattern
        match = re.search(r'organizations_(\d+)-(\d+)', filename.name)
        if match:
            return int(match.group(1))
        
        # Try chunk_X-Y pattern
        match = re.search(r'chunk_(\d+)-(\d+)', filename.name)
        if match:
            return int(match.group(1))
            
        return 0
    
    csv_files = sorted(csv_files, key=get_page_range)
    
    print(f"\n{'='*60}")
    print(f"Merging {len(csv_files)} CSV file(s)...")
    print(f"{'='*60}\n")
    
    output_path = Path(data_dir) / output_file
    total_rows = 0
    header_written = False
    
    with open(output_path, 'w', encoding='utf-8', newline='') as outfile:
        for i, csv_file in enumerate(csv_files, 1):
            print(f"[{i}/{len(csv_files)}] Processing {csv_file.name}...", end=' ')
            
            with open(csv_file, 'r', encoding='utf-8') as infile:
                lines = infile.readlines()
                
                if not lines:
                    print("(empty)")
                    continue
                
                # Write header only once
                if not header_written:
                    outfile.write(lines[0])
                    header_written = True
                
                # Write data rows (skip header)
                data_rows = lines[1:]
                outfile.write(''.join(data_rows))
                
                row_count = len(data_rows)
                total_rows += row_count
                print(f"({row_count:,} rows)")
    
    print(f"\n{'='*60}")
    print(f"✅ Merge complete!")
    print(f"{'='*60}")
    print(f"Output file: {output_path}")
    print(f"Total rows: {total_rows:,}")
    print(f"File size: {output_path.stat().st_size / (1024*1024):.2f} MB")
    print(f"{'='*60}\n")

def deduplicate_csv(input_file, output_file=None):
    """Remove duplicate rows from CSV file"""
    if output_file is None:
        base = Path(input_file).stem
        output_file = f"{base}_deduped.csv"
    
    print(f"\n{'='*60}")
    print(f"Deduplicating {input_file}...")
    print(f"{'='*60}\n")
    
    seen = set()
    unique_rows = []
    duplicates = 0
    
    with open(input_file, 'r', encoding='utf-8') as f:
        header = f.readline()
        unique_rows.append(header)
        
        for line in f:
            if line in seen:
                duplicates += 1
            else:
                seen.add(line)
                unique_rows.append(line)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.writelines(unique_rows)
    
    print(f"Original rows: {len(unique_rows) + duplicates - 1:,}")
    print(f"Unique rows: {len(unique_rows) - 1:,}")
    print(f"Duplicates removed: {duplicates:,}")
    print(f"Output file: {output_file}")
    print(f"{'='*60}\n")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Merge CSV files')
    parser.add_argument('--data-dir', type=str, default='../data/raw_chunks',
                        help='Directory containing CSV files (default: ../data/raw_chunks)')
    parser.add_argument('--output', type=str, default='organizations_merged.csv',
                        help='Output filename (default: organizations_merged.csv)')
    parser.add_argument('--dedupe', action='store_true',
                        help='Remove duplicate rows after merging')
    
    args = parser.parse_args()
    
    merge_csv_files(args.data_dir, args.output)
    
    if args.dedupe:
        merged_path = Path(args.data_dir) / args.output
        if merged_path.exists():
            deduplicate_csv(merged_path, merged_path.parent / f"{Path(args.output).stem}_deduped.csv")
