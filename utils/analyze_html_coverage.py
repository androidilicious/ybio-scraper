"""
Analyze HTML scrape chunks to find missing pages.
Handles overlapping chunks and different sizes.

Author: Diwas Puri
Affiliation: Duke University
Email: diwas.puri@duke.edu
"""

import os
import re
from pathlib import Path

def analyze_coverage(data_dir="../data/raw_chunks", total_pages=3945):
    print(f"Analyzing coverage in {data_dir}...")
    
    path = Path(data_dir)
    if not path.exists():
        print(f"Directory {data_dir} not found!")
        return

    # Track covered pages
    covered_pages = set()
    files = list(path.glob("chunk_*.csv"))
    
    print(f"Found {len(files)} chunk files.")
    
    for file in files:
        # Extract range from filename chunk_START-END.csv
        match = re.search(r'chunk_(\d+)-(\d+)\.csv', file.name)
        if match:
            start = int(match.group(1))
            end = int(match.group(2))
            
            # Add pages to set
            # Note: The filename implies the range attempted, but we should check if file has data
            # For now, assuming if file exists and has content, it covers the range
            # A more robust check would count rows, but let's trust the filename for coverage first
            # since the scraper saves chunks based on processed count.
            
            # Actually, the scraper saves "current_chunk_rows" to "chunk_start_page - current_page".
            # But let's verify row counts to be sure.
            try:
                with open(file, 'r', encoding='utf-8') as f:
                    # Subtract 1 for header
                    row_count = sum(1 for _ in f) - 1
                
                if row_count > 0:
                    # If we have rows, we assume the pages in that range are covered.
                    # However, since we don't know exactly which pages produced which rows 
                    # (unless we check the content, but we didn't save page num in CSV),
                    # we have to rely on the filename range.
                    # The scraper naming logic was: f"{chunk_start_page}-{chunk_start_page + chunk_size - 1}"
                    # This implies it covers that full range.
                    covered_pages.update(range(start, end + 1))
            except Exception as e:
                print(f"Error reading {file.name}: {e}")

    # Calculate missing
    all_pages = set(range(1, total_pages + 1))
    missing = sorted(list(all_pages - covered_pages))
    
    print(f"\nTotal pages expected: {total_pages}")
    print(f"Pages covered: {len(covered_pages)}")
    print(f"Missing pages: {len(missing)}")
    
    if missing:
        print("\nMissing Page Ranges:")
        # Group into ranges
        ranges = []
        if missing:
            range_start = missing[0]
            prev = missing[0]
            for page in missing[1:]:
                if page != prev + 1:
                    ranges.append((range_start, prev))
                    range_start = page
                prev = page
            ranges.append((range_start, prev))
            
            for start, end in ranges:
                if start == end:
                    print(f"  {start}")
                else:
                    print(f"  {start}-{end}")
                    
        print("\nTo retry missing pages, run:")
        cmd_parts = []
        for start, end in ranges:
            print(f"python scrape_html_table.py --from-page {start} --to-page {end} --workers 5 --output-dir ybio_html_data")

if __name__ == "__main__":
    analyze_coverage()
