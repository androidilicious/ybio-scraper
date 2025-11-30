"""
Scrape YBIO data by parsing HTML tables directly.
This avoids the issue where the CSV download button returns the same file.

Author: Diwas Puri
Affiliation: Duke University
Email: diwas.puri@duke.edu
"""

import requests
import concurrent.futures
import time
import os
import csv
import pickle
import argparse
from pathlib import Path
from bs4 import BeautifulSoup

# Configuration
BASE_URL = "https://ybio-brillonline-com.proxy.lib.duke.edu/ybio"
OUTPUT_DIR = "data/raw_chunks"
MAX_WORKERS = 5  # Be gentle with the server
MAX_RETRIES = 3
TIMEOUT = 30

class HTMLScraper:
    def __init__(self, from_page, to_page, max_workers, output_dir, cookie_file):
        self.from_page = from_page
        self.to_page = to_page
        self.max_workers = max_workers
        self.output_dir = output_dir
        self.cookie_file = cookie_file
        self.session = requests.Session()
        self.failed = []
        self.total_rows = 0
        
        # Setup output directory
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
            
        # Load cookies
        self.load_cookies()
        
        # Set headers to mimic browser
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Referer': BASE_URL
        })

    def load_cookies(self):
        """Load cookies from pickle file"""
        if os.path.exists(self.cookie_file):
            try:
                with open(self.cookie_file, 'rb') as f:
                    cookies = pickle.load(f)
                    # Handle both list (from browser_cookie3) and dict (standard)
                    if isinstance(cookies, list):
                        for cookie in cookies:
                            self.session.cookies.set_cookie(cookie)
                    else:
                        self.session.cookies.update(cookies)
                print(f"✓ Loaded authentication cookies from {self.cookie_file}")
            except Exception as e:
                print(f"Error loading cookies: {e}")
                print("⚠️  Proceeding without cookies (might fail if auth required)")
        else:
            print(f"⚠️  Cookie file {self.cookie_file} not found!")

    def scrape_page(self, page_num):
        """Scrape a single page and return list of rows"""
        url = f"{BASE_URL}?page={page_num}"
        
        for attempt in range(MAX_RETRIES):
            try:
                response = self.session.get(url, timeout=TIMEOUT)
                
                if response.status_code == 200:
                    # Parse HTML
                    soup = BeautifulSoup(response.content, 'html.parser')
                    
                    # Find the table
                    table = soup.find('table', class_='views-table')
                    
                    if not table:
                        # Check if we're redirected to login
                        if "login" in response.url or "shibboleth" in response.url:
                            print(f"⚠️  Page {page_num}: Redirected to login page!")
                            return None
                        
                        print(f"⚠️  Page {page_num}: Table not found (Attempt {attempt+1})")
                        continue
                    
                    # Extract rows
                    rows = []
                    tbody = table.find('tbody')
                    if tbody:
                        for tr in tbody.find_all('tr'):
                            cols = []
                            for td in tr.find_all('td'):
                                cols.append(td.get_text(strip=True))
                            if cols:
                                rows.append(cols)
                    
                    return rows
                
                elif response.status_code == 403 or response.status_code == 401:
                    print(f"⚠️  Page {page_num}: Access Denied ({response.status_code})")
                    return None
                else:
                    print(f"⚠️  Page {page_num}: Status {response.status_code}")
                    
            except Exception as e:
                print(f"⚠️  Page {page_num}: Error {e}")
            
            time.sleep(1 * (attempt + 1))  # Backoff
            
        return None

    def save_chunk(self, rows, chunk_id):
        """Save a chunk of rows to CSV"""
        if not rows:
            return
            
        filename = f"{self.output_dir}/chunk_{chunk_id}.csv"
        
        # Define headers based on inspection
        headers = ['Name', 'Acronym', 'Founded', 'City', 'Country', 'Type I', 'Type II', 'UID']
        
        with open(filename, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(headers)
            writer.writerows(rows)
            
        print(f"✓ Saved {len(rows)} rows to {filename}")

    def run(self):
        """Run the scraper"""
        print(f"Starting HTML scrape for pages {self.from_page}-{self.to_page}")
        print(f"Workers: {self.max_workers}")
        
        start_time = time.time()
        all_rows = []
        processed_count = 0
        
        # Chunk saving
        chunk_size = 10
        current_chunk_rows = []
        chunk_start_page = self.from_page
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all tasks
            future_to_page = {
                executor.submit(self.scrape_page, page): page 
                for page in range(self.from_page, self.to_page + 1)
            }
            
            for i, future in enumerate(concurrent.futures.as_completed(future_to_page)):
                page = future_to_page[future]
                try:
                    rows = future.result()
                    if rows:
                        all_rows.extend(rows)
                        current_chunk_rows.extend(rows)
                        processed_count += 1
                        
                        # Save chunk if needed
                        if processed_count % chunk_size == 0:
                            self.save_chunk(current_chunk_rows, f"{chunk_start_page}-{chunk_start_page + chunk_size - 1}")
                            current_chunk_rows = []
                            chunk_start_page += chunk_size
                            
                        if processed_count % 10 == 0:
                            print(f"Progress: {processed_count}/{(self.to_page - self.from_page + 1)} pages scraped")
                    else:
                        self.failed.append(page)
                except Exception as e:
                    print(f"Page {page} generated an exception: {e}")
                    self.failed.append(page)
        
        # Save remaining rows
        if current_chunk_rows:
            self.save_chunk(current_chunk_rows, f"{chunk_start_page}-{self.to_page}")
        
        duration = time.time() - start_time
        print(f"\n{'='*60}")
        print(f"Scraping Complete!")
        print(f"Time: {duration:.2f} seconds")
        print(f"Total pages processed: {processed_count}")
        print(f"Total rows extracted: {len(all_rows)}")
        print(f"Failed pages: {len(self.failed)}")
        if self.failed:
            print(f"Failed: {self.failed}")
        print(f"{'='*60}\n")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Scrape YBIO HTML tables')
    parser.add_argument('--from-page', type=int, default=1, help='Start page')
    parser.add_argument('--to-page', type=int, default=3945, help='End page')
    parser.add_argument('--workers', type=int, default=10, help='Max workers')
    parser.add_argument('--output-dir', type=str, default='ybio_html_data', help='Output directory')
    parser.add_argument('--cookies', type=str, default='cookies.pkl', help='Cookie file')
    
    args = parser.parse_args()
    
    scraper = HTMLScraper(
        from_page=args.from_page,
        to_page=args.to_page,
        max_workers=args.workers,
        output_dir=args.output_dir,
        cookie_file=args.cookies
    )
    
    scraper.run()
