# YBIO Scraper & Analysis Pipeline

A robust data pipeline to scrape, process, and analyze data from the **Yearbook of International Organizations (YBIO)**. This tool extracts detailed information about international organizations, handles authentication via cookies, and provides analytical insights.

## 👤 Author

**Diwas Puri**  
Duke University  
📧 [diwas.puri@duke.edu](mailto:diwas.puri@duke.edu)

---

## 🚀 Features

- **Automated Scraping**: Iterates through thousands of pages on YBIO to extract organization data.
- **Robust Error Handling**: Retries failed pages and saves data in chunks to prevent loss.
- **Data Cleaning**: Deduplicates entries, removes artifacts, and standardizes formats.
- **Analysis**: Jupyter notebooks for visualizing geographic distribution, founding timelines, and organization types.

## 📂 Project Structure

```
├── analysis/
│   ├── analysis.ipynb       # Visualizations and insights
│   └── data_cleanup.ipynb   # Data cleaning logic
├── data/
│   ├── organizations_clean.csv  # Final cleaned dataset
│   └── raw_chunks/          # Raw scraped data chunks
├── utils/
│   ├── merge_csv.py         # Script to merge raw chunks
│   └── analyze_html_coverage.py # Checks for missing pages
├── scrape_html_table.py     # Main scraper script
├── requirements.txt         # Python dependencies
└── README.md
```

## 🛠️ Installation

1.  **Clone the repository**:
    ```bash
    git clone https://github.com/yourusername/ybio-scraper.git
    cd ybio-scraper
    ```

2.  **Install dependencies**:
    ```bash
    pip install -r requirements.txt
    ```

## 🔑 Authentication

Access to YBIO requires institutional login (e.g., via Duke University). This scraper uses browser cookies for authentication.

1.  Log in to YBIO in your web browser.
2.  Export your cookies to a file named `cookies.pkl` in the root directory.
    *   *Note: Cookie extraction scripts are provided in `utils/` but are excluded from the repo for security.*

## 💻 Usage

### 1. Scrape Data
Run the main scraper to fetch data from the website.
```bash
python scrape_html_table.py --workers 5
```
*Data is saved to `data/raw_chunks/`.*

### 2. Merge & Deduplicate
Combine all raw chunks into a single CSV file.
```bash
python utils/merge_csv.py
```

### 3. Clean Data
Run the cleanup notebook or script to remove duplicates and fix formatting.
*   Open `analysis/data_cleanup.ipynb` and run all cells.

### 4. Analyze
Explore the dataset using the analysis notebook.
*   Open `analysis/analysis.ipynb` to see charts and statistics.

## 📊 Dataset Overview

The final dataset includes:
- **Name**: Organization name
- **Acronym**: Abbreviation
- **Founded**: Year of establishment
- **Location**: City and Country
- **Type**: Classification (Type I/II)

---
*Disclaimer: This tool is for educational and research purposes only. Please respect the website's terms of service and crawl rate limits.*
