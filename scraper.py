import os
import time
import json
import logging
import requests
import pandas as pd
from bs4 import BeautifulSoup
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("pipeline.log"),
        logging.StreamHandler()
    ]
)

class BooksETLPipeline:
    def __init__(self, base_url: str):
        self.base_url = base_url
        self.session = requests.Session()
        
        # Configure Best Practice Headers
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://google.com'
        })
        
        # Implement Robust Retry Logic
        retry_strategy = Retry(
            total=3,
            backoff_factor=1.5,  # Delays: 1.5s, 3s, 4.5s
            status_forcelist=[429, 500, 502, 503, 504]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)

    def extract_page(self, page_num: int) -> str:
        """EXTRACT: Fetches raw HTML content from a target page with rate limiting."""
        url = f"{self.base_url}/catalogue/page-{page_num}.html"
        try:
            # Respectful Rate Limiting
            time.sleep(1.0) 
            
            logging.info(f"Extracting data from: {url}")
            response = self.session.get(url, timeout=10)
            
            if response.status_code == 404:
                return None  # Reached the end of available pages
                
            response.raise_for_status()
            return response.text
        except requests.RequestException as e:
            logging.error(f"Failed to fetch page {page_num}: {e}")
            return None

    def transform_data(self, html_content: str) -> list:
        """TRANSFORM: Parses raw HTML and maps it to structured dictionaries."""
        if not html_content:
            return []
            
        soup = BeautifulSoup(html_content, 'html.parser')
        books = soup.find_all('article', class_='product_pod')
        parsed_books = []
        
        # Mapping for text ratings to integers
        rating_map = {"One": 1, "Two": 2, "Three": 3, "Four": 4, "Five": 5}

        for book in books:
            try:
                title = book.h3.a['title']
                
                # Raw text cleanup (e.g., "£51.77")
                price_raw = book.find('p', class_='price_color').text
                price_clean = float(price_raw.replace('£', '').replace('$', ''))
                
                # Extract rating from class name (e.g., class="star-rating Three")
                rating_classes = book.find('p', class_='star-rating')['class']
                rating_text = [c for c in rating_classes if c != 'star-rating'][0]
                rating_int = rating_map.get(rating_text, 0)
                
                availability = book.find('p', class_='instock availability').text.strip()

                parsed_books.append({
                    "title": title,
                    "price_gbp": price_clean,
                    "rating_stars": rating_int,
                    "availability": availability
                })
            except Exception as e:
                logging.warning(f"Error parsing structural element: {e}")
                continue
                
        return parsed_books

    def generate_analytics_report(self, df: pd.DataFrame):
        """LOAD/REPORT: Computes summary statistics and saves report metrics."""
        report_path = "output/summary_report.json"
        
        stats = {
            "total_items_scraped": int(df.shape[0]),
            "average_price": float(df["price_gbp"].mean()),
            "median_price": float(df["price_gbp"].median()),
            "max_price": float(df["price_gbp"].max()),
            "min_price": float(df["price_gbp"].min()),
            "average_rating": float(df["rating_stars"].mean()),
            "stock_status_counts": df["availability"].value_counts().to_dict()
        }
        
        with open(report_path, 'w') as f:
            json.dump(stats, f, indent=4)
            
        logging.info("--- AUTOMATED ANALYTICAL REPORT ---")
        logging.info(json.dumps(stats, indent=4))

    def run_pipeline(self, max_pages: int = 3):
        """Orchestrates the E-T-L pipeline sequence."""
        all_data = []
        os.makedirs("output", exist_ok=True)
        
        logging.info("Starting ETL Pipeline Execution...")
        
        for page in range(1, max_pages + 1):
            html = self.extract_page(page)
            if not html:
                logging.info("No more pages found or structural extraction halted.")
                break
                
            page_data = self.transform_data(html)
            all_data.extend(page_data)
            logging.info(f"Successfully processed page {page}. Items found: {len(page_data)}")

        if all_data:
            # Transform to Dataframe
            df = pd.DataFrame(all_data)
            
            # LOAD: Save structured dataset to CSV
            csv_path = "output/books_dataset.csv"
            df.to_csv(csv_path, index=False)
            logging.info(f"Dataset successfully loaded to file path: {csv_path}")
            
            # AUTOMATED REPORT GENERATION
            self.generate_analytics_report(df)
            logging.info("Pipeline executed successfully without critical faults.")
        else:
            logging.error("Pipeline finished with zero records extracted.")

if __name__ == "__main__":
    TARGET_URL = "https://toscrape.com"
    pipeline = BooksETLPipeline(base_url=TARGET_URL)
    # Scraping first 3 pages as a production sample proof-of-concept
    pipeline.run_pipeline(max_pages=3)
