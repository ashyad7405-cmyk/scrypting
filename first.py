import os
import logging
import requests
from bs4 import BeautifulSoup
import pandas as pd
from datetime import datetime

# Setup basic logging for the pipeline
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class ScrapingPipeline:
    def __init__(self, target_url):
        self.target_url = target_url
        # Anti-bot measures: Mimic a real browser browser headers
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "Accept-Language": "en-US,en;q=0.9"
        }

    def extract(self):
        """Step 1: Fetch raw HTML with error handling"""
        try:
            logging.info(f"Initiating extraction from: {self.target_url}")
            response = requests.get(self.target_url, headers=self.headers, timeout=10)
            response.raise_for_status() # Trigger error for bad status codes (404, 500, etc.)
            return response.text
        except requests.exceptions.RequestException as e:
            logging.error(f"Extraction failed: {e}")
            return None

    def transform(self, raw_html):
        """Step 2: Parse raw HTML and transform into structured Pandas DataFrame"""
        if not raw_html:
            return None
        
        soup = BeautifulSoup(raw_html, "html.parser")
        parsed_data = []

        # Example scenario: Scraping a list of books/products
        # Adjust selectors (.product-pod, h3, .price_color) to match your target website
        items = soup.find_all("article", class_="product_pod")
        
        for item in items:
            try:
                title = item.find("h3").find("a")["title"].strip()
                price_str = item.find("p", class_="price_color").text.strip()
                
                # Basic cleaning: convert price string to a clean float
                price = float(price_str.replace("£", "").replace("$", ""))
                
                parsed_data.append({
                    "title": title,
                    "price": price,
                    "scraped_at": datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                })
            except (AttributeError, ValueError) as e:
                logging.warning(f"Skipping an item due to parsing anomaly: {e}")
                continue

        df = pd.DataFrame(parsed_data)
        logging.info(f"Successfully transformed {len(df)} items.")
        return df

    def load(self, df, output_path="output_data.csv"):
        """Step 3: Save the clean dataset (Append mode for continuous pipelines)"""
        if df is None or df.empty:
            logging.warning("No data available to load.")
            return
        
        # Check if file exists to append or write fresh with headers
        file_exists = os.path.isfile(output_path)
        df.to_csv(output_path, mode='a', index=False, header=not file_exists)
        logging.info(f"Pipeline complete. Data successfully loaded to {output_path}")

    def run(self):
        """Execute the full ETL Pipeline"""
        raw_html = self.extract()
        structured_df = self.transform(raw_html)
        self.load(structured_df)

if __name__ == "__main__":
    # Test target: A safe, public sandbox website meant for scraping practices
    URL = "https://books.toscrape.com/"
    pipeline = ScrapingPipeline(target_url=URL)
    pipeline.run()
