import yaml
import os
import requests
from datetime import datetime, timedelta
import pytz
import glob

# Set timezone and API details
timezone = pytz.timezone("UTC")
base_url = "https://stocknewsapi.com/api/v1/category"
api_token = "xubakrabgfrygi5gyebnw6vcnl7lcmrjxxikridd"

# Load and normalize tickers list
with open("tickers_list.txt", "r") as f:
    tickers_list = {line.strip().upper() for line in f if line.strip()}

# Function to validate tickers
def is_valid_ticker(ticker):
    return ticker.strip().upper() in tickers_list

# Flexible date range processing
def process_date_blocks(start_date, end_date, block_size=15):
    current_start = start_date
    while current_start < end_date:
        current_end = min(current_start + timedelta(days=block_size - 1), end_date)
        yield current_start, current_end
        current_start = current_end + timedelta(days=1)

# Define the date range for the entire period
start_date = datetime(2025, 1, 1, tzinfo=timezone)
end_date = datetime(2025, 1, 30, tzinfo=timezone)

existing_urls_cache = {}

# Track cumulative counts
cumulative_articles_stored = 0
cumulative_distinct_tickers = set()

def populate_url_cache(ticker):
    ticker_dir = f"articles/{ticker}"
    existing_urls_cache[ticker] = set()

    if os.path.exists(ticker_dir):
        for filename in glob.glob(os.path.join(ticker_dir, 'article_*.yaml')):
            try:
                with open(filename, 'r', encoding='utf-8') as f:
                    article_data = yaml.safe_load(f)
                    existing_urls_cache[ticker].add(article_data['news_url'])
            except Exception as e:
                print(f"Error reading {filename}: {e}")

# Start processing the date blocks
for current_start, current_end in process_date_blocks(start_date, end_date):
    print(f"Processing: {current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')}")

    page = 1

    while True:
        params = {
            "section": "alltickers",
            "items": 100,
            "page": page,
            "date": f"{current_start.strftime('%m%d%Y')}-{current_end.strftime('%m%d%Y')}",
            "token": api_token
        }

        response = requests.get(base_url, params=params)
        print(f"API Response Status: {response.status_code}")  # Check if API is working
        if response.status_code != 200:
            break

        news_data = response.json().get("data", [])
        print(f"Number of articles retrieved: {len(news_data)}")  # Check number of articles retrieved
        if not news_data:
            break

        for news in news_data:
            article_url = news.get("news_url", "")
            article_tickers = news.get("tickers", [])
            article_date = news.get("date", "")

            if not article_url or not article_date:
                continue

            # Filter valid tickers from the article tickers list
            valid_tickers = [t for t in article_tickers if is_valid_ticker(t)]
            print(f"Article Tickers: {article_tickers}")  # See tickers for each article
            print(f"Valid Tickers: {valid_tickers}")  # See valid tickers
            if not valid_tickers:
                continue

            try:
                # Parse the article date and ensure it falls within the current date block
                article_dt = datetime.strptime(article_date, "%a, %d %b %Y %H:%M:%S %z")
                if not (current_start <= article_dt <= current_end):
                    continue
            except ValueError:
                continue

            # Process each valid ticker for the article
            for ticker in valid_tickers:
                cumulative_distinct_tickers.add(ticker)

                # If we haven't already populated the URL cache for this ticker, do so
                if ticker not in existing_urls_cache:
                    populate_url_cache(ticker)

                # If the article URL is already in the cache, skip storing the article
                if article_url in existing_urls_cache[ticker]:
                    continue

                # Store the article as a YAML file
                article = {
                    'ticker': ticker,
                    'headline': news.get('title'),
                    'image_url': news.get('image_url', ''),
                    'date': article_dt.strftime("%Y-%m-%d"),
                    'body': news.get('text'),
                    'source': news.get('source_name'),
                    'news_url': article_url,
                    'metadata': {
                        'scrape_time': datetime.now(timezone).isoformat(),
                        'source_type': 'news'
                    }
                }

                # Save the article under the appropriate ticker directory
                ticker_dir = f"articles/{ticker}"
                os.makedirs(ticker_dir, exist_ok=True)

                # Check and determine the next article number
                num_file_path = os.path.join(ticker_dir, 'latest_article_number.txt')
                if os.path.exists(num_file_path):
                    with open(num_file_path, 'r') as num_file:
                        next_num = int(num_file.read()) + 1
                else:
                    next_num = 1  # Starting number if file doesn't exist

                # Save the updated next_num after processing
                with open(num_file_path, 'w') as num_file:
                    num_file.write(str(next_num))

                filepath = os.path.join(ticker_dir, f"article_{next_num}.yaml")

                # Write the article data to a YAML file
                with open(filepath, 'w', encoding='utf-8') as f:
                    yaml.safe_dump(article, f, allow_unicode=True, default_flow_style=False)

                # Update the URL cache
                existing_urls_cache[ticker].add(article_url)
                cumulative_articles_stored += 1

        page += 1

    print(f"Articles saved for block {current_start.strftime('%Y-%m-%d')} to {current_end.strftime('%Y-%m-%d')}")
    print(f"Till Now: Articles Stored = {cumulative_articles_stored}, Distinct Tickers = {len(cumulative_distinct_tickers)}\n")

print("Scraping completed.")
