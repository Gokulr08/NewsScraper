import requests
import yaml
import pytz
import schedule
import time
import boto3
import re
from datetime import datetime, timedelta
from download_tickers import download_nasdaq_tickers

BUCKET_NAME = "your-s3-bucket-name"
S3_CLIENT = boto3.client("s3")


# Function to replace HTML entities with readable text
def replace_html_entities(html_content):
    html_entities = {
        '&#8217;': "'", '&#8216;': "'", '&#8220;': '"', '&#8221;': '"',
        '&#8211;': '-', '&#8212;': '--', '&nbsp;': ' ', '&amp;': '&',
        '&lt;': '<', '&gt;': '>', '&quot;': '"', '&apos;': "'",
        '&#39;': "'", '&#x27;': "'",
    }
    for entity, replacement in html_entities.items():
        html_content = html_content.replace(entity, replacement)
    return html_content


# Function to extract main content by removing unwanted HTML tags
def extract_main_content(html_content):
    if not any(tag in html_content.lower() for tag in ['<html', '<body', '<div']):
        return html_content

    html_content = replace_html_entities(html_content)

    # Remove script, style, and unnecessary tags
    tags_to_remove_with_content = ['style', 'script', 'noscript', 'iframe', 'nav', 'header', 'footer']
    for tag in tags_to_remove_with_content:
        html_content = re.sub(f'<{tag}[^>]*>.*?</{tag}>', '', html_content, flags=re.DOTALL | re.IGNORECASE)

    cleaned_text = re.sub(r'<[^>]+>', '', html_content)  # Remove remaining HTML tags
    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()  # Clean extra spaces
    return cleaned_text


# Function to fetch news articles from the API
def fetch_articles(ticker, date_from, date_to):
    API_URL = "https://api.benzinga.com/api/v2/news"
    API_TOKEN = "c7471a6d2d954e2f80e8da82e272d9e4"

    params = {
        "token": API_TOKEN,
        "tickers": ticker,
        "dateFrom": date_from,
        "dateTo": date_to,
        "pageSize": 99,
        "page": 0,
        "displayOutput": "full"
    }

    all_articles = []
    while True:
        try:
            response = requests.get(API_URL, headers={"accept": "application/json"}, params=params)
            response.raise_for_status()
            data = response.json()
            if not data:
                break
            all_articles.extend(data)
            print(f"Fetched {len(data)} articles for {ticker} on page {params['page']}")
            params["page"] += 1
        except requests.RequestException as e:
            print(f"Error fetching articles for {ticker}: {e}")
            break
    return all_articles


# Function to convert API date format to YYYY-MM-DD
def convert_to_yyyy_mm_dd(date_str):
    try:
        dt = datetime.strptime(date_str, "%a, %d %b %Y %H:%M:%S %z")
        return dt.strftime("%Y-%m-%d")
    except ValueError:
        return "unknown_date"


# Function to save data to S3 in YAML format
def save_to_s3(data, ticker, created_date, index):
    yaml_data = yaml.dump(data, default_flow_style=False, sort_keys=False, allow_unicode=True, indent=4, width=80)
    s3_key = f'news/{ticker}/{created_date}/article_{index + 1}.yaml'
    try:
        S3_CLIENT.put_object(Bucket=BUCKET_NAME, Key=s3_key, Body=yaml_data)
        print(f"Uploaded to S3: {s3_key}")
    except Exception as e:
        print(f"Failed to upload {s3_key} to S3: {e}")


# Function to process and save articles
def save_articles(articles, ticker):
    if articles:
        for i, article in enumerate(articles):
            created_date = convert_to_yyyy_mm_dd(article.get("created", "unknown_date"))

            # Extract and clean the content before saving
            if "content" in article:
                article["content"] = extract_main_content(article["content"])

            save_to_s3(article, ticker, created_date, i)
        print(f"Saved {len(articles)} articles for {ticker} to S3.")
    else:
        print(f"No articles found for {ticker}.")


# Function to fetch and save articles for all tickers
def fetch_and_save_for_all_tickers(date_from, date_to):
    tickers_list = download_nasdaq_tickers()
    for ticker in tickers_list:
        print(f"\nFetching articles for {ticker}...")
        articles = fetch_articles(ticker, date_from, date_to)
        save_articles(articles, ticker)


# Function to fetch daily articles
def fetch_daily_articles():
    tz = pytz.timezone("UTC")
    today = datetime.now(tz)
    yesterday = today - timedelta(days=1)
    date_from = yesterday.strftime("%Y-%m-%dT00:00:00Z")
    date_to = today.strftime("%Y-%m-%dT23:59:59Z")
    print(f"Fetching daily articles from {date_from} to {date_to}")
    fetch_and_save_for_all_tickers(date_from, date_to)


# Function to schedule daily article fetching
def schedule_daily_task():
    schedule.every().day.at("07:00").do(fetch_daily_articles)
    print("Scheduled task to run daily at 7 AM")
    while True:
        schedule.run_pending()
        time.sleep(60)


# Main execution block
if __name__ == "__main__":
    mode = input("Enter mode (historical/daily): ").strip().lower()
    if mode == "historical":
        start_date = input("Enter start date (YYYY-MM-DD): ")
        end_date = input("Enter end date (YYYY-MM-DD): ")
        fetch_and_save_for_all_tickers(start_date, end_date)
    elif mode == "daily":
        schedule_daily_task()
    else:
        print("Invalid mode! Choose either 'historical' or 'daily'.")
