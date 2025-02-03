import requests
import boto3
import yaml
from datetime import datetime
import schedule
import time

# AWS S3 Configuration
s3 = boto3.client('s3')
bucket_name = 'your-s3-bucket-name'  # Replace with your bucket name

# API Configuration
api_token = 'xubakrabgfrygi5gyebnw6vcnl7lcmrjxxikridd'
base_url = 'https://stocknewsapi.com/api/v1'

tickers = [
    "AAPL", "MSFT", "NVDA", "AMZN", "GOOGL", "META", "TSLA", "AVGO", "BRK-B", "TSM",
    "WMT", "JPM", "LLY", "V", "MA", "UNH", "ORCL", "XOM", "COST", "NFLX",
    "HD", "PG", "NVO", "JNJ", "BAC", "CRM", "ABBV", "SAP", "ASML", "KO",
    "CVX", "TMUS", "WFC", "TM", "MRK", "CSCO", "ACN", "IBM", "TMO", "BABA",
    "AXP", "MS", "ABT", "AZN", "GE", "GS", "BX", "LIN", "NOW", "PEP",
    "NVS", "MCD", "DIS", "ISRG", "SHEL", "PM"
]


def fetch_and_upload_articles():
    date_range = 'yesterday+000000-today+070000'

    tickers_with_articles = 0
    tickers_without_articles = 0

    for ticker in tickers:
        params = {
            'tickers': ticker,
            'datetimerange': date_range,
            'items': 100,
            'token': api_token,
            'page': 1
        }

        response = requests.get(base_url, params=params)
        data = response.json()

        if 'data' in data and data['data']:
            articles = data['data']
            for idx, article in enumerate(articles):
                metadata = {
                    'scrape_time': datetime.now().isoformat(),
                    'source_type': 'news'
                }

                article_data = {
                    'ticker': ticker,
                    'headline': article.get('title', ''),
                    'date': article.get('date', ''),
                    'body': article.get('text', ''),
                    'source': article.get('source_name', ''),
                    'url': article.get('news_url', ''),
                    'metadata': metadata
                }

                # this is help to convert to YAML format
                yaml_data = yaml.dump(article_data)

                # Define S3 file path
                s3_key = f'news/{ticker}/{datetime.now().strftime("%Y-%m-%d")}/article_{idx + 1}.yaml'

                # Upload to S3
                s3.put_object(Bucket=bucket_name, Key=s3_key, Body=yaml_data)

            print(f"Uploaded articles for {ticker} to S3")
            tickers_with_articles += 1
        else:
            print(f"No articles found for {ticker}")
            tickers_without_articles += 1

    print(f"\nTotal tickers with articles: {tickers_with_articles}")
    print(f"Total tickers without articles: {tickers_without_articles}")


# Schedule to run daily at 7:00 AM
schedule.every().day.at("07:00").do(fetch_and_upload_articles)

# Keep the script running
while True:
    schedule.run_pending()
    time.sleep(60)
