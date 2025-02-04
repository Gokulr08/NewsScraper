import json
import os
import requests
from datetime import datetime, timedelta
import pytz

timezone = pytz.timezone("UTC")

base_url = "https://stocknewsapi.com/api/v1/category"
api_token = "xubakrabgfrygi5gyebnw6vcnl7lcmrjxxikridd"

with open("tickers_list.txt", "r") as f:
    tickers_list = f.read().splitlines()

start_date = datetime(2019, 3, 1, tzinfo=timezone)
end_date = datetime(2019, 3, 31, tzinfo=timezone)

current_start = start_date
while current_start <= end_date:
    current_end = current_start + timedelta(days=15)
    if current_end > end_date:
        current_end = end_date

    date_range = f"{current_start.strftime('%m%d%Y')}-{current_end.strftime('%m%d%Y')}"
    page = 1

    tickers_stored = set()
    articles_stored = 0

    print(f"Processing date range: {current_start.strftime('%b %d, %Y')} to {current_end.strftime('%b %d, %Y')}")

    while True:
        params = {
            "section": "alltickers",
            "items": 100,
            "page": page,
            "date": date_range,
            "token": api_token
        }

        response = requests.get(base_url, params=params)

        if response.status_code == 200:
            news_data = response.json().get("data", [])

            if not news_data:
                break

            for news in news_data:
                article_date = news.get("date", "")
                article_ticker = news.get("tickers", [])

                if not any(ticker in tickers_list for ticker in article_ticker):
                    continue

                try:
                    article_datetime = datetime.strptime(article_date, "%a, %d %b %Y %H:%M:%S %z")

                    if article_datetime < current_start or article_datetime > current_end:
                        continue

                    article = {
                        'ticker': article_ticker,
                        'headline': news.get('title'),
                        'image_url': news.get('image_url', ''),
                        'date': article_date,
                        'body': news.get('text'),
                        'source': news.get('source_name'),
                        'news_url': news.get('news_url'),
                        'metadata': {
                            'scrape_time': datetime.now().isoformat(),
                            'source_type': 'news'
                        }
                    }

                    for ticker in article_ticker:
                        ticker_dir = f"articles/{ticker}"
                        if not os.path.exists(ticker_dir):
                            os.makedirs(ticker_dir)

                        existing_files = os.listdir(ticker_dir)
                        existing_articles = [file for file in existing_files if
                                             file.startswith('article_') and file.endswith('.yaml')]

                        if existing_articles:
                            existing_numbers = [int(file.split('_')[1].split('.')[0]) for file in existing_articles]
                            next_article_number = max(existing_numbers) + 1
                        else:
                            next_article_number = 1

                        file_name = f"article_{next_article_number}.yaml"
                        file_path = os.path.join(ticker_dir, file_name)

                        with open(file_path, "w", encoding="utf-8") as file:
                            json.dump(article, file, ensure_ascii=False, indent=4)

                        print(f"Saved article for {ticker} on {article_datetime.strftime('%Y-%m-%d')} as {file_name}")

                        tickers_stored.add(ticker)
                        articles_stored += 1

                except ValueError:
                    continue

            page += 1

        else:
            print(f"Error fetching data: {response.status_code}")
            break

    print(
        f"Processed {len(tickers_stored)} tickers and {articles_stored} articles from {current_start.strftime('%b %d, %Y')} to {current_end.strftime('%b %d, %Y')}.")

    current_start = current_end

print("Articles saved successfully.")
