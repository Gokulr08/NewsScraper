from ftplib import FTP
import csv

def download_nasdaq_tickers():
    try:
        ftp = FTP('ftp.nasdaqtrader.com')
        ftp.login()
        ftp.cwd('symboldirectory')

        files = ['nasdaqlisted.txt', 'otherlisted.txt']
        tickers = []

        for file in files:
            print(f"Downloading {file}...")  # Debugging line to check file download
            with open(file, 'wb') as f:
                ftp.retrbinary(f'RETR {file}', f.write)

            # Now read the downloaded file
            with open(file, 'r') as f:
                csv_reader = csv.reader(f, delimiter='|')
                next(csv_reader)  # Skip header

                for row in csv_reader:
                    if row[0] != 'File Creation Time':  # Check the first column
                        tickers.append(row[0])  # Ticker symbol is the first column

        ftp.quit()  # Quit the FTP session after downloading all files



        return tickers

    except Exception as e:
        print(f"Error: {e}")


