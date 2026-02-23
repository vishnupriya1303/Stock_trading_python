import requests
import os
from dotenv import load_dotenv 
import time 
import csv

# Load environment variables from .env file
load_dotenv()
MASSIVE_API_KEY = os.getenv('MASSIVE_API_KEY')  # Use the correct variable name 

url=f"https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit=1000&sort=ticker&apiKey={MASSIVE_API_KEY}"

response = requests.get(url)
data = response.json()
tickers = []

while True:

    if 'results' not in data:
        print("Unexpected response structure:")
        print(data)
        break

    tickers.extend(data['results'])

    next_url = data.get('next_url')

    if not next_url:
        break

    print("Requesting next page...")

    time.sleep(1)  # prevents rate limit

    response = requests.get(next_url + f"&apiKey={MASSIVE_API_KEY}")

    if response.status_code != 200:
        print("HTTP Error:", response.status_code)
        print(response.text)
        break

    data = response.json()
    print(data)

print(f"Total tickers retrieved: {len(tickers)}")
file_name = "tickers.csv"

# Collect all unique field names dynamically
fieldnames = set()
for ticker in tickers:
    fieldnames.update(ticker.keys())

fieldnames = list(fieldnames)

with open(file_name, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    
    writer.writeheader()
    writer.writerows(tickers)

print(f"CSV file created successfully: {file_name}")