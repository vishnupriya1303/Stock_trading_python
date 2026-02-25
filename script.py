import requests
import os
from dotenv import load_dotenv 
import time 
import json
import snowflake.connector

# Load environment variables from .env file
load_dotenv()
MASSIVE_API_KEY = os.getenv('MASSIVE_API_KEY')  # Use the correct variable name 

def run_stock_job():
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

    # Insert into Snowflake as VARIANT JSON records
    # Read Snowflake connection info from environment with sensible defaults
    SF_USER = os.getenv('SF_USER')
    SF_PASSWORD = os.getenv('SF_PASSWORD')
    # From the provided URL 'app.snowflake.com/us-east-1/ric21716',
    # use account identifier in the form '<account>.<region>'
    SF_ACCOUNT = os.getenv('SF_ACCOUNT')

    # Optional but required for DDL operations in many accounts
    SF_WAREHOUSE = os.getenv('SF_WAREHOUSE')
    SF_DATABASE = os.getenv('SF_DATABASE')
    SF_SCHEMA = os.getenv('SF_SCHEMA')
    SF_ROLE = os.getenv('SF_ROLE')

    if not SF_DATABASE or not SF_SCHEMA:
        print("Snowflake database and schema are not set. Set SF_DATABASE and SF_SCHEMA environment variables or provide them in the .env file.")
        print("Example .env entries:\nSF_DATABASE=MY_DB\nSF_SCHEMA=PUBLIC\nSF_WAREHOUSE=COMPUTE_WH")
        return

    conn_params = {
        'user': SF_USER,
        'password': SF_PASSWORD,
        'account': SF_ACCOUNT,
        'database': SF_DATABASE,
        'schema': SF_SCHEMA,
    }

    if SF_WAREHOUSE:
        conn_params['warehouse'] = SF_WAREHOUSE
    if SF_ROLE:
        conn_params['role'] = SF_ROLE

    try:
        ctx = snowflake.connector.connect(**conn_params)

        cur = ctx.cursor()
        try:
            # Insert into the user-provided `stock_tickers` table, mapping fields
            insert_sql = (
                "INSERT INTO stock_tickers ("
                "ticker, name, market, locale, primary_exchange, type, active, "
                "currency_name, cik, composite_figi, share_class_figi, last_updated_utc) "
                "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
            )

            params = []
            for t in tickers:
                params.append((
                    t.get('ticker'),
                    t.get('name'),
                    t.get('market'),
                    t.get('locale'),
                    t.get('primary_exchange'),
                    t.get('type'),
                    t.get('active'),
                    t.get('currency_name'),
                    t.get('cik'),
                    t.get('composite_figi'),
                    t.get('share_class_figi'),
                    t.get('last_updated_utc'),
                ))

            if params:
                cur.executemany(insert_sql, params)

            ctx.commit()
            print(f"Inserted {len(params)} records into Snowflake table stock_tickers in {SF_DATABASE}.{SF_SCHEMA}")
        finally:
            cur.close()
    except Exception as e:
        print("Error writing to Snowflake:", e)
    finally:
        try:
            ctx.close()
        except Exception:
            pass

if __name__ == "__main__":
    run_stock_job()