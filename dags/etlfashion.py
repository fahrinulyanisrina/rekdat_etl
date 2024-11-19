from airflow import DAG
from airflow.providers.http.hooks.http import HttpHook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
import logging

# Constants
COMPANIES = [
    {"symbol": "F", "table": "ford_data", "name": "Ford"},
    {"symbol": "TSLA", "table": "tesla_data", "name": "Tesla"}
]
INTERVAL = "60min"  # Time interval (hourly)
POSTGRES_CONN_ID = "postgres_default"
API_CONN_ID = "alpha_vantage_api"

# Default arguments for the DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
with DAG(
    dag_id="alpha_vantage_separate_tables_etl",
    default_args=default_args,
    schedule_interval="@hourly",
    catchup=False,
) as dag:

    @task()
    def extract_stock_data(company):
        """
        Extract stock data from Alpha Vantage API for a specific company.
        """
        logging.info(f"Starting data extraction for {company['name']} ({company['symbol']})...")
        http_hook = HttpHook(http_conn_id=API_CONN_ID, method="GET")

        # Build the API endpoint
        # Mengambil koneksi dari Airflow
        conn = http_hook.get_connection(API_CONN_ID)

        # Mengambil API key dari extra
        api_key = conn.extra_dejson.get("api_key")

        # Gunakan API key dalam endpoint
        endpoint = f"/query?function=TIME_SERIES_INTRADAY&symbol={company['symbol']}&interval={INTERVAL}&apikey={api_key}"

        logging.info(f"API Endpoint: {endpoint}")

        # Fetch data from the API
        response = http_hook.run(endpoint)
        if response.status_code != 200:
            logging.error(f"Failed to fetch data for {company['symbol']}: {response.status_code}")
            raise Exception(f"API request failed with status code {response.status_code}")

        data = response.json()
        time_series = data.get("Time Series (60min)", {})
        if not time_series:
            logging.error(f"No 'Time Series (60min)' data found in the API response for {company['symbol']}.")
            raise ValueError("Missing 'Time Series (60min)' in API response")

        logging.info(f"Fetched {len(time_series)} records for {company['name']}.")
        return {"company": company, "data": time_series}

    @task()
    def transform_stock_data(stock_payload):
        """
        Transform the stock data into a format suitable for PostgreSQL.
        """
        company = stock_payload["company"]
        stock_data = stock_payload["data"]
        logging.info(f"Starting data transformation for {company['name']}...")

        transformed_data = []
        for timestamp, values in stock_data.items():
            transformed_data.append({
                "timestamp": datetime.strptime(timestamp, "%Y-%m-%d %H:%M:%S"),
                "open": float(values["1. open"]),
                "high": float(values["2. high"]),
                "low": float(values["3. low"]),
                "close": float(values["4. close"]),
                "volume": int(values["5. volume"]),
            })
        logging.info(f"Transformed {len(transformed_data)} records for {company['name']}.")
        return {"table": company["table"], "data": transformed_data}

    @task()
    def load_stock_data(payload):
        """
        Load transformed stock data into PostgreSQL.
        """
        table_name = payload["table"]
        transformed_data = payload["data"]

        if not transformed_data:
            logging.warning(f"No data to load into {table_name}.")
            return

        logging.info(f"Starting data load to PostgreSQL for table {table_name}...")
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        conn = pg_hook.get_conn()
        cursor = conn.cursor()

        # Create table if it doesn't exist
        cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            timestamp TIMESTAMP PRIMARY KEY,
            open FLOAT,
            high FLOAT,
            low FLOAT,
            close FLOAT,
            volume BIGINT
        );
        """)

        for record in transformed_data:
            cursor.execute(f"""
            INSERT INTO {table_name} (timestamp, open, high, low, close, volume)
            VALUES (%s, %s, %s, %s, %s, %s)
            ON CONFLICT (timestamp) DO NOTHING;
            """, (
                record["timestamp"],
                record["open"],
                record["high"],
                record["low"],
                record["close"],
                record["volume"],
            ))

        conn.commit()
        cursor.close()
        logging.info(f"Data successfully loaded into PostgreSQL table {table_name}.")

    # Define the DAG workflow
    for company in COMPANIES:
        stock_data = extract_stock_data(company)
        transformed_data = transform_stock_data(stock_data)
        load_stock_data(transformed_data)