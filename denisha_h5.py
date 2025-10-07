from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime, timedelta
import requests


SNOWFLAKE_CONNECTION_ID = "snowflake_conn"
ALPHA_VANTAGE_KEY = Variable.get("alpha_vantage_api_key")
STOCK_SYMBOL = "AAPL"
SNOWFLAKE_DATABASE = "USER_DB_D"



def get_snowflake_connection():
    """Establish connection to Snowflake using Airflow's SnowflakeHook."""
    hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONNECTION_ID)
    return hook.get_conn()


@task
def fetch_recent_stock_data():
    """Fetch last 90 days of stock data from Alpha Vantage API."""
    api_url = (
        f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY"
        f"&symbol={STOCK_SYMBOL}&apikey={ALPHA_VANTAGE_KEY}"
    )

    response = requests.get(api_url)
    data = response.json()
    recent_records = []

    # Filter for only last 90 days
    cutoff_date = datetime.now() - timedelta(days=90)

    for date_str, stock_info in data.get("Time Series (Daily)", {}).items():
        date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        if date_obj >= cutoff_date:
            recent_records.append((date_str, stock_info))  # keep as tuple for transform

    return recent_records



@task
def clean_and_structure_stock_data(raw_data):
    """Transform raw API data into structured records for database loading."""
    structured_records = []

    for date_str, daily_data in raw_data:
        try:
            date_obj = datetime.strptime(date_str, "%Y-%m-%d").date()
            record = (
                STOCK_SYMBOL,
                date_obj,
                float(daily_data["1. open"]),
                float(daily_data["4. close"]),
                float(daily_data["2. high"]),
                float(daily_data["3. low"]),
                int(daily_data["5. volume"]),
            )
            structured_records.append(record)
        except Exception as error:
            print(f"Skipping {date_str} due to error: {error}")

    return structured_records



@task
def load_data_into_snowflake(stock_records):
    """Load structured stock data into Snowflake (full refresh)."""
    target_table = f"{SNOWFLAKE_DATABASE}.raw.stock_prices_denisha"
    connection = get_snowflake_connection()
    cursor = connection.cursor()

    try:
        cursor.execute("BEGIN;")

        # Create table if it doesn't exist
        cursor.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {target_table} (
                symbol VARCHAR(10) NOT NULL,
                trade_date DATE NOT NULL,
                open_price DECIMAL(12,4) NOT NULL,
                close_price DECIMAL(12,4) NOT NULL,
                high_price DECIMAL(12,4) NOT NULL,
                low_price DECIMAL(12,4) NOT NULL,
                volume BIGINT NOT NULL,
                PRIMARY KEY (symbol, trade_date)
            );
            """
        )

        # Perform full refresh
        cursor.execute(f"DELETE FROM {target_table};")

        insert_sql = f"""
            INSERT INTO {target_table}
            (symbol, trade_date, open_price, close_price, high_price, low_price, volume)
            VALUES (%s, %s, %s, %s, %s, %s, %s);
        """

        cursor.executemany(insert_sql, stock_records)
        cursor.execute("COMMIT;")

        print(f" Successfully loaded {len(stock_records)} records into {target_table}")

    except Exception as error:
        cursor.execute("ROLLBACK;")
        print(f" Load failed, transaction rolled back. Error: {error}")
        raise

    finally:
        cursor.close()
        connection.close()



with DAG(
    dag_id="denisha_stock_data_etl",
    start_date=datetime(2025, 10, 3),
    schedule="0 15 * * *",  # Every day at 15:00 UTC
    catchup=False,
    tags=["ETL", "Snowflake", "Denisha"],
) as dag:

    raw_stock_data = fetch_recent_stock_data()
    transformed_data = clean_and_structure_stock_data(raw_stock_data)
    load_data_into_snowflake(transformed_data)

