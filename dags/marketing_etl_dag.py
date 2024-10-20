from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
from snowflake.connector import connect
import random
import os
from dotenv import load_dotenv  # Import dotenv to load .env file

# Load environment variables from .env file
load_dotenv()

def generate_mock_data(num_rows=10):
    """Generates mock data for Google Ads."""
    data = []
    for _ in range(num_rows):
        data.append({
            'ad_id': f'ad_{random.randint(1000, 9999)}',
            'campaign_id': f'camp_{random.randint(100, 999)}',
            'user_id': f'user_{random.randint(1, 50)}',
            'click_time': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'cost': round(random.uniform(1.0, 100.0), 2),
            'revenue': round(random.uniform(1.0, 150.0), 2),
            'device_type': random.choice(['mobile', 'desktop']),
            'location': random.choice(['New York', 'Los Angeles', 'Chicago']),
            'impressions': random.randint(100, 10000),
            'clicks': random.randint(1, 100),
            'conversions': random.randint(0, 10),
            'conversion_rate': round(random.uniform(0.0, 100.0), 2),
            'campaign_name': f'Campaign {random.randint(1, 5)}',
            'ad_type': random.choice(['text', 'image']),
            'ad_group': f'Group {random.choice(["A", "B", "C", "D"])}',
            'created_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        })
    return data

# Insert data into Snowflake table
def insert_google_ads_data():
    # Generate mock data
    mock_data = generate_mock_data(num_rows=10)

    # Convert to DataFrame
    df = pd.DataFrame(mock_data)

    # Connect to Snowflake using environment variables
    try:
        conn = connect(
            user=os.getenv('SNOWFLAKE_USER'), 
            password=os.getenv('SNOWFLAKE_PASSWORD'), 
            account=os.getenv('SNOWFLAKE_ACCOUNT'), 
            database=os.getenv('SNOWFLAKE_DATABASE'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'), 
            schema=os.getenv('SNOWFLAKE_SCHEMA')
        )
        cursor = conn.cursor()

        # Insert each row into Snowflake
        for row in df.itertuples(index=False):
            cursor.execute(f"""
                INSERT INTO source.raw_google_ads (
                    ad_id, campaign_id, user_id, click_time, cost, revenue, 
                    device_type, location, impressions, clicks, 
                    conversions, conversion_rate, campaign_name, ad_type, 
                    ad_group, created_at
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                row.ad_id, row.campaign_id, row.user_id, row.click_time, 
                row.cost, row.revenue, row.device_type, row.location, 
                row.impressions, row.clicks, row.conversions, 
                row.conversion_rate, row.campaign_name, row.ad_type, 
                row.ad_group, row.created_at
            ))

        conn.commit()
        cursor.close()
        conn.close()
        print("Data successfully inserted into source.raw_google_ads")

    except Exception as e:
        print(f"Error occurred: {e}")

default_args = {
    'owner': 'data_engineer',
    'start_date': datetime(2024, 10, 19),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG('marketing_etl_dag', default_args=default_args, schedule_interval='@daily')

# Task to extract Google Ads data
extract_google_ads = PythonOperator(
    task_id='extract_google_ads',
    python_callable=insert_google_ads_data,
    dag=dag
)

# Task to run dbt models using BashOperator
run_dbt_models = BashOperator(
    task_id='run_dbt_models',
    bash_command='cd /home/rishabh/marketing_attribution_project/dbt_project && dbt run',
    dag=dag
)

# Defining task dependencies
extract_google_ads >> run_dbt_models
