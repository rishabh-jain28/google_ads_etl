from snowflake.connector import connect
import pandas as pd
from datetime import datetime
import random
import os
from dotenv import load_dotenv
load_dotenv()

# Generate mock data
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

    # Connect to Snowflake
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
        for row in df.itertuples(index=False):  # `index=False` to skip the index
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
        print(f"Data successfully inserted into source.raw_google_ads")

    except Exception as e:
        print(f"Error occurred: {e}")

# Main execution
if __name__ == '__main__':
    insert_google_ads_data()
