import os
import psycopg2
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Slack configuration
SLACK_BOT_TOKEN = os.getenv('SLACK_BOT_TOKEN')
CHANNEL_ID = "C072VNQC7PH"  # Replace with your actual channel ID

# Database configuration
DB_HOST = os.getenv('DB_HOST')
DB_NAME = os.getenv('DB_NAME')
DB_USER = os.getenv('DB_USER')
DB_PASS = os.getenv('DB_PASS')

def get_db_data():
    try:
        # Add connect_timeout and options for longer query timeout
        conn = psycopg2.connect(
            host=DB_HOST,
            database=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            connect_timeout=30,  # Connection timeout in seconds
            options='-c statement_timeout=900000'  # Query timeout in milliseconds (15 minutes)
        )
        
        cur = conn.cursor()
        
        # Set a longer statement timeout for this specific connection
        cur.execute("SET statement_timeout = '900000'")  # 15 minutes in milliseconds
        
        cur.execute("SELECT * FROM public.latest_block_and_missing_view")
        result = cur.fetchone()
        cur.close()
        conn.close()
        return result
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def send_slack_message(message):
    client = WebClient(token=SLACK_BOT_TOKEN)
    try:
        response = client.chat_postMessage(
            channel=CHANNEL_ID,
            text=message
        )
    except SlackApiError as e:
        print(f"Error sending message: {e}")

def main():
    data = get_db_data()
    if data:
        latest_block, timestamp, missing_blocks = data
        message = f"""
:chart_with_upwards_trend: *Daily Fossil DB Update* :chart_with_upwards_trend:

- Latest Block Number: `{latest_block}`
- Timestamp: `{timestamp}`
- Total Missing Blocks: `{missing_blocks}`

Keep an eye on those missing blocks! :eyes:
        """
        send_slack_message(message)
    else:
        send_slack_message("Error: Unable to retrieve data from the database.")

if __name__ == "__main__":
    print("DB_HOST:", DB_HOST)
    print("DB_NAME:", DB_NAME)
    print("DB_USER:", DB_USER)
    print("DB_PASS:", DB_PASS[:3] + "*****")  # Print only first 3 characters of password for security
    main()