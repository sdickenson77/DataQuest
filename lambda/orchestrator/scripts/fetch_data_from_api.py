import boto3
import requests
import json
from datetime import datetime
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    handler = logging.StreamHandler()  # stdout -> CloudWatch Logs
    formatter = logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

def fetch_and_store_population_data():
    logger.info("Starting fetch...")


    try:

        bucket_name = 'rearc-part1'

        # Initialize S3 client
        s3_client = boto3.client('s3')
        
        # API endpoint
        url = "https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population"
        
        logger.info(f"Making API request to {url}")
        
        # Make the API request
        response = requests.get(url)
        response.raise_for_status()
        
        # Parse the JSON response
        data = response.json()
        
        # Create filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        s3_key = f'population_data/population_data_{timestamp}.json'
        
        # Convert data to JSON string
        json_data = json.dumps(data, indent=2, ensure_ascii=False)
        
        # Upload to S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=json_data,
            ContentType='application/json'
        )
        
        logger.info(f"Data successfully uploaded to s3://{bucket_name}/{s3_key}")
        
        # Log some basic information about the data
        if 'data' in data:
            record_count = len(data['data'])
            years = sorted(set(item['Year'] for item in data['data']))
            logger.info(f"Uploaded {record_count} records spanning years {years[0]}-{years[-1]}")
            
            # Log the most recent population data
            latest_year = max(years)
            latest_population = next(
                item['Population'] 
                for item in data['data'] 
                if item['Year'] == latest_year
            )
            logger.info(f"Most recent population ({latest_year}): {latest_population:,}")
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Error making API request: {str(e)}")
        raise
    except json.JSONDecodeError as e:
        logger.error(f"Error decoding JSON response: {str(e)}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {str(e)}")
        raise
    logger.info("Done.")

if __name__ == "__main__":
    fetch_and_store_population_data()