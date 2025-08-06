import boto3
import requests
import json
from datetime import datetime
import os
from dotenv import load_dotenv
import logging
from pathlib import Path

def fetch_and_store_population_data():
    # Setup logging
    log_dir = Path('logs')
    log_dir.mkdir(exist_ok=True)
    
    logging.basicConfig(
        filename=log_dir / f'api_fetch_{datetime.now().strftime("%Y%m%d_%H%M%S")}.log',
        level=logging.INFO,
        format='[%(asctime)s] %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    
    logger = logging.getLogger(__name__)
    
    try:
        # Load environment variables
        load_dotenv()
        
        # Get AWS credentials
        aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
        aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
        aws_region = os.getenv('AWS_REGION')
        bucket_name = os.getenv('S3_BUCKET_NAME')
        
        if not all([aws_access_key, aws_secret_key, aws_region, bucket_name]):
            raise EnvironmentError("Missing required AWS credentials")
        
        # Initialize S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key,
            region_name=aws_region
        )
        
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

if __name__ == "__main__":
    fetch_and_store_population_data()