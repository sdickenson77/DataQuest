from typing import Dict
import requests
from dotenv import load_dotenv
from bs4 import BeautifulSoup
import boto3
import os
from urllib.parse import urljoin
import logging

# Configure CloudWatch-compatible logging (stdout/stderr)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Avoid duplicate handlers on Lambda warm starts
if not any(isinstance(h, logging.StreamHandler) for h in logger.handlers):
    _handler = logging.StreamHandler()
    _handler.setFormatter(logging.Formatter('%(asctime)s %(levelname)s %(name)s: %(message)s'))
    logger.addHandler(_handler)

def get_website_files(url: str, session: requests.Session) -> Dict:
    try:
        response = session.get(url)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        links = soup.find_all('a')

        website_files = {}

        for link in links:
            file_url = urljoin(url, link.get('href'))
            if file_url.startswith(url) and not file_url.endswith('/'):
                try:
                    head_response = session.head(file_url)
                    head_response.raise_for_status()

                    filename = os.path.basename(file_url)
                    website_files[filename] = {
                        'size': int(head_response.headers.get('content-length', '0')),
                        'last_modified': head_response.headers.get('last-modified', ''),
                        'url': file_url
                    }
                except Exception as e:
                    print(f"Error getting metadata for {file_url}: {str(e)}")

        return website_files

    except Exception as e:
        print(f"Error accessing website: {str(e)}")
        return {}


def get_s3_files(s3_client, bucket_name: str) -> Dict:
    try:
        s3_files = {}
        paginator = s3_client.get_paginator('list_objects_v2')

        # List all objects in the bls_data prefix
        for page in paginator.paginate(Bucket=bucket_name, Prefix='bls_data/'):
            if 'Contents' in page:
                for obj in page['Contents']:
                    # Get the filename without the prefix
                    filename = os.path.basename(obj['Key'])
                    if filename:  # Skip if it's a directory
                        s3_files[filename] = {
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'].strftime('%a, %d %b %Y %H:%M:%S GMT'),
                            'key': obj['Key']
                        }
        return s3_files

    except Exception as e:
        print(f"Error accessing S3: {str(e)}")
        return {}


def compare_files(website_files: Dict, s3_files: Dict) -> tuple:
    website_filenames = set(website_files.keys())
    s3_filenames = set(s3_files.keys())

    new_files = website_filenames - s3_filenames
    deleted_files = s3_filenames - website_filenames
    common_files = website_filenames & s3_filenames

    modified_files = {
        filename for filename in common_files
        if website_files[filename]['size'] != s3_files[filename]['size']
    }

    return new_files, deleted_files, modified_files




def main():
    # Load environment variables
    load_dotenv()
    
    # Get credentials from .env file
    aws_access_key = os.getenv('AWS_ACCESS_KEY_ID')
    aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')
    aws_region = os.getenv('AWS_REGION')
    bucket_name = os.getenv('S3_BUCKET_NAME')
    
    # Check if all required environment variables are set
    if not all([aws_access_key, aws_secret_key, aws_region, bucket_name]):
        raise EnvironmentError("Missing required environment variables")
    
    # Create S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key,
        region_name=aws_region
    )
    
    try:
        # Create sessions and clients
        session = requests.Session()
        session.headers.update({
            'User-Agent': os.getenv('USER_AGENT', 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36')
        })
        
        base_url = 'https://download.bls.gov/pub/time.series/pr/'
        
        logger.log("Fetching files from website...")
        website_files = get_website_files(base_url, session)
        
        logger.log("Fetching files from S3 bucket...")
        s3_files = get_s3_files(s3_client, bucket_name)
        
        if not website_files or not s3_files:
            logger.log("Error: Could not fetch files from either website or S3")
            return
        
        new_files, deleted_files, modified_files = compare_files(website_files, s3_files)
        
        # Log new files
        if new_files:
            logger.log("\nNew files on website (not in S3):")
            for filename in new_files:
                logger.log(f"+ {filename}")
                logger.log(f"  URL: {website_files[filename]['url']}")
                logger.log(f"  Size: {website_files[filename]['size']} bytes")
                logger.log(f"  Last Modified: {website_files[filename]['last_modified']}")
                
                try:
                    file_response = session.get(website_files[filename]['url'])
                    file_response.raise_for_status()
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=f'bls_data/{filename}',
                        Body=file_response.content
                    )
                    logger.log(f"Successfully uploaded {filename}")
                except Exception as e:
                    logger.log(f"Error uploading {filename}: {str(e)}")
        
        # Log and handle deleted files
        if deleted_files:
            logger.log("\nFiles in S3 but no longer on website:")
            for filename in deleted_files:
                logger.log(f"- {filename}")
                logger.log(f"  S3 Key: {s3_files[filename]['key']}")
                logger.log(f"  Size: {s3_files[filename]['size']} bytes")
                logger.log(f"  Last Modified: {s3_files[filename]['last_modified']}")
                
                try:
                    s3_client.delete_object(
                        Bucket=bucket_name,
                        Key=s3_files[filename]['key']
                    )
                    logger.log(f"Successfully deleted {filename} from S3")
                except Exception as e:
                    logger.log(f"Error deleting {filename}: {str(e)}")
        
        # Log modified files
        if modified_files:
            logger.log("\nModified files (different sizes):")
            for filename in modified_files:
                logger.log(f"~ {filename}")
                logger.log(f"  URL: {website_files[filename]['url']}")
                logger.log(f"  Website size: {website_files[filename]['size']} bytes")
                logger.log(f"  S3 size: {s3_files[filename]['size']} bytes")
                logger.log(f"  Website last modified: {website_files[filename]['last_modified']}")
                logger.log(f"  S3 last modified: {s3_files[filename]['last_modified']}")
                
                try:
                    file_response = session.get(website_files[filename]['url'])
                    file_response.raise_for_status()
                    s3_client.put_object(
                        Bucket=bucket_name,
                        Key=f'bls_data/{filename}',
                        Body=file_response.content
                    )
                    logger.log(f"Successfully updated {filename}")
                except Exception as e:
                    logger.log(f"Error updating {filename}: {str(e)}")
        
        if not (new_files or deleted_files or modified_files):
            logger.log("\nAll files are in sync between website and S3 bucket.")
        
    except Exception as e:
        logger.log(f"An error occurred: {str(e)}")
    
    finally:
        # Save logs to S3
        logger.save_logs()

if __name__ == "__main__":
    main()

def lambda_handler(event, context):
    return main()