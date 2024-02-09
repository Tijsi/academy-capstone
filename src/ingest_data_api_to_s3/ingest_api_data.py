import requests
import boto3
import json
import logging
import os

DATE_FROM = os.getenv('DATE_FROM')
DATE_TO = os.getenv('DATE_TO')
S3_BUCKET = 'dataminded-academy-capstone-resources'

def get_data_from_Api():
    """Get data from OpenAQ API"""
    # Init params
    url = 'https://api.openaq.org/v2/measurements'
    params = {
        'date_from': f"{DATE_FROM}T00:00:00Z",
        'date_from': f"{DATE_TO}T00:00:00Z",
        'limit': 10000,
        'page': 1,
        'offset': 0,
        'sort': 'desc',
        'radius': 1000,
        'country': 'BE',
        'order_by': 'datetime'
    }
    # Query API
    response = requests.get(url, params=params, headers={'accept': 'application/json'})
    # Check if the request was successful
    if response.status_code == 200:
        # Extract the JSON content
        return response.json()
        logging.info(f"Got response {response.status_code}")
    else:
        logging.error(f"Got response {response.status_code}")
        return None 

def upload_to_s3(data, bucket_name):
    """Upload a file to an S3 bucket"""
    # Convert the JSON data to a string and encode it to bytes
    json_string = json.dumps(data)
    json_bytes = json_string.encode()
    # Init s3
    s3 = boto3.resource('s3')
    try:
        object_name = f"Tijs/ingest/openaq_be_{DATE_FROM}_to_{DATE_TO}.json"
        # Upload the bytes to S3
        s3.Bucket(bucket_name).put_object(Key=object_name, Body=json_bytes)
        logging.info(f"File uploaded to {bucket_name}/{object_name}")
        return True
    except Exception as e:
        print(f"Upload failed: {e}")
        return False

def main():
    # Get openAQ data
    logging.info("Requesting data with openAQ API...")
    res = get_data_from_Api()
    if res:
        # Upload to S3
        logging.info('Upload to S3...')
        upload_to_s3(res, S3_BUCKET)
    else:
        logging.info("No data were processed with this request.")

if __name__ == "__main__":
    main()
