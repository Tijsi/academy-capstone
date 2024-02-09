import boto3
import json
import logging
import os
import polars as pl
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import s3fs

SFSCHEMA = 'TIJS'

def read_data():
    """Read json files from s3 bucket and store in df"""
    # Read data
    # Create an S3 FileSystem object
    fs = s3fs.S3FileSystem(anon=False)  # Use anon=False if you need authentication
    # Use s3fs to open the file as a file-like object
    with fs.open('s3a://dataminded-academy-capstone-resources/raw/open_aq/', 'r') as f:
        df = pl.read_json(f)
    return df

def clean_data(df):
    """Clean df columns and rows"""
    # Flatten nested values
    df = df.with_column(pl.col("date.utc").str.strptime(pl.Datetime, "%Y-%m-%dT%H:%M:%S").alias("datetimeUtc"))
    # Extract "latitude" and "longitude" from the nested "coordinates" structure
    df = df.with_columns([
        pl.col("coordinates.latitude").cast(pl.Float64).alias("latitude"),
        pl.col("coordinates.longitude").cast(pl.Float64).alias("longitude")
    ])    
    # Drop columns
    df = df.drop(["date", "coordinates", "city", "country"])
    # Cast datatype
    df = df.with_columns([
        pl.col("value").cast(pl.Float64),
        pl.col("isMobile").cast(pl.Boolean),
        pl.col("isAnalysis").cast(pl.Boolean)
    ])    
    return df

def get_secret():
    """Get the Snowflake secret from the secret manager"""
    # Specify your secret name and AWS region
    secret_name = "snowflake/capstone/config"
    region_name = "eu-west-1"
    # Initialize a session using Amazon Secrets Manager
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=region_name
    )
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
    except Exception as e:
        raise e
    else:
        if 'SecretString' in get_secret_value_response:
            secret = get_secret_value_response['SecretString']
        else:
            secret = base64.b64decode(get_secret_value_response['SecretBinary'])
    # Assuming the secret is a JSON string and you want to parse it
    secret_dict = json.loads(secret)
    return secret_dict

def upload_to_snowflake(secret_dict, df):
    """Upload to snowflake"""
    # Snowflake connection parameters
    conn_info = {
        'user': secret_dict["USER_NAME"],
        'password': secret_dict["PASSWORD"],
        'warehouse': secret_dict["WAREHOUSE"],
        'database': secret_dict["DATABASE"],
        'schema': SFSCHEMA,
        'role': secret_dict["ROLE"]
    }
    # Establish connection
    conn = snowflake.connector.connect(**conn_info)
    # Convert Polars DataFrame to Pandas for compatibility
    df_pandas = df.to_pandas()
    # Use the write_pandas function to write the DataFrame to Snowflake
    write_pandas(conn, df_pandas, 'airquality', database=conn_info['database'], schema=conn_info['schema'], chunk_size=16000, method='COPY', overwrite=True)
    # Close the connection
    conn.close()

def main():
    # Load json
    logging.info('Reading data...')
    df = read_data()
    # Clean df
    logging.info('Cleaning data...')
    df = clean_data(df)
    # Get Snowflake secret
    logging.info('Get secrets...')
    secret_dict = get_secret()
    # Upload to Snowflake
    logging.info('Upload to Snowflake...')
    upload_to_snowflake(secret_dict, df)

if __name__ == "__main__":
    main()
