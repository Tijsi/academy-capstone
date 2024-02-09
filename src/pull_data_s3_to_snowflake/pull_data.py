from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark.sql import functions as sf
import boto3
import json
import logging
import os

SFSCHEMA = 'TIJS'
spark = None

# Show logging in stdout as print statements
# logging.basicConfig(
#     level=logging.INFO,
#     format='%(asctime)s | %(levelname)s | %(message)s'
# )
# logging.getLogger().addHandler(logging.StreamHandler())

def init_spark():
    """Initialize Spark"""
    config = {
        "spark.jars.packages": "net.snowflake:spark-snowflake_2.12:2.9.0-spark_3.1,net.snowflake:snowflake-jdbc:3.13.3,org.apache.hadoop:hadoop-aws:3.2.0",
        "spark.hadoop.fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
    }
    conf = SparkConf().setAll(config.items())
    global spark
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

def read_data():
    """Read json files from s3 bucket and store in df"""
    # Read data
    return spark.read.json(f"s3a://dataminded-academy-capstone-resources/raw/open_aq/")
    ## DEV data inspection functions
    # df.printSchema()
    # df.show()
    # df.toPandas()

def clean_data(df):
    """Clean df columns and rows"""
    # Flatten nested values
    df = df.withColumn("datetimeUtc", sf.to_utc_timestamp("date.utc", "Europe/Brussels"))
    df = df.withColumn("latitude", sf.col("coordinates.latitude").cast('double'))
    df = df.withColumn("longitude", sf.col("coordinates.longitude").cast('double'))
    # Drop cols
    df = df.drop("date")
    df = df.drop("coordinates")
    df = df.drop("city")
    df = df.drop("country")
    # Cast datatype
    df = df.withColumn("value", sf.col("value").cast('double'))
    df = df.withColumn("isMobile", sf.col("isMobile").cast('boolean'))
    df = df.withColumn("isAnalysis", sf.col("isAnalysis").cast('boolean'))
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
    # Snowflake source options
    sfOptions = {
        "sfURL": secret_dict["URL"],
        # "sfAccount": secret_dict["sfAccount"],
        "sfUser": secret_dict["USER_NAME"],
        "sfPassword": secret_dict["PASSWORD"],
        "sfDatabase": secret_dict["DATABASE"],
        "sfSchema": SFSCHEMA,
        "sfWarehouse": secret_dict["WAREHOUSE"],
        "sfRole": secret_dict["ROLE"]
    }
    df.write.format("snowflake").options(**sfOptions).option("dbtable", "airquality").mode("overwrite").save()

def main():
    # Init Spark
    init_spark()
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
