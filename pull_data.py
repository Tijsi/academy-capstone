from pyspark import SparkConf
from pyspark.sql import SparkSession
config = {
    "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.2.0",
    "fs.s3a.aws.credentials.provider": "com.amazonaws.auth.DefaultAWSCredentialsProviderChain"
}
conf = SparkConf().setAll(config.items())
spark = SparkSession.builder.config(conf=conf).getOrCreate()

df = spark.read.json(f"s3a://dataminded-academy-capstone-resources/raw/open_aq/")
df.printSchema()
df.show()
##df.toPandas()