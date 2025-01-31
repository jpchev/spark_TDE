import os
from pyspark.sql import SparkSession, Row

S3_ENDPOINT = os.environ['S3_ENDPOINT']
S3_ACCESS_KEY = os.environ['S3_ACCESS_KEY']
S3_SECRET_KEY = os.environ['S3_SECRET_KEY']
S3_BUCKET_NAME = os.environ['S3_BUCKET_NAME']

key = 'g+7bgI4aOujRoccZxyi5CVsWrUgkwLzWYmiYcZKW0Gk='
iv = 'PRrfBZze6v914JgV97V/IQ=='

spark = SparkSession.builder \
    .appName("S3A Encryption in PySpark") \
    .config("spark.hadoop.aes.key", key) \
    .config("spark.hadoop.aes.iv", iv) \
    .config("spark.hadoop.fs.s3a.impl", "my.custom.fs.EncryptedS3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY) \
    .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY) \
    .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT) \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
    .getOrCreate()

print(spark._jsc.hadoopConfiguration().get("fs.s3a.impl"))

# Define S3 path
s3_key = "encrypted_data"

rows = [Row(name=f"test{i}") for i in range(1, 1000000)]

df = spark.createDataFrame(rows, ["col"])
df.write.mode("overwrite") \
    .format("delta") \
    .save(f"s3a://{S3_BUCKET_NAME}/{s3_key}")
