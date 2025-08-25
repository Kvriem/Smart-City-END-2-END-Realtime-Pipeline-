import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

def main():
    print("🚀 Starting Spark Test Job...")
    
    spark = SparkSession.builder \
        .appName("Spark City Test") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.hadoop.fs.s3a.access.key", os.environ.get("AWS_ACCESS_KEY_ID")) \
        .config("spark.hadoop.fs.s3a.secret.key", os.environ.get("AWS_SECRET_ACCESS_KEY")) \
        .config("spark.hadoop.fs.s3a.credentials.provider", 'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider') \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    print("✅ Spark Session Created")
    
    # Simple vehicle schema
    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("location", ArrayType(DoubleType()), True),
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", StringType(), True),
        StructField("fuel_type", StringType(), True),
    ])
    
    print("📡 Connecting to Kafka...")
    
    # Read from Kafka
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", 'broker:29092') \
        .option("subscribe", "vehicle_data") \
        .load() \
        .selectExpr("CAST(value AS STRING)") \
        .select(from_json(col("value"), vehicleSchema).alias("data")) \
        .select("data.*")
    
    print("✅ Kafka Connection Established")
    print("💾 Writing to S3...")
    
    # Write to S3
    query = df.writeStream \
        .format("parquet") \
        .option("checkpointLocation", "s3a://spark-streaming-data-smart-city-bucket/test-checkpoints/vehicle") \
        .option("path", "s3a://spark-streaming-data-smart-city-bucket/test-data/vehicle") \
        .outputMode("append") \
        .trigger(processingTime='10 seconds') \
        .start()
    
    print("🎯 Streaming Query Started")
    print("⏰ Running for 60 seconds...")
    
    # Run for 60 seconds then stop
    query.awaitTermination(60)
    
    print("✅ Test completed successfully!")
    spark.stop()

if __name__ == "__main__":
    main()
