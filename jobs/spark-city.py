import os
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType,
    DoubleType, IntegerType, ArrayType
)
from config import configurations

def main():
    # Set AWS credentials from config
    os.environ["AWS_ACCESS_KEY_ID"] = configurations["AWS_ACCESS_KEY_ID"]
    os.environ["AWS_SECRET_ACCESS_KEY"] = configurations["AWS_SECRET_ACCESS_KEY"]
    
    spark = SparkSession.builder \
        .appName("Spark City") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.access.key", configurations["AWS_ACCESS_KEY_ID"]) \
        .config("spark.hadoop.fs.s3a.secret.key", configurations["AWS_SECRET_ACCESS_KEY"]) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
    # adjust the log level to minimizing the console output on executor 
    spark.sparkContext.setLogLevel("WARN")



    vehicleSchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("location", ArrayType(DoubleType()), True),  # Array of [latitude, longitude]
        StructField("speed", DoubleType(), True),
        StructField("direction", StringType(), True),
        StructField("make", StringType(), True),
        StructField("model", StringType(), True),
        StructField("year", IntegerType(), True),
        StructField("fuel_type", StringType(), True),
    ])

    gpsSchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("location", ArrayType(DoubleType()), True),  # Array of [latitude, longitude]
        StructField("direction", StringType(), True),
        StructField("vehicle_type", StringType(), True),
    ])

    trafficSchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),  # Fixed field name
        StructField("camera_id", StringType(), True),  # Fixed field name
        StructField("timestamp", StringType(), True),
        StructField("snapshot", StringType(), True),
        StructField("location", ArrayType(DoubleType()), True),  # Array of [latitude, longitude]
        StructField("direction", StringType(), True),  # Added missing field
        StructField("speed", DoubleType(), True),  # Added missing field
    ])

    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),  # Fixed field name
        StructField("timestamp", StringType(), True),
        StructField("location", ArrayType(DoubleType()), True),  # Array of [latitude, longitude]
        StructField("temperature", DoubleType(), True),
        StructField("weather_condition", StringType(), True),  # Fixed field name
        StructField("humidity", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),  # Fixed field name
        StructField("precipitation", DoubleType(), True),
        StructField("airQualityIndex", DoubleType(), True),
    ])

    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),  # Fixed field name
        StructField("incidentId", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("location", ArrayType(DoubleType()), True),  # Array of [latitude, longitude]
        StructField("incident_type", StringType(), True),  # Fixed field name
        StructField("status", StringType(), True),
        StructField("description", StringType(), True),
    ])

    def read_from_kafka (topic, schema):
        return(spark.readStream
            .format("kafka")
            .option("kafka.bootstrap.servers", 'broker:29092')
            .option("subscribe", topic)
            .load()
            .selectExpr("CAST(value AS STRING)")
            .select(from_json(col("value"), schema).alias("data"))
            .select("data.*")
        )


    def streamWriter(input_df: DataFrame, checkpointFolder, output):
        return(input_df.writeStream
            .format("parquet")
            .option("checkpointLocation", checkpointFolder)
            .option("path", output)
            .outputMode("append")
            .start()
        )
    
    vehicleDF = read_from_kafka("vehicle_data", vehicleSchema).alias("vehicle")
    gpsDF = read_from_kafka("gps_data", gpsSchema).alias("gps")
    trafficDF = read_from_kafka("traffic_camera_data", trafficSchema).alias("traffic")
    weatherDF = read_from_kafka("weather_data", weatherSchema).alias("weather")
    emergencyDF = read_from_kafka("emergency_incident_data", emergencySchema).alias("emergency")

    #join the dataframes using timestamp and ids
    query1 = streamWriter(vehicleDF,"s3a://spark-streaming-data-smart-city-bucket/checkpoints/vehicle", "s3a://spark-streaming-data-smart-city-bucket/data/vehicle")
    query2 = streamWriter(gpsDF,"s3a://spark-streaming-data-smart-city-bucket/checkpoints/gps", "s3a://spark-streaming-data-smart-city-bucket/data/gps")
    query3 = streamWriter(trafficDF,"s3a://spark-streaming-data-smart-city-bucket/checkpoints/traffic", "s3a://spark-streaming-data-smart-city-bucket/data/traffic")
    query4 = streamWriter(weatherDF,"s3a://spark-streaming-data-smart-city-bucket/checkpoints/weather", "s3a://spark-streaming-data-smart-city-bucket/data/weather")
    query5 = streamWriter(emergencyDF,"s3a://spark-streaming-data-smart-city-bucket/checkpoints/emergency", "s3a://spark-streaming-data-smart-city-bucket/data/emergency")

    query5.awaitTermination()

if __name__ == "__main__":
    main()