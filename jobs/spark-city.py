import os
import json
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import from_json, col, to_json, struct
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
        .appName("Smart City Dual Stream") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")\
        .config("spark.hadoop.fs.s3a.access.key", configurations["AWS_ACCESS_KEY_ID"]) \
        .config("spark.hadoop.fs.s3a.secret.key", configurations["AWS_SECRET_ACCESS_KEY"]) \
        .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()
    
    # Adjust the log level to minimize console output on executor 
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
        StructField("device_id", StringType(), True),
        StructField("camera_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("snapshot", StringType(), True),
        StructField("location", ArrayType(DoubleType()), True),  # Array of [latitude, longitude]
        StructField("direction", StringType(), True),
        StructField("speed", DoubleType(), True),
    ])

    weatherSchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("location", ArrayType(DoubleType()), True),  # Array of [latitude, longitude]
        StructField("temperature", DoubleType(), True),
        StructField("weather_condition", StringType(), True),
        StructField("humidity", DoubleType(), True),
        StructField("wind_speed", DoubleType(), True),
        StructField("precipitation", DoubleType(), True),
        StructField("airQualityIndex", DoubleType(), True),
    ])

    emergencySchema = StructType([
        StructField("id", StringType(), True),
        StructField("device_id", StringType(), True),
        StructField("incidentId", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("location", ArrayType(DoubleType()), True),  # Array of [latitude, longitude]
        StructField("incident_type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("description", StringType(), True),
    ])

    def read_from_kafka(topic, schema):
        return (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", 'broker:29092')
                .option("subscribe", topic)
                .option("failOnDataLoss", "false")  # For development - handle topic resets
                .option("startingOffsets", "latest")  # Start from latest data
                .load()
                .selectExpr("CAST(value AS STRING)")
                .select(from_json(col("value"), schema).alias("data"))
                .select("data.*"))

    def write_to_redis_batch(df, epoch_id):
        """Write batch to Redis with TTL for real-time layer"""
        try:
            import redis
            r = redis.Redis(host='redis', port=6379, decode_responses=True)
            
            # Test connection first
            r.ping()
            
            # Convert to list for processing
            rows = df.collect()
            
            for row in rows:
                data = row.asDict()
                device_id = data.get('device_id', 'unknown')
                
                # Convert any complex objects to strings for JSON serialization
                for key, value in data.items():
                    if isinstance(value, (list, tuple)):
                        data[key] = list(value) if value else []
                
                # Store with 1 hour TTL for general real-time access
                topic_key = f"realtime:{device_id}"
                r.setex(topic_key, 3600, json.dumps(data, default=str))
                
                # Store in type-specific keys for targeted queries
                if 'make' in data and 'model' in data:  # Vehicle data
                    r.setex(f"vehicles:{device_id}", 3600, json.dumps(data, default=str))
                elif 'vehicle_type' in data:  # GPS data
                    r.setex(f"gps:{device_id}", 1800, json.dumps(data, default=str))  # 30min TTL
                elif 'incident_type' in data:  # Emergency data
                    if data.get('status') == 'Active':
                        r.setex(f"emergencies:active:{device_id}", 7200, json.dumps(data, default=str))  # 2hr TTL
                elif 'weather_condition' in data:  # Weather data
                    location_key = f"weather:{data.get('location', [0,0])[0]}_{data.get('location', [0,0])[1]}"
                    r.setex(location_key, 1800, json.dumps(data, default=str))
                elif 'camera_id' in data:  # Traffic camera data
                    r.setex(f"traffic:{data['camera_id']}", 600, json.dumps(data, default=str))  # 10min TTL
                        
            print(f"✅ Batch {epoch_id}: Wrote {len(rows)} records to Redis")
            
        except ImportError:
            print(f"⚠️ Batch {epoch_id}: Redis module not available - skipping Redis writes")
        except Exception as e:
            print(f"⚠️ Batch {epoch_id}: Redis unavailable ({e}) - continuing with S3 only")

    def dual_stream_writer(input_df: DataFrame, checkpoint_folder, s3_output, redis_enabled=True):
        """Write to both S3 (batch layer) and Redis (speed layer)"""
        
        # S3 Stream (batch layer) - for historical analytics
        s3_query = (input_df.writeStream
                   .format("parquet")
                   .option("checkpointLocation", checkpoint_folder)
                   .option("path", s3_output)
                   .outputMode("append")
                   .start())
        
        # Redis Stream (speed layer) - for real-time access
        redis_query = None
        if redis_enabled:
            try:
                redis_query = (input_df.writeStream
                              .foreachBatch(write_to_redis_batch)
                              .option("checkpointLocation", f"{checkpoint_folder}_redis")
                              .outputMode("update")
                              .start())
                print(f"✅ Started Redis stream for {checkpoint_folder}")
            except Exception as e:
                print(f"⚠️ Redis stream disabled for {checkpoint_folder}: {e}")
        
        return s3_query, redis_query

    # Create DataFrames
    vehicleDF = read_from_kafka("vehicle_data", vehicleSchema).alias("vehicle")
    gpsDF = read_from_kafka("gps_data", gpsSchema).alias("gps")
    trafficDF = read_from_kafka("traffic_camera_data", trafficSchema).alias("traffic")
    weatherDF = read_from_kafka("weather_data", weatherSchema).alias("weather")
    emergencyDF = read_from_kafka("emergency_incident_data", emergencySchema).alias("emergency")

    print("🚀 Starting Smart City Dual-Stream Processing...")
    print("📊 Batch Layer: Data → S3 (Historical Analytics)")
    print("⚡ Speed Layer: Data → Redis (Real-time Access)")

    # Start dual streams for each data type
    vehicle_s3, vehicle_redis = dual_stream_writer(
        vehicleDF, 
        "s3a://spark-streaming-data-smart-city-bucket/checkpoints/vehicle",
        "s3a://spark-streaming-data-smart-city-bucket/data/vehicle"
    )
    
    gps_s3, gps_redis = dual_stream_writer(
        gpsDF,
        "s3a://spark-streaming-data-smart-city-bucket/checkpoints/gps", 
        "s3a://spark-streaming-data-smart-city-bucket/data/gps"
    )
    
    traffic_s3, traffic_redis = dual_stream_writer(
        trafficDF,
        "s3a://spark-streaming-data-smart-city-bucket/checkpoints/traffic",
        "s3a://spark-streaming-data-smart-city-bucket/data/traffic" 
    )
    
    weather_s3, weather_redis = dual_stream_writer(
        weatherDF,
        "s3a://spark-streaming-data-smart-city-bucket/checkpoints/weather",
        "s3a://spark-streaming-data-smart-city-bucket/data/weather"
    )
    
    emergency_s3, emergency_redis = dual_stream_writer(
        emergencyDF,
        "s3a://spark-streaming-data-smart-city-bucket/checkpoints/emergency",
        "s3a://spark-streaming-data-smart-city-bucket/data/emergency"
    )
    
    print("🔄 All streams started successfully!")
    print("💡 Use Ctrl+C to stop processing")
    
    # Wait for termination
    try:
        vehicle_s3.awaitTermination()
    except KeyboardInterrupt:
        print("\n🛑 Stopping Smart City streams...")
        print("✅ Shutdown complete")

if __name__ == "__main__":
    main()