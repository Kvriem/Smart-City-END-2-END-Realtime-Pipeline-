#!/opt/bitnami/python/bin/python3

import sys
import os

def test_s3_from_spark_container():
    """Test S3 connection from within Spark container"""
    
    print("🐳 DOCKER SPARK CONTAINER S3 TEST")
    print("="*50)
    
    # Set AWS credentials as environment variables
    os.environ['AWS_ACCESS_KEY_ID'] = 'AKIAYPZAWIVEDC37UOPJ'
    os.environ['AWS_SECRET_ACCESS_KEY'] = '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'
    
    try:
        from pyspark.sql import SparkSession
        from pyspark.sql.functions import col, lit, current_timestamp
        from pyspark.sql.types import StructType, StructField, StringType, DoubleType
        
        print("✅ PySpark imports successful")
        
        # Create Spark session with S3 configuration
        print("🚀 Creating Spark Session with S3 config...")
        spark = SparkSession.builder \
            .appName("Docker S3 Test") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.access.key", os.environ['AWS_ACCESS_KEY_ID']) \
            .config("spark.hadoop.fs.s3a.secret.key", os.environ['AWS_SECRET_ACCESS_KEY']) \
            .config("spark.hadoop.fs.s3a.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("WARN")
        print("✅ Spark Session created successfully!")
        
        # Create test data
        print("\n📊 Creating test DataFrame...")
        data = [
            ("test_1", 25.5, "Docker Test 1"),
            ("test_2", 30.2, "Docker Test 2"),
            ("test_3", 28.7, "Docker Test 3")
        ]
        
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("speed", DoubleType(), True),
            StructField("description", StringType(), True)
        ])
        
        df = spark.createDataFrame(data, schema)
        df = df.withColumn("timestamp", current_timestamp())
        
        print(f"✅ Created DataFrame with {df.count()} rows")
        df.show()
        
        # Test writing to S3
        print("\n💾 Testing write to S3...")
        s3_test_path = "s3a://spark-streaming-data-smart-city-bucket/test/docker_spark_test"
        
        df.coalesce(1).write \
            .mode("overwrite") \
            .parquet(s3_test_path)
        
        print(f"✅ Successfully wrote to: {s3_test_path}")
        
        # Test reading from S3
        print("\n📖 Testing read from S3...")
        read_df = spark.read.parquet(s3_test_path)
        print(f"✅ Successfully read {read_df.count()} rows")
        read_df.show()
        
        # Test streaming setup (without actually starting)
        print("\n🌊 Testing streaming DataFrame creation...")
        
        # Simulate the structure of your Kafka data
        vehicle_schema = StructType([
            StructField("id", StringType(), True),
            StructField("deviceId", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("speed", DoubleType(), True),
            StructField("make", StringType(), True)
        ])
        
        print("✅ Streaming schema created successfully")
        
        # Test streaming writer configuration (without starting)
        print("\n⚙️ Testing streaming writer configuration...")
        s3_streaming_path = "s3a://spark-streaming-data-smart-city-bucket/test/streaming_config_test"
        s3_checkpoint_path = "s3a://spark-streaming-data-smart-city-bucket/test/checkpoints/config_test"
        
        # Create a dummy streaming query to test configuration
        try:
            dummy_df = spark.readStream.format("rate").load()
            
            writer = dummy_df.writeStream \
                .format("parquet") \
                .option("path", s3_streaming_path) \
                .option("checkpointLocation", s3_checkpoint_path) \
                .outputMode("append")
            
            print("✅ Streaming writer configuration successful")
            print(f"✅ Output path: {s3_streaming_path}")
            print(f"✅ Checkpoint path: {s3_checkpoint_path}")
            
        except Exception as e:
            print(f"❌ Streaming configuration error: {e}")
            return False
        
        spark.stop()
        
        print("\n" + "="*50)
        print("🎉 ALL DOCKER S3 TESTS PASSED!")
        print("✅ S3 connection works from Spark container")
        print("✅ Batch read/write operations successful")
        print("✅ Streaming configuration successful")
        return True
        
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = test_s3_from_spark_container()
    
    if success:
        print("\n🎯 RESULT:")
        print("✅ S3 integration works in Docker Spark!")
        print("🔍 The original issue might be:")
        print("   1. Kafka connectivity")
        print("   2. Data schema mismatch")
        print("   3. Resource limitations")
    else:
        print("\n❌ S3 integration issues detected")
        print("🔧 Check AWS dependencies in container")
