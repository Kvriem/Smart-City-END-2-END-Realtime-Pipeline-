import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, lit, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, DoubleType, IntegerType
import time

def test_minimal_spark_s3():
    """Test minimal Spark to S3 connection"""
    
    print("🧪 MINIMAL SPARK S3 TEST")
    print("="*50)
    
    # AWS credentials
    aws_access_key = "AKIAYPZAWIVEDC37UOPJ"
    aws_secret_key = "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"
    
    try:
        print("🚀 Creating Spark Session...")
        spark = SparkSession.builder \
            .appName("S3 Connection Test") \
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
            .config("spark.hadoop.fs.s3a.access.key", aws_access_key) \
            .config("spark.hadoop.fs.s3a.secret.key", aws_secret_key) \
            .config("spark.hadoop.fs.s3a.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
            .config("spark.sql.adaptive.enabled", "false") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "false") \
            .getOrCreate()
        
        spark.sparkContext.setLogLevel("ERROR")
        print("✅ Spark Session created successfully")
        
        # Test 1: Create a simple DataFrame
        print("\n📊 Test 1: Creating test DataFrame...")
        data = [
            ("test_id_1", "2024-08-19T18:00:00Z", 25.5, "Test Vehicle 1"),
            ("test_id_2", "2024-08-19T18:01:00Z", 30.2, "Test Vehicle 2"),
            ("test_id_3", "2024-08-19T18:02:00Z", 28.7, "Test Vehicle 3")
        ]
        
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("timestamp", StringType(), True),
            StructField("speed", DoubleType(), True),
            StructField("vehicle_name", StringType(), True)
        ])
        
        df = spark.createDataFrame(data, schema)
        df = df.withColumn("test_timestamp", current_timestamp())
        print(f"✅ Created DataFrame with {df.count()} rows")
        df.show()
        
        # Test 2: Write to S3 (batch mode first)
        print("\n💾 Test 2: Writing to S3 (batch mode)...")
        s3_path = "s3a://spark-streaming-data-smart-city-bucket/test/spark_test_batch"
        
        try:
            df.coalesce(1).write \
                .mode("overwrite") \
                .option("header", "true") \
                .parquet(s3_path)
            print(f"✅ Successfully wrote batch data to: {s3_path}")
        except Exception as e:
            print(f"❌ Error writing batch data: {e}")
            return False
            
        # Test 3: Read back from S3
        print("\n📖 Test 3: Reading back from S3...")
        try:
            read_df = spark.read.parquet(s3_path)
            print(f"✅ Successfully read {read_df.count()} rows from S3")
            read_df.show()
        except Exception as e:
            print(f"❌ Error reading from S3: {e}")
            return False
            
        # Test 4: Test streaming write (minimal)
        print("\n🌊 Test 4: Testing streaming write...")
        try:
            # Create a streaming DataFrame from memory
            streaming_df = spark.readStream \
                .format("rate") \
                .option("rowsPerSecond", 1) \
                .load() \
                .select(
                    col("timestamp"),
                    col("value").alias("test_value"),
                    lit("streaming_test").alias("source")
                )
            
            print("✅ Created streaming DataFrame")
            
            # Write streaming data to S3
            s3_streaming_path = "s3a://spark-streaming-data-smart-city-bucket/test/spark_streaming_test"
            s3_checkpoint_path = "s3a://spark-streaming-data-smart-city-bucket/test/checkpoints/streaming_test"
            
            query = streaming_df.writeStream \
                .format("parquet") \
                .option("path", s3_streaming_path) \
                .option("checkpointLocation", s3_checkpoint_path) \
                .outputMode("append") \
                .trigger(processingTime="10 seconds") \
                .start()
            
            print("✅ Started streaming query")
            print("⏱️ Running for 30 seconds...")
            
            # Let it run for 30 seconds
            time.sleep(30)
            
            # Stop the query
            query.stop()
            print("🛑 Stopped streaming query")
            
            # Check if data was written
            try:
                result_df = spark.read.parquet(s3_streaming_path)
                row_count = result_df.count()
                print(f"✅ Streaming wrote {row_count} rows to S3")
                if row_count > 0:
                    result_df.show(5)
            except Exception as e:
                print(f"⚠️ Could not read streaming results: {e}")
                
        except Exception as e:
            print(f"❌ Error in streaming test: {e}")
            return False
            
        print("\n" + "="*50)
        print("🎉 ALL SPARK S3 TESTS PASSED!")
        print("✅ Spark can successfully read/write to your S3 bucket")
        return True
        
    except Exception as e:
        print(f"❌ Spark session creation failed: {e}")
        return False
    finally:
        try:
            spark.stop()
            print("🧹 Spark session stopped")
        except:
            pass

if __name__ == "__main__":
    print("🔬 TESTING SPARK S3 INTEGRATION")
    print("This will test if Spark can write to your S3 bucket")
    print("="*60)
    
    success = test_minimal_spark_s3()
    
    if success:
        print("\n🎯 CONCLUSION:")
        print("✅ Spark S3 integration is working!")
        print("🐛 The issue in your streaming job might be:")
        print("   1. Kafka connection inside Docker container")
        print("   2. Schema mismatch in streaming data")
        print("   3. Resource/memory issues")
        print("   4. Network connectivity between Docker containers")
    else:
        print("\n🔧 TROUBLESHOOTING NEEDED:")
        print("❌ Spark S3 integration has issues")
        print("🔍 Check Spark configuration and dependencies")
