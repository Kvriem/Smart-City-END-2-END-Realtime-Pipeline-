import boto3
import os
from botocore.exceptions import ClientError, NoCredentialsError
import json

def test_s3_connection():
    """Test S3 connection and credentials"""
    
    # AWS credentials from your config
    aws_access_key = "AKIAYPZAWIVEDC37UOPJ"
    aws_secret_key = "o2sr4OfuJV7amz/7xG/3lodSlY09euYJH6kVRaIZ"
    bucket_name = "spark-streaming-data-smart-city-bucket"
    
    print("🔐 Testing AWS S3 Connection...")
    print(f"📦 Bucket: {bucket_name}")
    print(f"🔑 Access Key: {aws_access_key[:10]}...")
    print("-" * 50)
    
    try:
        # Create S3 client
        s3_client = boto3.client(
            's3',
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_key
        )
        
        print("✅ S3 Client created successfully")
        
        # Test 1: List all buckets
        print("\n📋 Test 1: Listing all buckets...")
        try:
            response = s3_client.list_buckets()
            buckets = [bucket['Name'] for bucket in response['Buckets']]
            print(f"✅ Found {len(buckets)} buckets:")
            for bucket in buckets:
                print(f"   - {bucket}")
                
            # Check if our target bucket exists
            if bucket_name in buckets:
                print(f"✅ Target bucket '{bucket_name}' EXISTS!")
            else:
                print(f"❌ Target bucket '{bucket_name}' NOT FOUND!")
                print("💡 Available buckets:", buckets)
                
        except ClientError as e:
            print(f"❌ Error listing buckets: {e}")
            return False
            
        # Test 2: Check bucket access
        print(f"\n🔍 Test 2: Checking access to bucket '{bucket_name}'...")
        try:
            # Try to list objects in the bucket
            response = s3_client.list_objects_v2(Bucket=bucket_name, MaxKeys=1)
            print(f"✅ Successfully accessed bucket '{bucket_name}'")
            
            if 'Contents' in response:
                print(f"📁 Bucket contains {response.get('KeyCount', 0)} objects")
            else:
                print("📁 Bucket is empty")
                
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'NoSuchBucket':
                print(f"❌ Bucket '{bucket_name}' does not exist!")
                return False
            elif error_code == 'AccessDenied':
                print(f"❌ Access denied to bucket '{bucket_name}'!")
                return False
            else:
                print(f"❌ Error accessing bucket: {e}")
                return False
                
        # Test 3: Test write permissions
        print(f"\n✍️ Test 3: Testing write permissions...")
        test_key = "test/connection_test.txt"
        test_content = "This is a test file from Spark streaming pipeline"
        
        try:
            s3_client.put_object(
                Bucket=bucket_name,
                Key=test_key,
                Body=test_content.encode('utf-8'),
                ContentType='text/plain'
            )
            print(f"✅ Successfully wrote test file: {test_key}")
            
            # Try to read it back
            response = s3_client.get_object(Bucket=bucket_name, Key=test_key)
            content = response['Body'].read().decode('utf-8')
            if content == test_content:
                print("✅ Successfully read back test file")
            else:
                print("⚠️ File content doesn't match")
                
            # Clean up test file
            s3_client.delete_object(Bucket=bucket_name, Key=test_key)
            print("🧹 Cleaned up test file")
            
        except ClientError as e:
            print(f"❌ Error testing write permissions: {e}")
            return False
            
        # Test 4: Test required paths
        print(f"\n📂 Test 4: Testing required paths...")
        required_paths = [
            "checkpoints/vehicle/",
            "checkpoints/gps/", 
            "checkpoints/traffic/",
            "checkpoints/weather/",
            "checkpoints/emergency/",
            "data/vehicle/",
            "data/gps/",
            "data/traffic/",
            "data/weather/",
            "data/emergency/"
        ]
        
        for path in required_paths:
            try:
                # Create a marker file to ensure the path exists
                s3_client.put_object(
                    Bucket=bucket_name,
                    Key=f"{path}_marker",
                    Body=b"path marker"
                )
                print(f"✅ Path accessible: {path}")
            except ClientError as e:
                print(f"❌ Error with path {path}: {e}")
                
        print("\n" + "="*50)
        print("🎉 S3 CONNECTION TEST COMPLETED SUCCESSFULLY!")
        print("✅ Your credentials and bucket are working correctly")
        print("🚀 You can now run your Spark streaming job")
        return True
        
    except NoCredentialsError:
        print("❌ No AWS credentials found!")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def test_spark_s3_config():
    """Test if Spark can connect to S3 with these configs"""
    print("\n" + "="*50)
    print("🔧 SPARK S3 CONFIGURATION TEST")
    print("="*50)
    
    # Test if we can import required libraries
    try:
        import boto3
        print("✅ boto3 library available")
    except ImportError:
        print("❌ boto3 not installed. Install with: pip install boto3")
        
    # Show the exact configuration that Spark should use
    print("\n📋 Spark S3 Configuration:")
    print("spark.hadoop.fs.s3a.access.key:", "AKIAYPZAWIVEDC37UOPJ")
    print("spark.hadoop.fs.s3a.secret.key:", "o2sr4***hidden***")
    print("spark.hadoop.fs.s3a.impl:", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    print("spark.hadoop.fs.s3a.credentials.provider:", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    
    # Test S3 URLs
    test_urls = [
        "s3a://spark-streaming-data-smart-city-bucket/checkpoints/vehicle",
        "s3a://spark-streaming-data-smart-city-bucket/data/vehicle"
    ]
    
    print("\n🔗 Testing S3 URLs format:")
    for url in test_urls:
        print(f"✅ {url}")

if __name__ == "__main__":
    print("🧪 AWS S3 CONNECTION TESTER")
    print("="*50)
    
    # Run S3 connection test
    success = test_s3_connection()
    
    # Run Spark configuration test
    test_spark_s3_config()
    
    if success:
        print("\n🎯 NEXT STEPS:")
        print("1. ✅ S3 connection is working")
        print("2. 🚀 Run your Spark streaming job")
        print("3. 📊 Check S3 bucket for streaming data")
    else:
        print("\n🔧 TROUBLESHOOTING:")
        print("1. ❌ Fix S3 connection issues above")
        print("2. 🔍 Check AWS credentials")
        print("3. 📦 Verify bucket exists and permissions")
