import boto3
from datetime import datetime

def check_test_data():
    """Check the test data directory created by Spark test job"""
    
    # AWS credentials
    aws_access_key = "AKIAYPZAWIVEDC37UOPJ"
    aws_secret_key = "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"
    bucket_name = "spark-streaming-data-smart-city-bucket"
    
    # Create S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )
    
    print("🔍 CHECKING TEST DATA FROM SPARK JOB")
    print("=" * 50)
    
    # Check test directories
    test_paths = [
        "test-data/vehicle/",
        "test-checkpoints/vehicle/"
    ]
    
    for path in test_paths:
        try:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=path,
                MaxKeys=100
            )
            
            if 'Contents' in response:
                count = len(response['Contents'])
                total_size = sum(obj['Size'] for obj in response['Contents'])
                latest_modified = max(obj['LastModified'] for obj in response['Contents'])
                
                print(f"✅ {path:<30} | Files: {count:>3} | Size: {total_size:>8} bytes | Latest: {latest_modified}")
                
                # Show all files
                print(f"    📁 Files:")
                for obj in response['Contents']:
                    print(f"      - {obj['Key']} ({obj['Size']} bytes) - {obj['LastModified']}")
                
            else:
                print(f"❌ {path:<30} | No files found")
                
        except Exception as e:
            print(f"❌ {path:<30} | Error: {e}")
    
    # Check all prefixes in bucket
    print(f"\n📂 ALL DIRECTORIES IN BUCKET:")
    try:
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Delimiter='/'
        )
        
        if 'CommonPrefixes' in response:
            for prefix in response['CommonPrefixes']:
                print(f"   📁 {prefix['Prefix']}")
        
        if 'Contents' in response:
            print(f"   📄 Root files:")
            for obj in response['Contents']:
                print(f"      - {obj['Key']}")
                
    except Exception as e:
        print(f"❌ Error listing directories: {e}")

if __name__ == "__main__":
    check_test_data()
