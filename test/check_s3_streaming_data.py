import boto3
import time
from datetime import datetime

def check_s3_streaming_data():
    """Monitor S3 bucket for streaming data"""
    
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
    
    print("🔍 MONITORING S3 BUCKET FOR STREAMING DATA")
    print("=" * 50)
    print(f"📦 Bucket: {bucket_name}")
    print(f"⏰ Time: {datetime.now()}")
    print("-" * 50)
    
    # Data paths to monitor
    data_paths = [
        "data/vehicle/",
        "data/gps/", 
        "data/traffic/",
        "data/weather/",
        "data/emergency/"
    ]
    
    # Checkpoint paths to monitor
    checkpoint_paths = [
        "checkpoints/vehicle/",
        "checkpoints/gps/",
        "checkpoints/traffic/", 
        "checkpoints/weather/",
        "checkpoints/emergency/"
    ]
    
    print("📊 DATA DIRECTORIES:")
    for path in data_paths:
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
                
                print(f"✅ {path:<20} | Files: {count:>3} | Size: {total_size:>8} bytes | Latest: {latest_modified}")
                
                # Show some files if they exist
                if count > 0:
                    print(f"    📁 Sample files:")
                    for obj in response['Contents'][:3]:  # Show first 3 files
                        print(f"      - {obj['Key']} ({obj['Size']} bytes)")
                
            else:
                print(f"❌ {path:<20} | No files found")
                
        except Exception as e:
            print(f"❌ {path:<20} | Error: {e}")
    
    print("\n🔧 CHECKPOINT DIRECTORIES:")
    for path in checkpoint_paths:
        try:
            response = s3_client.list_objects_v2(
                Bucket=bucket_name,
                Prefix=path,
                MaxKeys=10
            )
            
            if 'Contents' in response:
                count = len(response['Contents'])
                print(f"✅ {path:<25} | Files: {count:>3}")
            else:
                print(f"❌ {path:<25} | No checkpoint files")
                
        except Exception as e:
            print(f"❌ {path:<25} | Error: {e}")
    
    print("\n" + "=" * 50)
    
    # Check for recent activity
    print("🕐 RECENT ACTIVITY (last 5 minutes):")
    try:
        five_minutes_ago = datetime.now().timestamp() - 300  # 5 minutes ago
        
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix="data/",
            MaxKeys=1000
        )
        
        recent_files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['LastModified'].timestamp() > five_minutes_ago:
                    recent_files.append(obj)
        
        if recent_files:
            print(f"✅ Found {len(recent_files)} files created in the last 5 minutes!")
            for obj in recent_files[:5]:  # Show first 5
                print(f"   📄 {obj['Key']} - {obj['LastModified']}")
        else:
            print("❌ No recent files found. Spark streaming might not be working.")
            
    except Exception as e:
        print(f"❌ Error checking recent activity: {e}")
    
    print("\n🎯 SUMMARY:")
    try:
        # Count total data files
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix="data/",
            MaxKeys=1000
        )
        
        total_data_files = len(response.get('Contents', []))
        total_data_size = sum(obj['Size'] for obj in response.get('Contents', []))
        
        print(f"📊 Total data files: {total_data_files}")
        print(f"💾 Total data size: {total_data_size} bytes")
        
        if total_data_files > 0:
            print("🎉 SUCCESS: Data is being written to S3!")
        else:
            print("⚠️  WARNING: No data files found in S3")
            
    except Exception as e:
        print(f"❌ Error getting summary: {e}")

if __name__ == "__main__":
    check_s3_streaming_data()
