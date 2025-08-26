from datetime import datetime, timedelta
import os
import logging
import pandas as pd
import boto3
import io
from typing import List, Dict, Any

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from airflow.models import Variable
from airflow.utils.dates import days_ago

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

# DAG Configuration
DAG_ID = 's3_to_snowflake_etl'
SCHEDULE_INTERVAL = '@daily'  # Run daily at midnight
CATCHUP = False

# S3 Configuration  
S3_BUCKET = 'spark-streaming-data-smart-city-bucket'
S3_REGION = 'eu-north-1'
DATA_TYPES = ['vehicle', 'gps', 'traffic', 'weather', 'emergency']

# Snowflake Configuration
SNOWFLAKE_CONFIG = {
    'account': os.environ.get('SNOWFLAKE_ACCOUNT'),
    'user': os.environ.get('SNOWFLAKE_USER'), 
    'password': os.environ.get('SNOWFLAKE_PASSWORD'),
    'database': os.environ.get('SNOWFLAKE_DATABASE', 'SMART_CITY_DW'),
    'schema': os.environ.get('SNOWFLAKE_SCHEMA', 'RAW_DATA'),
    'warehouse': os.environ.get('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH')
}

# Default DAG arguments
default_args = {
    'owner': 'smart-city-analytics',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'catchup': False
}

def get_s3_client():
    """Create and return S3 client"""
    return boto3.client(
        's3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name=S3_REGION
    )

def get_snowflake_connection():
    """Create and return Snowflake connection"""
    return snowflake.connector.connect(**SNOWFLAKE_CONFIG)

def discover_s3_parquet_files(data_type: str) -> List[str]:
    """
    Discover all parquet files for a given data type in S3
    Filters out metadata and marker files
    """
    logging.info(f"🔍 Discovering parquet files for {data_type}")
    
    s3_client = get_s3_client()
    parquet_files = []
    
    try:
        # Use paginator to handle large number of files
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(
            Bucket=S3_BUCKET,
            Prefix=f'data/{data_type}/'
        )
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    key = obj['Key']
                    # Filter for parquet files, exclude metadata and markers
                    if (key.endswith('.parquet') and 
                        '_marker' not in key and 
                        '_spark_metadata' not in key):
                        parquet_files.append(key)
        
        logging.info(f"✅ Found {len(parquet_files)} parquet files for {data_type}")
        return parquet_files
        
    except Exception as e:
        logging.error(f"❌ Error discovering files for {data_type}: {str(e)}")
        raise

def extract_and_transform_data(data_type: str) -> pd.DataFrame:
    """
    Extract data from S3 parquet files and perform basic transformations
    """
    logging.info(f"📥 Extracting and transforming {data_type} data")
    
    s3_client = get_s3_client()
    parquet_files = discover_s3_parquet_files(data_type)
    
    if not parquet_files:
        logging.warning(f"⚠️ No parquet files found for {data_type}")
        return pd.DataFrame()
    
    all_dataframes = []
    
    for file_key in parquet_files:
        try:
            logging.info(f"📄 Processing {file_key}")
            
            # Download parquet file
            response = s3_client.get_object(Bucket=S3_BUCKET, Key=file_key)
            parquet_data = response['Body'].read()
            
            # Read parquet file
            df = pd.read_parquet(io.BytesIO(parquet_data))
            
            if not df.empty:
                # Basic data cleaning
                df = clean_dataframe(df, data_type)
                
                all_dataframes.append(df)
                
        except Exception as e:
            logging.error(f"❌ Error processing {file_key}: {str(e)}")
            continue
    
    if all_dataframes:
        # Combine all dataframes
        combined_df = pd.concat(all_dataframes, ignore_index=True)
        logging.info(f"✅ Combined {len(all_dataframes)} files into {len(combined_df)} rows for {data_type}")
        return combined_df
    else:
        logging.warning(f"⚠️ No data extracted for {data_type}")
        return pd.DataFrame()

def clean_dataframe(df: pd.DataFrame, data_type: str) -> pd.DataFrame:
    """
    Perform data type specific cleaning and standardization
    """
    try:
        # Common cleaning for all data types
        
        # Standardize timestamp column
        if 'timestamp' in df.columns:
            df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
        
        # Remove duplicates based on id if present
        if 'id' in df.columns:
            df = df.drop_duplicates(subset=['id'], keep='first')
        
        # Data type specific cleaning
        if data_type == 'vehicle':
            # Ensure speed is numeric
            if 'speed' in df.columns:
                df['speed'] = pd.to_numeric(df['speed'], errors='coerce')
            # Ensure year is numeric
            if 'year' in df.columns:
                df['year'] = pd.to_numeric(df['year'], errors='coerce')
                
        elif data_type == 'gps':
            # Ensure speed is numeric
            if 'speed' in df.columns:
                df['speed'] = pd.to_numeric(df['speed'], errors='coerce')
                
        elif data_type == 'traffic':
            # Clean traffic specific fields
            pass
            
        elif data_type == 'weather':
            # Ensure numeric weather fields
            numeric_cols = ['temperature', 'humidity', 'wind_speed', 'precipitation']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
                    
        elif data_type == 'emergency':
            # Emergency specific cleaning
            pass
        
        # Remove rows with null primary keys
        if 'id' in df.columns:
            df = df.dropna(subset=['id'])
            
        return df
        
    except Exception as e:
        logging.error(f"❌ Error cleaning {data_type} data: {str(e)}")
        return df

def load_to_snowflake(data_type: str, df: pd.DataFrame) -> None:
    """
    Load transformed data into Snowflake
    """
    logging.info(f"📤 Loading {data_type} data to Snowflake")
    
    if df.empty:
        logging.warning(f"⚠️ No data to load for {data_type}")
        return
    
    table_name = f"{data_type.upper()}_DATA"
    
    try:
        # Standardize column names to uppercase for Snowflake
        df.columns = df.columns.str.upper()
        
        # Handle specific column name mappings for different data types
        if data_type == 'vehicle':
            column_mapping = {
                'DEVICEID': 'DEVICE_ID',
                'FUELTYPE': 'FUEL_TYPE'
            }
            df.rename(columns=column_mapping, inplace=True)
            
        elif data_type == 'gps':
            column_mapping = {
                'DEVICEID': 'DEVICE_ID',
                'VEHICLETYPE': 'VEHICLE_TYPE'
            }
            df.rename(columns=column_mapping, inplace=True)
            
        elif data_type == 'traffic':
            column_mapping = {
                'DEVICEID': 'DEVICE_ID',
                'CAMERAID': 'CAMERA_ID'
            }
            df.rename(columns=column_mapping, inplace=True)
            
        elif data_type == 'weather':
            column_mapping = {
                'AIRQUALITYINDEX': 'AIR_QUALITY_INDEX',
                'WEATHERCONDITION': 'WEATHER_CONDITION',
                'WINDSPEED': 'WIND_SPEED',
                'DEVICEID': 'DEVICE_ID'
            }
            df.rename(columns=column_mapping, inplace=True)
            
        elif data_type == 'emergency':
            column_mapping = {
                'DEVICEID': 'DEVICE_ID',
                'INCIDENTTYPE': 'INCIDENT_TYPE',
                'INCIDENTID': 'INCIDENT_ID'
            }
            df.rename(columns=column_mapping, inplace=True)
        
        # Get Snowflake connection
        conn = get_snowflake_connection()
        
        # Create cursor
        cursor = conn.cursor()
        
        # Use the correct database and schema
        cursor.execute(f"USE DATABASE {SNOWFLAKE_CONFIG['database']}")
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_CONFIG['schema']}")
        
        # Truncate table before loading (for daily refresh)
        logging.info(f"🗑️ Truncating table {table_name}")
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        
        # Load data using pandas tools
        logging.info(f"📊 Writing {len(df)} rows to {table_name}")
        
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table_name,
            database=SNOWFLAKE_CONFIG['database'],
            schema=SNOWFLAKE_CONFIG['schema'],
            auto_create_table=False,  # Tables already created in Phase 2
            overwrite=False  # We truncated above
        )
        
        if success:
            logging.info(f"✅ Successfully loaded {nrows} rows to {table_name}")
        else:
            raise Exception(f"Failed to write data to {table_name}")
            
        # Commit and close
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"❌ Error loading {data_type} to Snowflake: {str(e)}")
        raise

def validate_data_load(data_type: str) -> None:
    """
    Validate that data was successfully loaded to Snowflake
    """
    logging.info(f"✅ Validating {data_type} data load")
    
    table_name = f"{data_type.upper()}_DATA"
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        # Use the correct database and schema
        cursor.execute(f"USE DATABASE {SNOWFLAKE_CONFIG['database']}")
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_CONFIG['schema']}")
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        row_count = cursor.fetchone()[0]
        
        # Get latest timestamp
        cursor.execute(f"SELECT MAX(INGESTION_TIME) FROM {table_name}")
        latest_etl = cursor.fetchone()[0]
        
        logging.info(f"📊 {table_name}: {row_count} rows, latest ETL: {latest_etl}")
        
        if row_count == 0:
            logging.warning(f"⚠️ No data found in {table_name}")
        else:
            logging.info(f"✅ Validation successful for {table_name}")
            
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"❌ Error validating {data_type}: {str(e)}")
        raise

# Define task functions
def process_vehicle_data():
    """Process vehicle data from S3 to Snowflake"""
    df = extract_and_transform_data('vehicle')
    load_to_snowflake('vehicle', df)
    validate_data_load('vehicle')

def process_gps_data():
    """Process GPS data from S3 to Snowflake"""
    df = extract_and_transform_data('gps')
    load_to_snowflake('gps', df)
    validate_data_load('gps')

def process_traffic_data():
    """Process traffic data from S3 to Snowflake"""
    df = extract_and_transform_data('traffic')
    load_to_snowflake('traffic', df)
    validate_data_load('traffic')

def process_weather_data():
    """Process weather data from S3 to Snowflake"""
    df = extract_and_transform_data('weather')
    load_to_snowflake('weather', df)
    validate_data_load('weather')

def process_emergency_data():
    """Process emergency data from S3 to Snowflake"""
    df = extract_and_transform_data('emergency')
    load_to_snowflake('emergency', df)
    validate_data_load('emergency')

def generate_etl_summary():
    """Generate ETL summary report"""
    logging.info("📊 Generating ETL summary")
    
    try:
        conn = get_snowflake_connection()
        cursor = conn.cursor()
        
        cursor.execute(f"USE DATABASE {SNOWFLAKE_CONFIG['database']}")
        cursor.execute(f"USE SCHEMA {SNOWFLAKE_CONFIG['schema']}")
        
        summary = {}
        for data_type in DATA_TYPES:
            table_name = f"{data_type.upper()}_DATA"
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            row_count = cursor.fetchone()[0]
            summary[data_type] = row_count
        
        cursor.close()
        conn.close()
        
        logging.info("📋 ETL Summary:")
        for data_type, count in summary.items():
            logging.info(f"   {data_type}: {count:,} rows")
            
        total_rows = sum(summary.values())
        logging.info(f"📊 Total rows processed: {total_rows:,}")
        
    except Exception as e:
        logging.error(f"❌ Error generating summary: {str(e)}")

# Create DAG
dag = DAG(
    DAG_ID,
    default_args=default_args,
    description='Smart City S3 to Snowflake ETL Pipeline',
    schedule_interval=SCHEDULE_INTERVAL,
    catchup=CATCHUP,
    tags=['smartcity', 'etl', 's3', 'snowflake', 'analytics']
)

# Define tasks
start_task = DummyOperator(
    task_id='start_etl_pipeline',
    dag=dag
)

# Data processing tasks
vehicle_task = PythonOperator(
    task_id='process_vehicle_data',
    python_callable=process_vehicle_data,
    dag=dag
)

gps_task = PythonOperator(
    task_id='process_gps_data',
    python_callable=process_gps_data,
    dag=dag
)

traffic_task = PythonOperator(
    task_id='process_traffic_data',
    python_callable=process_traffic_data,
    dag=dag
)

weather_task = PythonOperator(
    task_id='process_weather_data',
    python_callable=process_weather_data,
    dag=dag
)

emergency_task = PythonOperator(
    task_id='process_emergency_data',
    python_callable=process_emergency_data,
    dag=dag
)

# Summary task
summary_task = PythonOperator(
    task_id='generate_etl_summary',
    python_callable=generate_etl_summary,
    dag=dag
)

end_task = DummyOperator(
    task_id='etl_pipeline_complete',
    dag=dag
)

# Define task dependencies
start_task >> [vehicle_task, gps_task, traffic_task, weather_task, emergency_task]
[vehicle_task, gps_task, traffic_task, weather_task, emergency_task] >> summary_task >> end_task
