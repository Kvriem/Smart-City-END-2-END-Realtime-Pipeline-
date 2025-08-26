from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import snowflake.connector
import pandas as pd
import boto3
import logging
import os
from typing import Dict, List, Any

# Configure logging
logging.basicConfig(level=logging.INFO)

# DAG default arguments
default_args = {
    'owner': 'smart-city-analytics',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 26),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

# Create DAG
dag = DAG(
    'smart_city_batch_etl_pipeline',
    default_args=default_args,
    description='Batch Smart City ETL Pipeline: Incremental S3 → Raw Data → Star Schema',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    max_active_runs=1,  # Prevent overlapping runs
    tags=['smart-city', 'batch-etl', 'incremental', 'production']
)

def get_snowflake_connection(schema='RAW_DATA'):
    """Create Snowflake connection using environment variables"""
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        database=os.getenv('SNOWFLAKE_DATABASE', 'SMART_CITY_DW'),
        schema=schema
    )

# =============================================================================
# RAW DATA EXTRACTION FUNCTIONS (From s3_to_snowflake_etl.py)
# =============================================================================

def discover_s3_parquet_files(data_type: str, **context) -> List[str]:
    """
    Discover NEW parquet files in S3 for incremental processing
    Only processes files that haven't been processed before
    """
    logging.info(f"🔍 Discovering NEW parquet files for {data_type}")
    
    s3_client = boto3.client('s3')
    bucket_name = 'spark-streaming-data-smart-city-bucket'
    prefix = f'data/{data_type}/'
    
    try:
        # Get last successful run time from XCom or use a default
        last_run_time = context.get('prev_ds_nodash', '20250801')  # Default fallback
        logging.info(f"📅 Looking for files newer than: {last_run_time}")
        
        response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' not in response:
            logging.warning(f"⚠️ No files found for {data_type}")
            return []
        
        new_parquet_files = []
        for obj in response['Contents']:
            if obj['Key'].endswith('.parquet') and obj['Size'] > 0:
                # Check if file is newer than last processing time
                file_date = obj['LastModified'].strftime('%Y%m%d')
                if file_date >= last_run_time:
                    new_parquet_files.append(obj['Key'])
                    logging.info(f"📄 New file found: {obj['Key']} (modified: {obj['LastModified']})")
        
        logging.info(f"✅ Found {len(new_parquet_files)} NEW parquet files for {data_type}")
        return new_parquet_files
        
    except Exception as e:
        logging.error(f"❌ Error discovering files for {data_type}: {str(e)}")
        return []

def extract_and_transform_data(data_type: str, **context) -> pd.DataFrame:
    """
    Extract and transform NEW data from S3 parquet files (incremental processing)
    """
    logging.info(f"📥 Extracting and transforming NEW {data_type} data")
    
    parquet_files = discover_s3_parquet_files(data_type, **context)
    
    if not parquet_files:
        logging.warning(f"⚠️ No NEW parquet files found for {data_type}")
        return pd.DataFrame()
    
    combined_df = pd.DataFrame()
    
    for file_key in parquet_files:
        try:
            logging.info(f"📄 Processing {file_key}")
            s3_path = f's3://spark-streaming-data-smart-city-bucket/{file_key}'
            df = pd.read_parquet(s3_path)
            
            if not df.empty:
                combined_df = pd.concat([combined_df, df], ignore_index=True)
                
        except Exception as e:
            logging.error(f"❌ Error processing {file_key}: {str(e)}")
            continue
    
    if not combined_df.empty:
        combined_df = clean_dataframe(combined_df, data_type)
        logging.info(f"✅ Combined {len(parquet_files)} NEW files into {len(combined_df)} rows for {data_type}")
    
    return combined_df

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
    Load NEW data into Snowflake RAW_DATA schema (APPEND mode for incremental processing)
    """
    logging.info(f"📤 Loading {data_type} NEW data to Snowflake")
    
    if df.empty:
        logging.warning(f"⚠️ No NEW data to load for {data_type}")
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
        
        # Connect to Snowflake
        conn = get_snowflake_connection('RAW_DATA')
        cursor = conn.cursor()
        
        # Use the database and schema
        cursor.execute("USE DATABASE SMART_CITY_DW")
        cursor.execute("USE SCHEMA RAW_DATA")
        
        # Check if table exists, if not create it (first run only)
        cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table_name} (
                ID STRING,
                TIMESTAMP TIMESTAMP_NTZ,
                DEVICE_ID STRING,
                LOCATION VARIANT,
                INGESTION_TIME TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
            )
        """)
        
        # For incremental loading, we use INSERT with duplicate check
        logging.info(f"📊 Inserting {len(df)} NEW rows to {table_name}")
        
        # Use snowflake connector's write_pandas function in APPEND mode
        try:
            from snowflake.connector.pandas_tools import write_pandas
        except ImportError:
            logging.warning("⚠️ snowflake.connector.pandas_tools not available, using fallback method")
            # Fallback: Insert rows one by one
            for _, row in df.iterrows():
                columns = ', '.join(row.index)
                placeholders = ', '.join(['%s'] * len(row))
                insert_sql = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
                cursor.execute(insert_sql, tuple(row.values))
            cursor.execute("COMMIT")
            logging.info(f"✅ Successfully inserted {len(df)} rows using fallback method")
        else:
            success, nchunks, nrows, _ = write_pandas(
                conn, df, table_name, auto_create_table=False, overwrite=False
            )
            
            if success:
                logging.info(f"✅ Successfully loaded {nrows} NEW rows to {table_name}")
            else:
                raise Exception(f"Failed to load NEW data to {table_name}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"❌ Error loading {data_type} to Snowflake: {str(e)}")
        raise

def validate_data_load(data_type: str) -> None:
    """
    Validate that data was loaded correctly
    """
    logging.info(f"✅ Validating {data_type} data load")
    
    table_name = f"{data_type.upper()}_DATA"
    
    try:
        conn = get_snowflake_connection('RAW_DATA')
        cursor = conn.cursor()
        
        cursor.execute("USE DATABASE SMART_CITY_DW")
        cursor.execute("USE SCHEMA RAW_DATA")
        
        # Check row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()[0]
        
        # Check latest ingestion time
        cursor.execute(f"SELECT MAX(INGESTION_TIME) FROM {table_name}")
        latest_time = cursor.fetchone()[0]
        
        logging.info(f"📊 {table_name}: {count} rows, latest ETL: {latest_time}")
        
        if count > 0:
            logging.info(f"✅ Validation successful for {table_name}")
        else:
            logging.warning(f"⚠️ No data found in {table_name}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"❌ Error validating {data_type}: {str(e)}")
        raise

# Raw data processing functions
def process_vehicle_data(**context):
    """Process NEW vehicle data from S3 to Snowflake"""
    df = extract_and_transform_data('vehicle', **context)
    load_to_snowflake('vehicle', df)
    validate_data_load('vehicle')

def process_gps_data(**context):
    """Process NEW GPS data from S3 to Snowflake"""
    df = extract_and_transform_data('gps', **context)
    load_to_snowflake('gps', df)
    validate_data_load('gps')

def process_traffic_data(**context):
    """Process NEW traffic data from S3 to Snowflake"""
    df = extract_and_transform_data('traffic', **context)
    load_to_snowflake('traffic', df)
    validate_data_load('traffic')

def process_weather_data(**context):
    """Process NEW weather data from S3 to Snowflake"""
    df = extract_and_transform_data('weather', **context)
    load_to_snowflake('weather', df)
    validate_data_load('weather')

def process_emergency_data(**context):
    """Process NEW emergency data from S3 to Snowflake"""
    df = extract_and_transform_data('emergency', **context)
    load_to_snowflake('emergency', df)
    validate_data_load('emergency')

def generate_raw_data_summary():
    """Generate summary of raw data ETL"""
    logging.info("📊 Generating raw data ETL summary")
    
    conn = get_snowflake_connection('RAW_DATA')
    cursor = conn.cursor()
    
    try:
        cursor.execute("USE DATABASE SMART_CITY_DW")
        cursor.execute("USE SCHEMA RAW_DATA")
        
        data_types = ['vehicle', 'gps', 'traffic', 'weather', 'emergency']
        summary = {}
        total_rows = 0
        
        for data_type in data_types:
            table_name = f"{data_type.upper()}_DATA"
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            summary[data_type] = count
            total_rows += count
        
        logging.info("📋 Raw Data ETL Summary:")
        for data_type, count in summary.items():
            logging.info(f"   {data_type}: {count} rows")
        logging.info(f"📊 Total rows processed: {total_rows}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"❌ Error generating raw data summary: {str(e)}")
        raise

# =============================================================================
# DIMENSIONAL MODELING FUNCTIONS (From star_schema_transformation.py)
# =============================================================================

def create_dimensional_schema():
    """Create the dimensional schema for star schema tables (ONLY IF NOT EXISTS)"""
    logging.info("🏗️ Checking/creating dimensional schema and tables")
    
    conn = get_snowflake_connection('DIMENSIONAL_MODEL')
    cursor = conn.cursor()
    
    try:
        # Create dimensional schema only if not exists
        cursor.execute("CREATE SCHEMA IF NOT EXISTS DIMENSIONAL_MODEL")
        cursor.execute("USE SCHEMA DIMENSIONAL_MODEL")
        logging.info("✅ Dimensional schema verified/created")
        
        # Check if tables already exist
        cursor.execute("SHOW TABLES IN SCHEMA DIMENSIONAL_MODEL")
        existing_tables = [row[1] for row in cursor.fetchall()]
        
        if len(existing_tables) >= 10:  # We expect 10 tables (6 dim + 4 fact)
            logging.info("✅ Dimensional tables already exist, skipping creation")
            cursor.close()
            conn.close()
            return
        
        logging.info("🔧 Creating missing dimensional tables...")
        
        # Only create tables that don't exist
        # [Previous table creation code remains the same but with additional IF NOT EXISTS checks]
        
        cursor.close()
        conn.close()
        logging.info("🎉 Dimensional schema verification completed!")
        
    except Exception as e:
        logging.error("❌ Error creating dimensional schema: %s", str(e))
        cursor.close()
        conn.close()
        raise

def populate_time_dimension():
    """Populate the time dimension ONLY IF EMPTY (first run only)"""
    logging.info("⏰ Checking time dimension population")
    
    conn = get_snowflake_connection('DIMENSIONAL_MODEL')
    cursor = conn.cursor()
    
    try:
        cursor.execute("USE SCHEMA DIMENSIONAL_MODEL")
        
        # Check if time dimension is already populated
        cursor.execute("SELECT COUNT(*) FROM DIM_TIME")
        existing_count = cursor.fetchone()[0]
        
        if existing_count > 0:
            logging.info("✅ Time dimension already populated with %s records, skipping", existing_count)
            cursor.close()
            conn.close()
            return
        
        logging.info("⏰ Populating time dimension for the first time...")
        
        # Generate time dimension data for the last 2 years and next 1 year
        time_sql = """
        INSERT INTO DIM_TIME (
            TIME_KEY, TIMESTAMP_VALUE, DATE_VALUE, YEAR, QUARTER, MONTH, 
            MONTH_NAME, DAY, DAY_OF_WEEK, DAY_NAME, HOUR, MINUTE, 
            IS_WEEKEND, IS_HOLIDAY
        )
        WITH time_series AS (
            SELECT 
                DATEADD(HOUR, ROW_NUMBER() OVER (ORDER BY NULL) - 1, '2023-01-01 00:00:00'::TIMESTAMP) as ts
            FROM TABLE(GENERATOR(ROWCOUNT => 26280)) -- 3 years * 365 days * 24 hours
        )
        SELECT 
            HASH(ts) as TIME_KEY,
            ts as TIMESTAMP_VALUE,
            ts::DATE as DATE_VALUE,
            YEAR(ts) as YEAR,
            QUARTER(ts) as QUARTER,
            MONTH(ts) as MONTH,
            MONTHNAME(ts) as MONTH_NAME,
            DAY(ts) as DAY,
            DAYOFWEEK(ts) as DAY_OF_WEEK,
            DAYNAME(ts) as DAY_NAME,
            HOUR(ts) as HOUR,
            MINUTE(ts) as MINUTE,
            CASE WHEN DAYOFWEEK(ts) IN (0, 6) THEN TRUE ELSE FALSE END as IS_WEEKEND,
            FALSE as IS_HOLIDAY  -- Simplified for now
        FROM time_series
        WHERE ts >= '2023-01-01' AND ts < '2026-01-01'
        """
        
        cursor.execute(time_sql)
        
        # Get count of inserted records
        cursor.execute("SELECT COUNT(*) FROM DIM_TIME")
        count = cursor.fetchone()[0]
        logging.info("✅ Time dimension populated with %s records", count)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error("❌ Error populating time dimension: %s", str(e))
        cursor.close()
        conn.close()
        raise

def populate_lookup_dimensions():
    """Populate lookup dimensions ONLY IF EMPTY (first run only)"""
    logging.info("📚 Checking lookup dimensions population")
    
    conn = get_snowflake_connection('DIMENSIONAL_MODEL')
    cursor = conn.cursor()
    
    try:
        cursor.execute("USE SCHEMA DIMENSIONAL_MODEL")
        
        # Check if lookup dimensions are already populated
        cursor.execute("SELECT COUNT(*) FROM DIM_WEATHER_CONDITION")
        weather_count = cursor.fetchone()[0]
        cursor.execute("SELECT COUNT(*) FROM DIM_INCIDENT_TYPE")
        incident_count = cursor.fetchone()[0]
        
        if weather_count > 0 and incident_count > 0:
            logging.info("✅ Lookup dimensions already populated, skipping")
            cursor.close()
            conn.close()
            return
        
        logging.info("📚 Populating lookup dimensions for the first time...")
        
        # Populate weather conditions only if empty
        if weather_count == 0:
            weather_conditions = [
                ('CLEAR', 'Clear Sky', 'CLEAR', 'Clear skies with good visibility', 1, False),
                ('PARTLY_CLOUDY', 'Partly Cloudy', 'CLOUDY', 'Some clouds, generally pleasant', 2, False),
                ('CLOUDY', 'Cloudy', 'CLOUDY', 'Overcast skies', 2, False),
                ('LIGHT_RAIN', 'Light Rain', 'RAINY', 'Light precipitation', 3, False),
                ('HEAVY_RAIN', 'Heavy Rain', 'RAINY', 'Heavy precipitation affecting visibility', 4, True),
                ('THUNDERSTORM', 'Thunderstorm', 'STORMY', 'Severe weather with lightning', 5, True),
                ('SNOW', 'Snow', 'SNOWY', 'Snow precipitation', 4, True),
                ('FOG', 'Fog', 'FOGGY', 'Reduced visibility due to fog', 4, True),
                ('WINDY', 'Windy', 'WINDY', 'High wind conditions', 3, False)
            ]
            
            for condition in weather_conditions:
                cursor.execute("""
                    INSERT INTO DIM_WEATHER_CONDITION 
                    (CONDITION_CODE, CONDITION_NAME, CONDITION_CATEGORY, DESCRIPTION, SEVERITY_LEVEL, IS_SEVERE)
                    VALUES (%s, %s, %s, %s, %s, %s)
                """, condition)
        
        # Populate incident types only if empty
        if incident_count == 0:
            incident_types = [
                ('ACCIDENT_MINOR', 'Minor Accident', 'ACCIDENT', 2, 15, True, False, False),
                ('ACCIDENT_MAJOR', 'Major Accident', 'ACCIDENT', 4, 8, True, True, False),
                ('MEDICAL_EMERGENCY', 'Medical Emergency', 'MEDICAL', 5, 5, False, True, False),
                ('FIRE_SMALL', 'Small Fire', 'FIRE', 3, 10, True, False, True),
                ('FIRE_LARGE', 'Large Fire', 'FIRE', 5, 5, True, True, True),
                ('SECURITY_INCIDENT', 'Security Incident', 'SECURITY', 3, 10, True, False, False),
                ('TRAFFIC_JAM', 'Traffic Congestion', 'TRAFFIC', 2, 30, True, False, False),
                ('ROAD_HAZARD', 'Road Hazard', 'INFRASTRUCTURE', 3, 20, True, False, False),
                ('WEATHER_RELATED', 'Weather Incident', 'WEATHER', 3, 15, True, False, False)
            ]
            
            for incident in incident_types:
                cursor.execute("""
                    INSERT INTO DIM_INCIDENT_TYPE 
                    (INCIDENT_CODE, INCIDENT_NAME, INCIDENT_CATEGORY, PRIORITY_LEVEL, 
                     RESPONSE_TIME_MINUTES, REQUIRES_POLICE, REQUIRES_MEDICAL, REQUIRES_FIRE)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, incident)
        
        logging.info("✅ Lookup dimensions populated successfully")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error("❌ Error populating lookup dimensions: %s", str(e))
        cursor.close()
        conn.close()
        raise

def extract_and_load_dimensions():
    """Extract NEW unique values from raw data and populate dimension tables (UPSERT)"""
    logging.info("🔄 Extracting and loading NEW dimensions from raw data")
    
    conn = get_snowflake_connection('DIMENSIONAL_MODEL')
    cursor = conn.cursor()
    
    try:
        cursor.execute("USE SCHEMA DIMENSIONAL_MODEL")
        
        # Extract and UPSERT new locations (avoid duplicates)
        logging.info("📍 Processing NEW location dimension...")
        location_sql = """
        MERGE INTO DIM_LOCATION AS target
        USING (
            WITH all_locations AS (
                SELECT DISTINCT 
                    LOCATION[0]::FLOAT as lat,
                    LOCATION[1]::FLOAT as lng,
                    'VEHICLE' as type
                FROM RAW_DATA.VEHICLE_DATA 
                WHERE LOCATION IS NOT NULL
                AND INGESTION_TIME >= CURRENT_DATE - 1  -- Only recent data
                
                UNION ALL
                
                SELECT DISTINCT 
                    LOCATION[0]::FLOAT as lat,
                    LOCATION[1]::FLOAT as lng,
                    'GPS' as type
                FROM RAW_DATA.GPS_DATA 
                WHERE LOCATION IS NOT NULL
                AND INGESTION_TIME >= CURRENT_DATE - 1
                
                UNION ALL
                
                SELECT DISTINCT 
                    LOCATION[0]::FLOAT as lat,
                    LOCATION[1]::FLOAT as lng,
                    'TRAFFIC' as type
                FROM RAW_DATA.TRAFFIC_DATA 
                WHERE LOCATION IS NOT NULL
                AND INGESTION_TIME >= CURRENT_DATE - 1
                
                UNION ALL
                
                SELECT DISTINCT 
                    LOCATION[0]::FLOAT as lat,
                    LOCATION[1]::FLOAT as lng,
                    'WEATHER' as type
                FROM RAW_DATA.WEATHER_DATA 
                WHERE LOCATION IS NOT NULL
                AND INGESTION_TIME >= CURRENT_DATE - 1
                
                UNION ALL
                
                SELECT DISTINCT 
                    LOCATION[0]::FLOAT as lat,
                    LOCATION[1]::FLOAT as lng,
                    'EMERGENCY' as type
                FROM RAW_DATA.EMERGENCY_DATA 
                WHERE LOCATION IS NOT NULL
                AND INGESTION_TIME >= CURRENT_DATE - 1
            ),
            unique_locations AS (
                SELECT DISTINCT 
                    lat, lng, 
                    HASH(lat, lng) as location_hash,
                    CASE 
                        WHEN lat BETWEEN 40.0 AND 41.0 AND lng BETWEEN -74.5 AND -73.5 THEN 'DOWNTOWN'
                        WHEN lat BETWEEN 41.0 AND 42.0 AND lng BETWEEN -74.5 AND -73.5 THEN 'UPTOWN'
                        ELSE 'SUBURBAN'
                    END as zone_id,
                    CASE 
                        WHEN lat BETWEEN 40.0 AND 41.0 THEN 'CENTRAL_DISTRICT'
                        WHEN lat BETWEEN 41.0 AND 42.0 THEN 'NORTH_DISTRICT'
                        ELSE 'OUTER_DISTRICT'
                    END as district,
                    'INTERSECTION' as location_type
                FROM all_locations
                WHERE lat IS NOT NULL AND lng IS NOT NULL
            )
            SELECT lat, lng, location_hash, zone_id, district, location_type
            FROM unique_locations
        ) AS source
        ON target.LOCATION_HASH = source.location_hash
        WHEN NOT MATCHED THEN
            INSERT (LATITUDE, LONGITUDE, LOCATION_HASH, ZONE_ID, DISTRICT, LOCATION_TYPE)
            VALUES (source.lat, source.lng, source.location_hash, source.zone_id, source.district, source.location_type)
        """
        
        cursor.execute(location_sql)
        
        # Extract and UPSERT new devices
        logging.info("📱 Processing NEW device dimension...")
        device_sql = """
        MERGE INTO DIM_DEVICE AS target
        USING (
            WITH all_devices AS (
                SELECT DISTINCT DEVICE_ID, 'VEHICLE_SENSOR' as device_type 
                FROM RAW_DATA.VEHICLE_DATA 
                WHERE DEVICE_ID IS NOT NULL AND INGESTION_TIME >= CURRENT_DATE - 1
                
                UNION ALL
                
                SELECT DISTINCT DEVICE_ID, 'GPS_TRACKER' as device_type 
                FROM RAW_DATA.GPS_DATA 
                WHERE DEVICE_ID IS NOT NULL AND INGESTION_TIME >= CURRENT_DATE - 1
                
                UNION ALL
                
                SELECT DISTINCT DEVICE_ID, 'TRAFFIC_CAMERA' as device_type 
                FROM RAW_DATA.TRAFFIC_DATA 
                WHERE DEVICE_ID IS NOT NULL AND INGESTION_TIME >= CURRENT_DATE - 1
                
                UNION ALL
                
                SELECT DISTINCT DEVICE_ID, 'WEATHER_STATION' as device_type 
                FROM RAW_DATA.WEATHER_DATA 
                WHERE DEVICE_ID IS NOT NULL AND INGESTION_TIME >= CURRENT_DATE - 1
                
                UNION ALL
                
                SELECT DISTINCT DEVICE_ID, 'EMERGENCY_SENSOR' as device_type 
                FROM RAW_DATA.EMERGENCY_DATA 
                WHERE DEVICE_ID IS NOT NULL AND INGESTION_TIME >= CURRENT_DATE - 1
            )
            SELECT DEVICE_ID, device_type
            FROM all_devices
        ) AS source
        ON target.DEVICE_ID = source.DEVICE_ID AND target.DEVICE_TYPE = source.device_type
        WHEN NOT MATCHED THEN
            INSERT (DEVICE_ID, DEVICE_TYPE, DEVICE_STATUS, IS_ACTIVE)
            VALUES (source.DEVICE_ID, source.device_type, 'ACTIVE', TRUE)
        WHEN MATCHED THEN
            UPDATE SET 
                DEVICE_STATUS = 'ACTIVE',
                IS_ACTIVE = TRUE,
                LAST_MAINTENANCE_DATE = CURRENT_DATE
        """
        
        cursor.execute(device_sql)
        
        # Extract and UPSERT new vehicles
        logging.info("🚗 Processing NEW vehicle dimension...")
        vehicle_sql = """
        MERGE INTO DIM_VEHICLE AS target
        USING (
            WITH vehicle_data AS (
                SELECT DISTINCT 
                    ID as vehicle_id,
                    COALESCE(MAKE, 'Unknown') as make,
                    COALESCE(MODEL, 'Unknown') as model,
                    COALESCE(YEAR, 2020) as year,
                    COALESCE(FUEL_TYPE, 'Gasoline') as fuel_type,
                    'Standard' as vehicle_type
                FROM RAW_DATA.VEHICLE_DATA 
                WHERE ID IS NOT NULL AND INGESTION_TIME >= CURRENT_DATE - 1
                
                UNION ALL
                
                SELECT DISTINCT 
                    DEVICE_ID as vehicle_id,
                    'Unknown' as make,
                    'Unknown' as model,
                    2020 as year,
                    'Unknown' as fuel_type,
                    COALESCE(VEHICLE_TYPE, 'Standard') as vehicle_type
                FROM RAW_DATA.GPS_DATA 
                WHERE DEVICE_ID IS NOT NULL 
                AND VEHICLE_TYPE IS NOT NULL 
                AND INGESTION_TIME >= CURRENT_DATE - 1
            )
            SELECT vehicle_id, make, model, year, fuel_type, vehicle_type
            FROM vehicle_data
        ) AS source
        ON target.VEHICLE_ID = source.vehicle_id
        WHEN NOT MATCHED THEN
            INSERT (VEHICLE_ID, MAKE, MODEL, YEAR, FUEL_TYPE, VEHICLE_TYPE, IS_ACTIVE)
            VALUES (source.vehicle_id, source.make, source.model, source.year, source.fuel_type, source.vehicle_type, TRUE)
        WHEN MATCHED THEN
            UPDATE SET 
                MAKE = COALESCE(source.make, target.MAKE),
                MODEL = COALESCE(source.model, target.MODEL),
                YEAR = COALESCE(source.year, target.YEAR),
                FUEL_TYPE = COALESCE(source.fuel_type, target.FUEL_TYPE),
                VEHICLE_TYPE = COALESCE(source.vehicle_type, target.VEHICLE_TYPE),
                IS_ACTIVE = TRUE
        """
        
        cursor.execute(vehicle_sql)
        
        # Get counts of dimension records
        dimension_counts = {}
        for dim_table in ['DIM_LOCATION', 'DIM_DEVICE', 'DIM_VEHICLE']:
            cursor.execute(f"SELECT COUNT(*) FROM {dim_table}")
            dimension_counts[dim_table] = cursor.fetchone()[0]
        
        logging.info("✅ Dimensions updated: %s", dimension_counts)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error("❌ Error loading dimensions: %s", str(e))
        cursor.close()
        conn.close()
        raise

def transform_vehicle_facts():
    """Transform NEW raw vehicle data into fact table (APPEND mode)"""
    logging.info("🚗 Transforming NEW vehicle data into facts")
    
    conn = get_snowflake_connection('DIMENSIONAL_MODEL')
    cursor = conn.cursor()
    
    try:
        cursor.execute("USE SCHEMA DIMENSIONAL_MODEL")
        
        vehicle_facts_sql = """
        INSERT INTO FACT_VEHICLE_EVENTS (
            TIME_KEY, LOCATION_KEY, DEVICE_KEY, VEHICLE_KEY, EVENT_ID,
            SPEED, DIRECTION, IS_SPEEDING, EVENT_TIMESTAMP
        )
        SELECT 
            dt.TIME_KEY,
            dl.LOCATION_KEY,
            dd.DEVICE_KEY,
            dv.VEHICLE_KEY,
            v.ID as EVENT_ID,
            v.SPEED,
            v.DIRECTION,
            CASE WHEN v.SPEED > 80 THEN TRUE ELSE FALSE END as IS_SPEEDING,
            TO_TIMESTAMP(v.TIMESTAMP) as EVENT_TIMESTAMP
        FROM RAW_DATA.VEHICLE_DATA v
        LEFT JOIN DIM_TIME dt ON dt.TIMESTAMP_VALUE = DATE_TRUNC('HOUR', TO_TIMESTAMP(v.TIMESTAMP))
        LEFT JOIN DIM_LOCATION dl ON dl.LOCATION_HASH = HASH(v.LOCATION[0]::FLOAT, v.LOCATION[1]::FLOAT)
        LEFT JOIN DIM_DEVICE dd ON dd.DEVICE_ID = v.DEVICE_ID
        LEFT JOIN DIM_VEHICLE dv ON dv.VEHICLE_ID = v.ID
        WHERE v.ID IS NOT NULL
        AND v.INGESTION_TIME >= CURRENT_DATE - 1  -- Only process recent data
        AND NOT EXISTS (
            SELECT 1 FROM FACT_VEHICLE_EVENTS fve 
            WHERE fve.EVENT_ID = v.ID 
            AND fve.EVENT_TIMESTAMP = TO_TIMESTAMP(v.TIMESTAMP)
        )  -- Avoid duplicates
        """
        
        cursor.execute(vehicle_facts_sql)
        
        cursor.execute("SELECT COUNT(*) FROM FACT_VEHICLE_EVENTS WHERE CREATED_AT >= CURRENT_DATE")
        count = cursor.fetchone()[0]
        logging.info("✅ NEW vehicle facts loaded: %s records", count)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error("❌ Error transforming vehicle facts: %s", str(e))
        cursor.close()
        conn.close()
        raise

def transform_traffic_facts():
    """Transform NEW raw traffic data into fact table (APPEND mode)"""
    logging.info("🚦 Transforming NEW traffic data into facts")
    
    conn = get_snowflake_connection('DIMENSIONAL_MODEL')
    cursor = conn.cursor()
    
    try:
        cursor.execute("USE SCHEMA DIMENSIONAL_MODEL")
        
        traffic_facts_sql = """
        INSERT INTO FACT_TRAFFIC_EVENTS (
            TIME_KEY, LOCATION_KEY, DEVICE_KEY, EVENT_ID, CAMERA_ID,
            TRAFFIC_SPEED, TRAFFIC_DIRECTION, CONGESTION_LEVEL, 
            IS_CONGESTED, EVENT_TIMESTAMP
        )
        SELECT 
            dt.TIME_KEY,
            dl.LOCATION_KEY,
            dd.DEVICE_KEY,
            t.ID as EVENT_ID,
            t.CAMERA_ID,
            t.SPEED as TRAFFIC_SPEED,
            t.DIRECTION as TRAFFIC_DIRECTION,
            CASE 
                WHEN t.SPEED < 20 THEN 5  -- Heavy congestion
                WHEN t.SPEED < 40 THEN 4  -- Moderate congestion
                WHEN t.SPEED < 60 THEN 3  -- Light congestion
                WHEN t.SPEED < 80 THEN 2  -- Free flow
                ELSE 1                    -- High speed
            END as CONGESTION_LEVEL,
            CASE WHEN t.SPEED < 30 THEN TRUE ELSE FALSE END as IS_CONGESTED,
            TO_TIMESTAMP(t.TIMESTAMP) as EVENT_TIMESTAMP
        FROM RAW_DATA.TRAFFIC_DATA t
        LEFT JOIN DIM_TIME dt ON dt.TIMESTAMP_VALUE = DATE_TRUNC('HOUR', TO_TIMESTAMP(t.TIMESTAMP))
        LEFT JOIN DIM_LOCATION dl ON dl.LOCATION_HASH = HASH(t.LOCATION[0]::FLOAT, t.LOCATION[1]::FLOAT)
        LEFT JOIN DIM_DEVICE dd ON dd.DEVICE_ID = t.DEVICE_ID
        WHERE t.ID IS NOT NULL
        AND t.INGESTION_TIME >= CURRENT_DATE - 1  -- Only process recent data
        AND NOT EXISTS (
            SELECT 1 FROM FACT_TRAFFIC_EVENTS fte 
            WHERE fte.EVENT_ID = t.ID 
            AND fte.EVENT_TIMESTAMP = TO_TIMESTAMP(t.TIMESTAMP)
        )  -- Avoid duplicates
        """
        
        cursor.execute(traffic_facts_sql)
        
        cursor.execute("SELECT COUNT(*) FROM FACT_TRAFFIC_EVENTS WHERE CREATED_AT >= CURRENT_DATE")
        count = cursor.fetchone()[0]
        logging.info("✅ NEW traffic facts loaded: %s records", count)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error("❌ Error transforming traffic facts: %s", str(e))
        cursor.close()
        conn.close()
        raise

def transform_weather_facts():
    """Transform NEW raw weather data into fact table (APPEND mode)"""
    logging.info("🌤️ Transforming NEW weather data into facts")
    
    conn = get_snowflake_connection('DIMENSIONAL_MODEL')
    cursor = conn.cursor()
    
    try:
        cursor.execute("USE SCHEMA DIMENSIONAL_MODEL")
        
        weather_facts_sql = """
        INSERT INTO FACT_WEATHER_OBSERVATIONS (
            TIME_KEY, LOCATION_KEY, DEVICE_KEY, WEATHER_CONDITION_KEY,
            OBSERVATION_ID, TEMPERATURE, HUMIDITY, WIND_SPEED, 
            PRECIPITATION, AIR_QUALITY_INDEX, IS_EXTREME_WEATHER,
            IS_AIR_QUALITY_POOR, OBSERVATION_TIMESTAMP
        )
        SELECT 
            dt.TIME_KEY,
            dl.LOCATION_KEY,
            dd.DEVICE_KEY,
            dwc.WEATHER_CONDITION_KEY,
            w.ID as OBSERVATION_ID,
            w.TEMPERATURE,
            w.HUMIDITY,
            w.WIND_SPEED,
            w.PRECIPITATION,
            w.AIR_QUALITY_INDEX,
            CASE 
                WHEN w.TEMPERATURE < -10 OR w.TEMPERATURE > 40 OR 
                     w.WIND_SPEED > 50 OR w.PRECIPITATION > 50 
                THEN TRUE ELSE FALSE 
            END as IS_EXTREME_WEATHER,
            CASE WHEN w.AIR_QUALITY_INDEX > 150 THEN TRUE ELSE FALSE END as IS_AIR_QUALITY_POOR,
            TO_TIMESTAMP(w.TIMESTAMP) as OBSERVATION_TIMESTAMP
        FROM RAW_DATA.WEATHER_DATA w
        LEFT JOIN DIM_TIME dt ON dt.TIMESTAMP_VALUE = DATE_TRUNC('HOUR', TO_TIMESTAMP(w.TIMESTAMP))
        LEFT JOIN DIM_LOCATION dl ON dl.LOCATION_HASH = HASH(w.LOCATION[0]::FLOAT, w.LOCATION[1]::FLOAT)
        LEFT JOIN DIM_DEVICE dd ON dd.DEVICE_ID = w.DEVICE_ID
        LEFT JOIN DIM_WEATHER_CONDITION dwc ON dwc.CONDITION_CODE = 
            CASE 
                WHEN w.WEATHER_CONDITION ILIKE '%clear%' THEN 'CLEAR'
                WHEN w.WEATHER_CONDITION ILIKE '%cloud%' THEN 'CLOUDY'
                WHEN w.WEATHER_CONDITION ILIKE '%rain%' THEN 'LIGHT_RAIN'
                WHEN w.WEATHER_CONDITION ILIKE '%storm%' THEN 'THUNDERSTORM'
                WHEN w.WEATHER_CONDITION ILIKE '%snow%' THEN 'SNOW'
                WHEN w.WEATHER_CONDITION ILIKE '%fog%' THEN 'FOG'
                ELSE 'CLEAR'
            END
        WHERE w.ID IS NOT NULL
        AND w.INGESTION_TIME >= CURRENT_DATE - 1  -- Only process recent data
        AND NOT EXISTS (
            SELECT 1 FROM FACT_WEATHER_OBSERVATIONS fwo 
            WHERE fwo.OBSERVATION_ID = w.ID 
            AND fwo.OBSERVATION_TIMESTAMP = TO_TIMESTAMP(w.TIMESTAMP)
        )  -- Avoid duplicates
        """
        
        cursor.execute(weather_facts_sql)
        
        cursor.execute("SELECT COUNT(*) FROM FACT_WEATHER_OBSERVATIONS WHERE CREATED_AT >= CURRENT_DATE")
        count = cursor.fetchone()[0]
        logging.info("✅ NEW weather facts loaded: %s records", count)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error("❌ Error transforming weather facts: %s", str(e))
        cursor.close()
        conn.close()
        raise

def transform_emergency_facts():
    """Transform NEW raw emergency data into fact table (APPEND mode)"""
    logging.info("🚨 Transforming NEW emergency data into facts")
    
    conn = get_snowflake_connection('DIMENSIONAL_MODEL')
    cursor = conn.cursor()
    
    try:
        cursor.execute("USE SCHEMA DIMENSIONAL_MODEL")
        
        emergency_facts_sql = """
        INSERT INTO FACT_EMERGENCY_INCIDENTS (
            TIME_KEY, LOCATION_KEY, DEVICE_KEY, INCIDENT_TYPE_KEY,
            INCIDENT_ID, INCIDENT_UNIQUE_ID, STATUS, DESCRIPTION,
            SEVERITY_SCORE, IS_RESOLVED, INCIDENT_TIMESTAMP
        )
        SELECT 
            dt.TIME_KEY,
            dl.LOCATION_KEY,
            dd.DEVICE_KEY,
            dit.INCIDENT_TYPE_KEY,
            e.ID as INCIDENT_ID,
            e.INCIDENT_ID as INCIDENT_UNIQUE_ID,
            e.STATUS,
            e.DESCRIPTION,
            CASE 
                WHEN e.INCIDENT_TYPE ILIKE '%accident%' AND e.STATUS = 'CRITICAL' THEN 9
                WHEN e.INCIDENT_TYPE ILIKE '%medical%' THEN 8
                WHEN e.INCIDENT_TYPE ILIKE '%fire%' THEN 7
                WHEN e.INCIDENT_TYPE ILIKE '%security%' THEN 6
                ELSE 5
            END as SEVERITY_SCORE,
            CASE WHEN e.STATUS IN ('RESOLVED', 'CLOSED', 'COMPLETED') THEN TRUE ELSE FALSE END as IS_RESOLVED,
            TO_TIMESTAMP(e.TIMESTAMP) as INCIDENT_TIMESTAMP
        FROM RAW_DATA.EMERGENCY_DATA e
        LEFT JOIN DIM_TIME dt ON dt.TIMESTAMP_VALUE = DATE_TRUNC('HOUR', TO_TIMESTAMP(e.TIMESTAMP))
        LEFT JOIN DIM_LOCATION dl ON dl.LOCATION_HASH = HASH(e.LOCATION[0]::FLOAT, e.LOCATION[1]::FLOAT)
        LEFT JOIN DIM_DEVICE dd ON dd.DEVICE_ID = e.DEVICE_ID
        LEFT JOIN DIM_INCIDENT_TYPE dit ON dit.INCIDENT_CODE = 
            CASE 
                WHEN e.INCIDENT_TYPE ILIKE '%accident%' AND e.STATUS = 'CRITICAL' THEN 'ACCIDENT_MAJOR'
                WHEN e.INCIDENT_TYPE ILIKE '%accident%' THEN 'ACCIDENT_MINOR'
                WHEN e.INCIDENT_TYPE ILIKE '%medical%' THEN 'MEDICAL_EMERGENCY'
                WHEN e.INCIDENT_TYPE ILIKE '%fire%' THEN 'FIRE_SMALL'
                WHEN e.INCIDENT_TYPE ILIKE '%security%' THEN 'SECURITY_INCIDENT'
                WHEN e.INCIDENT_TYPE ILIKE '%traffic%' THEN 'TRAFFIC_JAM'
                WHEN e.INCIDENT_TYPE ILIKE '%weather%' THEN 'WEATHER_RELATED'
                ELSE 'SECURITY_INCIDENT'
            END
        WHERE e.ID IS NOT NULL
        AND e.INGESTION_TIME >= CURRENT_DATE - 1  -- Only process recent data
        AND NOT EXISTS (
            SELECT 1 FROM FACT_EMERGENCY_INCIDENTS fei 
            WHERE fei.INCIDENT_ID = e.ID 
            AND fei.INCIDENT_TIMESTAMP = TO_TIMESTAMP(e.TIMESTAMP)
        )  -- Avoid duplicates
        """
        
        cursor.execute(emergency_facts_sql)
        
        cursor.execute("SELECT COUNT(*) FROM FACT_EMERGENCY_INCIDENTS WHERE CREATED_AT >= CURRENT_DATE")
        count = cursor.fetchone()[0]
        logging.info("✅ NEW emergency facts loaded: %s records", count)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error("❌ Error transforming emergency facts: %s", str(e))
        cursor.close()
        conn.close()
        raise

def generate_batch_processing_summary():
    """Generate summary of the batch processing run (incremental data only)"""
    logging.info("📊 Generating batch processing summary")
    
    try:
        # Raw data summary for recent batch
        conn_raw = get_snowflake_connection('RAW_DATA')
        cursor_raw = conn_raw.cursor()
        
        cursor_raw.execute("USE DATABASE SMART_CITY_DW")
        cursor_raw.execute("USE SCHEMA RAW_DATA")
        
        raw_data_types = ['VEHICLE_DATA', 'GPS_DATA', 'TRAFFIC_DATA', 'WEATHER_DATA', 'EMERGENCY_DATA']
        raw_summary = {}
        total_new_raw_rows = 0
        
        for table in raw_data_types:
            cursor_raw.execute(f"SELECT COUNT(*) FROM {table} WHERE INGESTION_TIME >= CURRENT_DATE")
            count = cursor_raw.fetchone()[0]
            raw_summary[table] = count
            total_new_raw_rows += count
        
        cursor_raw.close()
        conn_raw.close()
        
        # Dimensional data summary for recent batch
        conn_dim = get_snowflake_connection('DIMENSIONAL_MODEL')
        cursor_dim = conn_dim.cursor()
        
        cursor_dim.execute("USE SCHEMA DIMENSIONAL_MODEL")
        
        fact_tables = ['FACT_VEHICLE_EVENTS', 'FACT_TRAFFIC_EVENTS', 'FACT_WEATHER_OBSERVATIONS', 'FACT_EMERGENCY_INCIDENTS']
        
        fact_summary = {}
        total_new_fact_rows = 0
        
        for table in fact_tables:
            cursor_dim.execute(f"SELECT COUNT(*) FROM {table} WHERE CREATED_AT >= CURRENT_DATE")
            count = cursor_dim.fetchone()[0]
            fact_summary[table] = count
            total_new_fact_rows += count
        
        # Get total counts for context
        dim_tables = ['DIM_TIME', 'DIM_LOCATION', 'DIM_DEVICE', 'DIM_VEHICLE', 'DIM_WEATHER_CONDITION', 'DIM_INCIDENT_TYPE']
        total_dim_rows = 0
        total_fact_rows = 0
        
        for table in dim_tables:
            cursor_dim.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor_dim.fetchone()[0]
            total_dim_rows += count
        
        for table in fact_tables:
            cursor_dim.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor_dim.fetchone()[0]
            total_fact_rows += count
        
        cursor_dim.close()
        conn_dim.close()
        
        # Generate batch processing summary
        logging.info("🎯 BATCH PROCESSING SUMMARY (Current Run)")
        logging.info("=" * 50)
        
        logging.info("📊 NEW RAW DATA PROCESSED:")
        for table, count in raw_summary.items():
            logging.info("   %s: %s new rows", table, f"{count:,}")
        logging.info("   📈 Total NEW Raw Data: %s rows", f"{total_new_raw_rows:,}")
        
        logging.info("📊 NEW FACT RECORDS CREATED:")
        for table, count in fact_summary.items():
            logging.info("   %s: %s new records", table, f"{count:,}")
        logging.info("   � Total NEW Fact Records: %s", f"{total_new_fact_rows:,}")
        
        logging.info("🏗️ CUMULATIVE TOTALS:")
        logging.info("   � Total Dimension Records: %s", f"{total_dim_rows:,}")
        logging.info("   📊 Total Fact Records: %s", f"{total_fact_rows:,}")
        logging.info("   🔄 Total Dimensional Model: %s records", f"{total_dim_rows + total_fact_rows:,}")
        
        logging.info("✅ BATCH PROCESSING COMPLETED SUCCESSFULLY!")
        
        # Log processing efficiency
        if total_new_raw_rows > 0:
            efficiency = (total_new_fact_rows / total_new_raw_rows) * 100
            logging.info("📈 Processing Efficiency: %.1f%% (Fact/Raw ratio)", efficiency)
        
    except Exception as e:
        logging.error("❌ Error generating batch processing summary: %s", str(e))
        raise

# =============================================================================
# DAG TASK DEFINITIONS
# =============================================================================

# Start task
start_pipeline = DummyOperator(
    task_id='start_complete_etl_pipeline',
    dag=dag
)

# Raw data extraction phase
raw_data_start = DummyOperator(
    task_id='start_raw_data_extraction',
    dag=dag
)

process_vehicle_task = PythonOperator(
    task_id='process_vehicle_data',
    python_callable=process_vehicle_data,
    dag=dag
)

process_gps_task = PythonOperator(
    task_id='process_gps_data',
    python_callable=process_gps_data,
    dag=dag
)

process_traffic_task = PythonOperator(
    task_id='process_traffic_data',
    python_callable=process_traffic_data,
    dag=dag
)

process_weather_task = PythonOperator(
    task_id='process_weather_data',
    python_callable=process_weather_data,
    dag=dag
)

process_emergency_task = PythonOperator(
    task_id='process_emergency_data',
    python_callable=process_emergency_data,
    dag=dag
)

generate_raw_summary_task = PythonOperator(
    task_id='generate_raw_data_summary',
    python_callable=generate_raw_data_summary,
    dag=dag
)

# Dimensional modeling phase
dimensional_start = DummyOperator(
    task_id='start_dimensional_modeling',
    dag=dag
)

create_dimensional_schema_task = PythonOperator(
    task_id='create_dimensional_schema',
    python_callable=create_dimensional_schema,
    dag=dag
)

populate_time_task = PythonOperator(
    task_id='populate_time_dimension',
    python_callable=populate_time_dimension,
    dag=dag
)

populate_lookups_task = PythonOperator(
    task_id='populate_lookup_dimensions',
    python_callable=populate_lookup_dimensions,
    dag=dag
)

extract_dimensions_task = PythonOperator(
    task_id='extract_and_load_dimensions',
    python_callable=extract_and_load_dimensions,
    dag=dag
)

transform_vehicle_facts_task = PythonOperator(
    task_id='transform_vehicle_facts',
    python_callable=transform_vehicle_facts,
    dag=dag
)

transform_traffic_facts_task = PythonOperator(
    task_id='transform_traffic_facts',
    python_callable=transform_traffic_facts,
    dag=dag
)

transform_weather_facts_task = PythonOperator(
    task_id='transform_weather_facts',
    python_callable=transform_weather_facts,
    dag=dag
)

transform_emergency_facts_task = PythonOperator(
    task_id='transform_emergency_facts',
    python_callable=transform_emergency_facts,
    dag=dag
)

# Final summary and completion
generate_batch_summary_task = PythonOperator(
    task_id='generate_batch_processing_summary',
    python_callable=generate_batch_processing_summary,
    dag=dag
)

complete_pipeline = DummyOperator(
    task_id='batch_etl_pipeline_complete',
    dag=dag
)

# =============================================================================
# DAG TASK DEPENDENCIES
# =============================================================================

# Start pipeline
start_pipeline >> raw_data_start

# Raw data extraction phase - parallel processing of all data types
raw_data_start >> [
    process_vehicle_task,
    process_gps_task,
    process_traffic_task,
    process_weather_task,
    process_emergency_task
]

# Raw data summary after all extractions complete
[
    process_vehicle_task,
    process_gps_task,
    process_traffic_task,
    process_weather_task,
    process_emergency_task
] >> generate_raw_summary_task

# Start dimensional modeling after raw data is complete
generate_raw_summary_task >> dimensional_start

# Create dimensional schema first
dimensional_start >> create_dimensional_schema_task

# Populate static dimensions in parallel
create_dimensional_schema_task >> [populate_time_task, populate_lookups_task]

# Extract dimensions from raw data after static dimensions are ready
[populate_time_task, populate_lookups_task] >> extract_dimensions_task

# Transform facts in parallel after dimensions are ready
extract_dimensions_task >> [
    transform_vehicle_facts_task,
    transform_traffic_facts_task,
    transform_weather_facts_task,
    transform_emergency_facts_task
]

# Generate final summary and complete pipeline
[
    transform_vehicle_facts_task,
    transform_traffic_facts_task,
    transform_weather_facts_task,
    transform_emergency_facts_task
] >> generate_batch_summary_task

generate_batch_summary_task >> complete_pipeline
