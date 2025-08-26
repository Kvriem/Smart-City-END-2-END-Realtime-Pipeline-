from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
import snowflake.connector
import pandas as pd
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
    'star_schema_transformation',
    default_args=default_args,
    description='Transform raw Smart City data into star schema dimensional model',
    schedule_interval=timedelta(hours=6),  # Run every 6 hours
    catchup=False,
    tags=['smart-city', 'star-schema', 'dimensional-modeling', 'analytics']
)

def get_snowflake_connection():
    """Create Snowflake connection using environment variables"""
    return snowflake.connector.connect(
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
        database=os.getenv('SNOWFLAKE_DATABASE', 'SMART_CITY_DW'),
        schema='DIMENSIONAL_MODEL'
    )

def create_dimensional_schema():
    """Create the dimensional schema for star schema tables"""
    logging.info("🏗️ Creating dimensional schema and tables")
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        # Create dimensional schema
        cursor.execute("CREATE SCHEMA IF NOT EXISTS DIMENSIONAL_MODEL")
        cursor.execute("USE SCHEMA DIMENSIONAL_MODEL")
        logging.info("✅ Dimensional schema created/verified")
        
        # Create dimension tables
        dimension_tables = {
            'DIM_TIME': """
                CREATE TABLE IF NOT EXISTS DIM_TIME (
                    TIME_KEY INTEGER PRIMARY KEY,
                    TIMESTAMP_VALUE TIMESTAMP_NTZ,
                    DATE_VALUE DATE,
                    YEAR INTEGER,
                    QUARTER INTEGER,
                    MONTH INTEGER,
                    MONTH_NAME STRING,
                    DAY INTEGER,
                    DAY_OF_WEEK INTEGER,
                    DAY_NAME STRING,
                    HOUR INTEGER,
                    MINUTE INTEGER,
                    IS_WEEKEND BOOLEAN,
                    IS_HOLIDAY BOOLEAN,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """,
            
            'DIM_LOCATION': """
                CREATE TABLE IF NOT EXISTS DIM_LOCATION (
                    LOCATION_KEY INTEGER AUTOINCREMENT PRIMARY KEY,
                    LATITUDE FLOAT,
                    LONGITUDE FLOAT,
                    LOCATION_HASH STRING UNIQUE,
                    ZONE_ID STRING,
                    DISTRICT STRING,
                    CITY STRING DEFAULT 'Smart City',
                    COUNTRY STRING DEFAULT 'Unknown',
                    LOCATION_TYPE STRING, -- 'INTERSECTION', 'HIGHWAY', 'RESIDENTIAL', etc.
                    IS_MONITORED BOOLEAN DEFAULT TRUE,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """,
            
            'DIM_DEVICE': """
                CREATE TABLE IF NOT EXISTS DIM_DEVICE (
                    DEVICE_KEY INTEGER AUTOINCREMENT PRIMARY KEY,
                    DEVICE_ID STRING UNIQUE,
                    DEVICE_TYPE STRING, -- 'VEHICLE', 'CAMERA', 'WEATHER_STATION', 'EMERGENCY'
                    DEVICE_STATUS STRING DEFAULT 'ACTIVE',
                    INSTALLATION_DATE DATE,
                    LAST_MAINTENANCE_DATE DATE,
                    FIRMWARE_VERSION STRING,
                    IS_ACTIVE BOOLEAN DEFAULT TRUE,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """,
            
            'DIM_VEHICLE': """
                CREATE TABLE IF NOT EXISTS DIM_VEHICLE (
                    VEHICLE_KEY INTEGER AUTOINCREMENT PRIMARY KEY,
                    VEHICLE_ID STRING UNIQUE,
                    MAKE STRING,
                    MODEL STRING,
                    YEAR INTEGER,
                    FUEL_TYPE STRING,
                    VEHICLE_TYPE STRING,
                    ENGINE_SIZE STRING,
                    COLOR STRING,
                    IS_COMMERCIAL BOOLEAN,
                    IS_ACTIVE BOOLEAN DEFAULT TRUE,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """,
            
            'DIM_WEATHER_CONDITION': """
                CREATE TABLE IF NOT EXISTS DIM_WEATHER_CONDITION (
                    WEATHER_CONDITION_KEY INTEGER AUTOINCREMENT PRIMARY KEY,
                    CONDITION_CODE STRING UNIQUE,
                    CONDITION_NAME STRING,
                    CONDITION_CATEGORY STRING, -- 'CLEAR', 'CLOUDY', 'RAINY', 'STORMY', etc.
                    DESCRIPTION STRING,
                    SEVERITY_LEVEL INTEGER, -- 1-5 scale
                    IS_SEVERE BOOLEAN,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """,
            
            'DIM_INCIDENT_TYPE': """
                CREATE TABLE IF NOT EXISTS DIM_INCIDENT_TYPE (
                    INCIDENT_TYPE_KEY INTEGER AUTOINCREMENT PRIMARY KEY,
                    INCIDENT_CODE STRING UNIQUE,
                    INCIDENT_NAME STRING,
                    INCIDENT_CATEGORY STRING, -- 'ACCIDENT', 'MEDICAL', 'FIRE', 'SECURITY', etc.
                    PRIORITY_LEVEL INTEGER, -- 1-5 scale (1=Low, 5=Critical)
                    RESPONSE_TIME_MINUTES INTEGER, -- Expected response time
                    REQUIRES_POLICE BOOLEAN,
                    REQUIRES_MEDICAL BOOLEAN,
                    REQUIRES_FIRE BOOLEAN,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
                )
            """
        }
        
        for table_name, create_sql in dimension_tables.items():
            logging.info(f"🔧 Creating dimension table: {table_name}")
            cursor.execute(create_sql)
            logging.info(f"✅ Dimension table {table_name} created/verified")
        
        # Create fact tables
        fact_tables = {
            'FACT_VEHICLE_EVENTS': """
                CREATE TABLE IF NOT EXISTS FACT_VEHICLE_EVENTS (
                    EVENT_KEY INTEGER AUTOINCREMENT PRIMARY KEY,
                    TIME_KEY INTEGER,
                    LOCATION_KEY INTEGER,
                    DEVICE_KEY INTEGER,
                    VEHICLE_KEY INTEGER,
                    EVENT_ID STRING,
                    SPEED FLOAT,
                    DIRECTION STRING,
                    DISTANCE_TRAVELED FLOAT,
                    FUEL_CONSUMPTION FLOAT,
                    ENGINE_LOAD FLOAT,
                    IS_SPEEDING BOOLEAN,
                    IS_HARSH_BRAKING BOOLEAN,
                    IS_RAPID_ACCELERATION BOOLEAN,
                    EVENT_TIMESTAMP TIMESTAMP_NTZ,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    FOREIGN KEY (TIME_KEY) REFERENCES DIM_TIME(TIME_KEY),
                    FOREIGN KEY (LOCATION_KEY) REFERENCES DIM_LOCATION(LOCATION_KEY),
                    FOREIGN KEY (DEVICE_KEY) REFERENCES DIM_DEVICE(DEVICE_KEY),
                    FOREIGN KEY (VEHICLE_KEY) REFERENCES DIM_VEHICLE(VEHICLE_KEY)
                )
            """,
            
            'FACT_TRAFFIC_EVENTS': """
                CREATE TABLE IF NOT EXISTS FACT_TRAFFIC_EVENTS (
                    EVENT_KEY INTEGER AUTOINCREMENT PRIMARY KEY,
                    TIME_KEY INTEGER,
                    LOCATION_KEY INTEGER,
                    DEVICE_KEY INTEGER,
                    EVENT_ID STRING,
                    CAMERA_ID STRING,
                    TRAFFIC_SPEED FLOAT,
                    TRAFFIC_DIRECTION STRING,
                    VEHICLE_COUNT INTEGER,
                    CONGESTION_LEVEL INTEGER, -- 1-5 scale
                    AVERAGE_SPEED FLOAT,
                    TRAFFIC_FLOW_RATE FLOAT,
                    IS_CONGESTED BOOLEAN,
                    IS_ACCIDENT_DETECTED BOOLEAN,
                    EVENT_TIMESTAMP TIMESTAMP_NTZ,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    FOREIGN KEY (TIME_KEY) REFERENCES DIM_TIME(TIME_KEY),
                    FOREIGN KEY (LOCATION_KEY) REFERENCES DIM_LOCATION(LOCATION_KEY),
                    FOREIGN KEY (DEVICE_KEY) REFERENCES DIM_DEVICE(DEVICE_KEY)
                )
            """,
            
            'FACT_WEATHER_OBSERVATIONS': """
                CREATE TABLE IF NOT EXISTS FACT_WEATHER_OBSERVATIONS (
                    OBSERVATION_KEY INTEGER AUTOINCREMENT PRIMARY KEY,
                    TIME_KEY INTEGER,
                    LOCATION_KEY INTEGER,
                    DEVICE_KEY INTEGER,
                    WEATHER_CONDITION_KEY INTEGER,
                    OBSERVATION_ID STRING,
                    TEMPERATURE FLOAT,
                    HUMIDITY FLOAT,
                    WIND_SPEED FLOAT,
                    PRECIPITATION FLOAT,
                    AIR_QUALITY_INDEX FLOAT,
                    PRESSURE FLOAT,
                    VISIBILITY FLOAT,
                    UV_INDEX FLOAT,
                    HEAT_INDEX FLOAT,
                    WIND_CHILL FLOAT,
                    IS_EXTREME_WEATHER BOOLEAN,
                    IS_AIR_QUALITY_POOR BOOLEAN,
                    OBSERVATION_TIMESTAMP TIMESTAMP_NTZ,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    FOREIGN KEY (TIME_KEY) REFERENCES DIM_TIME(TIME_KEY),
                    FOREIGN KEY (LOCATION_KEY) REFERENCES DIM_LOCATION(LOCATION_KEY),
                    FOREIGN KEY (DEVICE_KEY) REFERENCES DIM_DEVICE(DEVICE_KEY),
                    FOREIGN KEY (WEATHER_CONDITION_KEY) REFERENCES DIM_WEATHER_CONDITION(WEATHER_CONDITION_KEY)
                )
            """,
            
            'FACT_EMERGENCY_INCIDENTS': """
                CREATE TABLE IF NOT EXISTS FACT_EMERGENCY_INCIDENTS (
                    INCIDENT_KEY INTEGER AUTOINCREMENT PRIMARY KEY,
                    TIME_KEY INTEGER,
                    LOCATION_KEY INTEGER,
                    DEVICE_KEY INTEGER,
                    INCIDENT_TYPE_KEY INTEGER,
                    INCIDENT_ID STRING,
                    INCIDENT_UNIQUE_ID STRING,
                    STATUS STRING,
                    DESCRIPTION STRING,
                    RESPONSE_TIME_MINUTES INTEGER,
                    RESOLUTION_TIME_MINUTES INTEGER,
                    SEVERITY_SCORE INTEGER, -- 1-10 scale
                    RESOURCES_DEPLOYED INTEGER,
                    IS_RESOLVED BOOLEAN,
                    IS_FALSE_ALARM BOOLEAN,
                    INCIDENT_TIMESTAMP TIMESTAMP_NTZ,
                    RESOLVED_TIMESTAMP TIMESTAMP_NTZ,
                    CREATED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
                    FOREIGN KEY (TIME_KEY) REFERENCES DIM_TIME(TIME_KEY),
                    FOREIGN KEY (LOCATION_KEY) REFERENCES DIM_LOCATION(LOCATION_KEY),
                    FOREIGN KEY (DEVICE_KEY) REFERENCES DIM_DEVICE(DEVICE_KEY),
                    FOREIGN KEY (INCIDENT_TYPE_KEY) REFERENCES DIM_INCIDENT_TYPE(INCIDENT_TYPE_KEY)
                )
            """
        }
        
        for table_name, create_sql in fact_tables.items():
            logging.info(f"🔧 Creating fact table: {table_name}")
            cursor.execute(create_sql)
            logging.info(f"✅ Fact table {table_name} created/verified")
        
        cursor.close()
        conn.close()
        logging.info("🎉 All dimensional tables created successfully!")
        
    except Exception as e:
        logging.error(f"❌ Error creating dimensional schema: {str(e)}")
        cursor.close()
        conn.close()
        raise

def populate_time_dimension():
    """Populate the time dimension with comprehensive time attributes"""
    logging.info("⏰ Populating time dimension")
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("USE SCHEMA DIMENSIONAL_MODEL")
        
        # Clear existing time dimension data
        cursor.execute("DELETE FROM DIM_TIME")
        
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
        logging.info(f"✅ Time dimension populated with {count} records")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"❌ Error populating time dimension: {str(e)}")
        cursor.close()
        conn.close()
        raise

def populate_lookup_dimensions():
    """Populate lookup dimensions with reference data"""
    logging.info("📚 Populating lookup dimensions")
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("USE SCHEMA DIMENSIONAL_MODEL")
        
        # Populate weather conditions
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
        
        cursor.execute("DELETE FROM DIM_WEATHER_CONDITION")
        for condition in weather_conditions:
            cursor.execute("""
                INSERT INTO DIM_WEATHER_CONDITION 
                (CONDITION_CODE, CONDITION_NAME, CONDITION_CATEGORY, DESCRIPTION, SEVERITY_LEVEL, IS_SEVERE)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, condition)
        
        # Populate incident types
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
        
        cursor.execute("DELETE FROM DIM_INCIDENT_TYPE")
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
        logging.error(f"❌ Error populating lookup dimensions: {str(e)}")
        cursor.close()
        conn.close()
        raise

def extract_and_load_dimensions():
    """Extract unique values from raw data and populate dimension tables"""
    logging.info("🔄 Extracting and loading dimensions from raw data")
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("USE SCHEMA DIMENSIONAL_MODEL")
        
        # Extract and load locations
        logging.info("📍 Processing location dimension...")
        location_sql = """
        INSERT INTO DIM_LOCATION (LATITUDE, LONGITUDE, LOCATION_HASH, ZONE_ID, DISTRICT, LOCATION_TYPE)
        WITH all_locations AS (
            SELECT DISTINCT 
                LOCATION[0]::FLOAT as lat,
                LOCATION[1]::FLOAT as lng,
                'VEHICLE' as type
            FROM RAW_DATA.VEHICLE_DATA 
            WHERE LOCATION IS NOT NULL
            
            UNION ALL
            
            SELECT DISTINCT 
                LOCATION[0]::FLOAT as lat,
                LOCATION[1]::FLOAT as lng,
                'GPS' as type
            FROM RAW_DATA.GPS_DATA 
            WHERE LOCATION IS NOT NULL
            
            UNION ALL
            
            SELECT DISTINCT 
                LOCATION[0]::FLOAT as lat,
                LOCATION[1]::FLOAT as lng,
                'TRAFFIC' as type
            FROM RAW_DATA.TRAFFIC_DATA 
            WHERE LOCATION IS NOT NULL
            
            UNION ALL
            
            SELECT DISTINCT 
                LOCATION[0]::FLOAT as lat,
                LOCATION[1]::FLOAT as lng,
                'WEATHER' as type
            FROM RAW_DATA.WEATHER_DATA 
            WHERE LOCATION IS NOT NULL
            
            UNION ALL
            
            SELECT DISTINCT 
                LOCATION[0]::FLOAT as lat,
                LOCATION[1]::FLOAT as lng,
                'EMERGENCY' as type
            FROM RAW_DATA.EMERGENCY_DATA 
            WHERE LOCATION IS NOT NULL
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
        WHERE NOT EXISTS (
            SELECT 1 FROM DIM_LOCATION dl 
            WHERE dl.LOCATION_HASH = unique_locations.location_hash
        )
        """
        
        cursor.execute(location_sql)
        
        # Extract and load devices
        logging.info("📱 Processing device dimension...")
        device_sql = """
        INSERT INTO DIM_DEVICE (DEVICE_ID, DEVICE_TYPE, DEVICE_STATUS, IS_ACTIVE)
        WITH all_devices AS (
            SELECT DISTINCT DEVICE_ID, 'VEHICLE_SENSOR' as device_type FROM RAW_DATA.VEHICLE_DATA WHERE DEVICE_ID IS NOT NULL
            UNION ALL
            SELECT DISTINCT DEVICE_ID, 'GPS_TRACKER' as device_type FROM RAW_DATA.GPS_DATA WHERE DEVICE_ID IS NOT NULL
            UNION ALL
            SELECT DISTINCT DEVICE_ID, 'TRAFFIC_CAMERA' as device_type FROM RAW_DATA.TRAFFIC_DATA WHERE DEVICE_ID IS NOT NULL
            UNION ALL
            SELECT DISTINCT DEVICE_ID, 'WEATHER_STATION' as device_type FROM RAW_DATA.WEATHER_DATA WHERE DEVICE_ID IS NOT NULL
            UNION ALL
            SELECT DISTINCT DEVICE_ID, 'EMERGENCY_SENSOR' as device_type FROM RAW_DATA.EMERGENCY_DATA WHERE DEVICE_ID IS NOT NULL
        )
        SELECT DEVICE_ID, device_type, 'ACTIVE', TRUE
        FROM all_devices
        WHERE NOT EXISTS (
            SELECT 1 FROM DIM_DEVICE dd WHERE dd.DEVICE_ID = all_devices.DEVICE_ID
        )
        """
        
        cursor.execute(device_sql)
        
        # Extract and load vehicles
        logging.info("🚗 Processing vehicle dimension...")
        vehicle_sql = """
        INSERT INTO DIM_VEHICLE (VEHICLE_ID, MAKE, MODEL, YEAR, FUEL_TYPE, VEHICLE_TYPE, IS_ACTIVE)
        WITH vehicle_data AS (
            SELECT DISTINCT 
                ID as vehicle_id,
                COALESCE(MAKE, 'Unknown') as make,
                COALESCE(MODEL, 'Unknown') as model,
                COALESCE(YEAR, 2020) as year,
                COALESCE(FUEL_TYPE, 'Gasoline') as fuel_type,
                'Standard' as vehicle_type
            FROM RAW_DATA.VEHICLE_DATA 
            WHERE ID IS NOT NULL
            
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
        )
        SELECT vehicle_id, make, model, year, fuel_type, vehicle_type, TRUE
        FROM vehicle_data
        WHERE NOT EXISTS (
            SELECT 1 FROM DIM_VEHICLE dv WHERE dv.VEHICLE_ID = vehicle_data.vehicle_id
        )
        """
        
        cursor.execute(vehicle_sql)
        
        # Get counts of loaded dimension records
        dimension_counts = {}
        for dim_table in ['DIM_LOCATION', 'DIM_DEVICE', 'DIM_VEHICLE']:
            cursor.execute(f"SELECT COUNT(*) FROM {dim_table}")
            dimension_counts[dim_table] = cursor.fetchone()[0]
        
        logging.info(f"✅ Dimensions loaded: {dimension_counts}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"❌ Error loading dimensions: {str(e)}")
        cursor.close()
        conn.close()
        raise

def transform_vehicle_facts():
    """Transform raw vehicle data into fact table"""
    logging.info("🚗 Transforming vehicle data into facts")
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("USE SCHEMA DIMENSIONAL_MODEL")
        cursor.execute("DELETE FROM FACT_VEHICLE_EVENTS")
        
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
        """
        
        cursor.execute(vehicle_facts_sql)
        
        cursor.execute("SELECT COUNT(*) FROM FACT_VEHICLE_EVENTS")
        count = cursor.fetchone()[0]
        logging.info(f"✅ Vehicle facts loaded: {count} records")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"❌ Error transforming vehicle facts: {str(e)}")
        cursor.close()
        conn.close()
        raise

def transform_traffic_facts():
    """Transform raw traffic data into fact table"""
    logging.info("🚦 Transforming traffic data into facts")
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("USE SCHEMA DIMENSIONAL_MODEL")
        cursor.execute("DELETE FROM FACT_TRAFFIC_EVENTS")
        
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
        """
        
        cursor.execute(traffic_facts_sql)
        
        cursor.execute("SELECT COUNT(*) FROM FACT_TRAFFIC_EVENTS")
        count = cursor.fetchone()[0]
        logging.info(f"✅ Traffic facts loaded: {count} records")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"❌ Error transforming traffic facts: {str(e)}")
        cursor.close()
        conn.close()
        raise

def transform_weather_facts():
    """Transform raw weather data into fact table"""
    logging.info("🌤️ Transforming weather data into facts")
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("USE SCHEMA DIMENSIONAL_MODEL")
        cursor.execute("DELETE FROM FACT_WEATHER_OBSERVATIONS")
        
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
        """
        
        cursor.execute(weather_facts_sql)
        
        cursor.execute("SELECT COUNT(*) FROM FACT_WEATHER_OBSERVATIONS")
        count = cursor.fetchone()[0]
        logging.info(f"✅ Weather facts loaded: {count} records")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"❌ Error transforming weather facts: {str(e)}")
        cursor.close()
        conn.close()
        raise

def transform_emergency_facts():
    """Transform raw emergency data into fact table"""
    logging.info("🚨 Transforming emergency data into facts")
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("USE SCHEMA DIMENSIONAL_MODEL")
        cursor.execute("DELETE FROM FACT_EMERGENCY_INCIDENTS")
        
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
        """
        
        cursor.execute(emergency_facts_sql)
        
        cursor.execute("SELECT COUNT(*) FROM FACT_EMERGENCY_INCIDENTS")
        count = cursor.fetchone()[0]
        logging.info(f"✅ Emergency facts loaded: {count} records")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"❌ Error transforming emergency facts: {str(e)}")
        cursor.close()
        conn.close()
        raise

def generate_star_schema_summary():
    """Generate summary of star schema transformation"""
    logging.info("📊 Generating star schema summary")
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        cursor.execute("USE SCHEMA DIMENSIONAL_MODEL")
        
        # Get table counts
        tables = [
            'DIM_TIME', 'DIM_LOCATION', 'DIM_DEVICE', 'DIM_VEHICLE', 
            'DIM_WEATHER_CONDITION', 'DIM_INCIDENT_TYPE',
            'FACT_VEHICLE_EVENTS', 'FACT_TRAFFIC_EVENTS', 
            'FACT_WEATHER_OBSERVATIONS', 'FACT_EMERGENCY_INCIDENTS'
        ]
        
        summary = {}
        total_facts = 0
        total_dimensions = 0
        
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            summary[table] = count
            
            if table.startswith('FACT_'):
                total_facts += count
            else:
                total_dimensions += count
        
        logging.info("📋 Star Schema Summary:")
        logging.info("🏗️ DIMENSION TABLES:")
        for table, count in summary.items():
            if table.startswith('DIM_'):
                logging.info(f"   {table}: {count:,} records")
        
        logging.info("📊 FACT TABLES:")
        for table, count in summary.items():
            if table.startswith('FACT_'):
                logging.info(f"   {table}: {count:,} records")
        
        logging.info(f"📈 TOTALS:")
        logging.info(f"   Total Dimension Records: {total_dimensions:,}")
        logging.info(f"   Total Fact Records: {total_facts:,}")
        logging.info(f"   Total Star Schema Records: {total_dimensions + total_facts:,}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        logging.error(f"❌ Error generating summary: {str(e)}")
        cursor.close()
        conn.close()
        raise

# Define DAG tasks
start_task = DummyOperator(
    task_id='start_star_schema_transformation',
    dag=dag
)

create_schema_task = PythonOperator(
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

transform_vehicle_task = PythonOperator(
    task_id='transform_vehicle_facts',
    python_callable=transform_vehicle_facts,
    dag=dag
)

transform_traffic_task = PythonOperator(
    task_id='transform_traffic_facts',
    python_callable=transform_traffic_facts,
    dag=dag
)

transform_weather_task = PythonOperator(
    task_id='transform_weather_facts',
    python_callable=transform_weather_facts,
    dag=dag
)

transform_emergency_task = PythonOperator(
    task_id='transform_emergency_facts',
    python_callable=transform_emergency_facts,
    dag=dag
)

generate_summary_task = PythonOperator(
    task_id='generate_star_schema_summary',
    python_callable=generate_star_schema_summary,
    dag=dag
)

complete_task = DummyOperator(
    task_id='star_schema_transformation_complete',
    dag=dag
)

# Define task dependencies
start_task >> create_schema_task

create_schema_task >> [populate_time_task, populate_lookups_task]

[populate_time_task, populate_lookups_task] >> extract_dimensions_task

extract_dimensions_task >> [
    transform_vehicle_task,
    transform_traffic_task, 
    transform_weather_task,
    transform_emergency_task
]

[
    transform_vehicle_task,
    transform_traffic_task,
    transform_weather_task,
    transform_emergency_task
] >> generate_summary_task

generate_summary_task >> complete_task
