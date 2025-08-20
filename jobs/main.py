import os
from confluent_kafka import SerializingProducer
from datetime import datetime
from random import uniform, randint, choice  # <-- import uniform, randint, and choice
import uuid
import time
from datetime import timedelta
import json


LONDON_CORDINATES = {"latitude": 51.5874, "longitude": -0.1278}
BIRMINGHAM_CORDINATES = {"latitude": 52.4862, "longitude": -1.8904}

# Reduce increments for a longer journey
LATITUDE_INCREAMENT = (BIRMINGHAM_CORDINATES["latitude"] - LONDON_CORDINATES["latitude"]) / 50
LONGITUDE_INCREAMENT = (BIRMINGHAM_CORDINATES["longitude"] - LONDON_CORDINATES["longitude"]) / 50

#ENVIRONMENT VARIABLES
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
VEHICLE_TOPIC = os.getenv("VEHICLE_TOPIC", "vehicle_data")
GPS_TOPIC = os.getenv("GPS_TOPIC", "gps_data")
TRAFFIC_TOPIC = os.getenv("TRAFFIC_TOPIC", "traffic_data")
WEATHER_TOPIC = os.getenv("WEATHER_TOPIC", "weather_data")
EMERGENCY_TOPIC = os.getenv("EMERGENCY_TOPIC", "emergency_data")
TRAFFIC_CAMERA_TOPIC = os.getenv("TRAFFIC_CAMERA_TOPIC", "traffic_camera_data")  # add this
EMERGENCY_INCIDENT_TOPIC = os.getenv("EMERGENCY_INCIDENT_TOPIC", "emergency_incident_data")  # add this

start_time = datetime.now()
start_location = LONDON_CORDINATES.copy()


def get_next_time():
    global start_time
    start_time += timedelta(seconds=randint(30, 60))  # use randint
    return start_time

def generate_gps_data(device_id, timestamp, vehicle_type='private'):
    return {
        "id": uuid.uuid4(),
        "device_id": device_id,
        "timestamp": timestamp,
        "speed": uniform(0, 40),  # use uniform
        "location": (start_location["latitude"], start_location["longitude"]),
        "direction": "North-East",
        "vehicle_type": vehicle_type
    }

def generate_traffic_camera_data(device_id, timestamp, location, camera_id):
    return {
        "id": uuid.uuid4(),
        "device_id": device_id,
        "camera_id": camera_id,
        "timestamp": timestamp,
        'snapshot': 'Base64EncodedString',
        "location": location,
        "direction": "North-East",
        "speed": uniform(0, 40),  # use uniform
    }

def generate_weather_data(device_id, timestamp, location):
    return {
        "id": uuid.uuid4(),
        "device_id": device_id,
        "timestamp": timestamp,
        "location": location,
        "temperature": uniform(-5, 35),  # use uniform
        "weather_condition": choice(["Clear", "Rain", "Snow", "Fog"]),
        "humidity": uniform(0, 100),  # use uniform
        "wind_speed": uniform(0, 100),  # use uniform
        "precipitation": uniform(0, 100),  # use uniform
        "airQualityIndex": uniform(0, 500),  # use uniform
    }

def generate_emergency_incident_data(device_id, timestamp, location):
    return {
        "id": uuid.uuid4(),
        "device_id": device_id,
        "incidentId": uuid.uuid4(),
        "timestamp": timestamp,
        "location": location,
        "incident_type": choice(["Accident", "Breakdown", "Theft"]),
        "status": choice(["Active", "Resolved"]),
        "description": "Emergency incident reported"

    }

def simulate_vehicle_movement():
    # Simulate vehicle movement by incrementing the location
    global start_location
    start_location["latitude"] += LATITUDE_INCREAMENT
    start_location["longitude"] += LONGITUDE_INCREAMENT

    start_location["latitude"] += uniform(-0.0005, 0.0005)  # use uniform
    start_location["longitude"] += uniform(-0.0005, 0.0005)  # use uniform

    return start_location

def generate_vehicle_data(device_id):

    location = simulate_vehicle_movement()
    return {
        "id": uuid.uuid4(),
        'device_id': device_id,
        "timestamp": get_next_time().isoformat(),
        "location": (location["latitude"], location["longitude"]),
        "speed": uniform(10, 40),  # use uniform
        "direction": "North-East",
        "make": "Toyota",
        "model": "Camry",
        "year": 2024,
        "fuel_type": "Hybrid"
    }

def json_serialzer(obj):
    if isinstance(obj, uuid.UUID):
        return str(obj)  # fix: return string value of UUID
    raise TypeError(f"Type {type(obj)} not serializable")

def delivery_report(err, msg):
    if err:
        print(f"Error producing message to Kafka: {err}")
    else:
        print(f"Produced message to : {msg.topic()} {msg.partition()}")

def produce_data_to_kafka(producer, topic, data):
    producer.produce(
        topic,
        key=str(data['id']),
        value=json.dumps(data, default=json_serialzer).encode('utf-8'),
        on_delivery=delivery_report
    )

def simulate_journey(producer, device_id):
    while True:
        vehicle_data = generate_vehicle_data(device_id)
        gps_data = generate_gps_data(device_id, vehicle_data['timestamp'])
        traffic_camera_data = generate_traffic_camera_data(device_id, vehicle_data['timestamp'], vehicle_data['location'], 'Camera331')
        weather_data = generate_weather_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])
        emergency_incident_data = generate_emergency_incident_data(device_id, vehicle_data['timestamp'], vehicle_data['location'])  

        if (vehicle_data["location"][0] >= BIRMINGHAM_CORDINATES['latitude']) and (vehicle_data["location"][1] <= BIRMINGHAM_CORDINATES['longitude']):
            print("Vehicle has entered Birmingham")
            break

        # Produce messages to Kafka topics
        produce_data_to_kafka(producer, VEHICLE_TOPIC, vehicle_data)
        produce_data_to_kafka(producer, GPS_TOPIC, gps_data)
        produce_data_to_kafka(producer, TRAFFIC_CAMERA_TOPIC, traffic_camera_data)
        produce_data_to_kafka(producer, WEATHER_TOPIC, weather_data)
        produce_data_to_kafka(producer, EMERGENCY_INCIDENT_TOPIC, emergency_incident_data)
        
        # Flush once per iteration instead of per message
        producer.flush()
        
        time.sleep(5)


if __name__ == "__main__":
    producer = SerializingProducer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "error_cb": lambda err: print(f"Kafka error: {err}"),
    })

    try:
        simulate_journey(producer, 'Vehicle1')
    except KeyboardInterrupt:
        print("Journey simulation interrupted by user")
    except Exception as e:
        print(f"Error occurred: {e}")