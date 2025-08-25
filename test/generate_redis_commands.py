import json

# Test data for Redis
test_data = [
    ("vehicle:location:vehicle_001", {
        "vehicle_id": "vehicle_001",
        "latitude": 37.7749,
        "longitude": -122.4194,
        "speed": 45.5,
        "direction": 180,
        "timestamp": "2025-08-25T12:35:00"
    }),
    ("vehicle:location:vehicle_002", {
        "vehicle_id": "vehicle_002", 
        "latitude": 37.7849,
        "longitude": -122.4094,
        "speed": 30.2,
        "direction": 90,
        "timestamp": "2025-08-25T12:35:00"
    }),
    ("emergency:incident:emerg_001", {
        "incident_id": "emerg_001",
        "incident_type": "accident",
        "latitude": 37.7649,
        "longitude": -122.4394,
        "severity": "high",
        "status": "active",
        "timestamp": "2025-08-25T12:30:00"
    }),
    ("traffic:road:highway_101", {
        "road_id": "highway_101",
        "average_speed": 35.8,
        "vehicle_count": 45,
        "congestion_level": "moderate",
        "timestamp": "2025-08-25T12:35:00"
    }),
    ("analytics:city:summary", {
        "total_vehicles": 15,
        "active_emergencies": 2,
        "average_city_speed": 42.3,
        "congested_roads": 3,
        "timestamp": "2025-08-25T12:35:00"
    })
]

# Generate Redis CLI commands
print("# Redis CLI commands to populate test data")
for key, data in test_data:
    json_str = json.dumps(data)
    print(f'docker exec redis redis-cli SET "{key}" \'{json_str}\'')
