"""
Redis Test Data Populator
Populates Redis with sample smart city data for API testing
"""

import redis
import json
import random
from datetime import datetime, timedelta
import time

class RedisDataPopulator:
    def __init__(self, host='localhost', port=6379):
        self.redis_client = redis.Redis(
            host=host, 
            port=port, 
            decode_responses=True
        )
        
    def test_connection(self):
        """Test Redis connection"""
        try:
            self.redis_client.ping()
            print("✅ Redis connection successful")
            return True
        except Exception as e:
            print(f"❌ Redis connection failed: {e}")
            return False
    
    def populate_vehicle_data(self, num_vehicles=20):
        """Populate vehicle location data"""
        print(f"📍 Populating {num_vehicles} vehicle locations...")
        
        # Smart city area bounds (example: San Francisco-like area)
        lat_bounds = (37.7000, 37.8000)
        lon_bounds = (-122.5000, -122.4000)
        
        for i in range(num_vehicles):
            vehicle_id = f"vehicle_{i+1:03d}"
            
            vehicle_data = {
                "vehicle_id": vehicle_id,
                "latitude": round(random.uniform(*lat_bounds), 6),
                "longitude": round(random.uniform(*lon_bounds), 6),
                "speed": round(random.uniform(0, 80), 1),
                "direction": round(random.uniform(0, 360), 1),
                "timestamp": datetime.now().isoformat()
            }
            
            # Store with TTL (10 minutes)
            key = f"vehicle:location:{vehicle_id}"
            self.redis_client.setex(key, 600, json.dumps(vehicle_data))
        
        print(f"✅ Added {num_vehicles} vehicle locations")
    
    def populate_traffic_data(self, num_roads=10):
        """Populate traffic condition data"""
        print(f"🚦 Populating {num_roads} traffic conditions...")
        
        road_names = [
            "highway_101", "interstate_280", "broadway_st", "market_st", "mission_st",
            "van_ness_ave", "lombard_st", "california_st", "geary_blvd", "19th_ave"
        ]
        
        congestion_levels = ["light", "moderate", "heavy", "severe"]
        
        for i, road_name in enumerate(road_names[:num_roads]):
            vehicle_count = random.randint(10, 200)
            congestion = random.choice(congestion_levels)
            
            # Speed varies by congestion
            speed_map = {"light": (45, 65), "moderate": (25, 45), "heavy": (10, 25), "severe": (0, 10)}
            speed_range = speed_map[congestion]
            
            traffic_data = {
                "road_id": road_name,
                "average_speed": round(random.uniform(*speed_range), 1),
                "vehicle_count": vehicle_count,
                "congestion_level": congestion,
                "timestamp": datetime.now().isoformat()
            }
            
            # Store with TTL (5 minutes)
            key = f"traffic:road:{road_name}"
            self.redis_client.setex(key, 300, json.dumps(traffic_data))
        
        print(f"✅ Added {num_roads} traffic conditions")
    
    def populate_emergency_data(self, num_emergencies=5):
        """Populate emergency incident data"""
        print(f"🚨 Populating {num_emergencies} emergency incidents...")
        
        incident_types = ["accident", "fire", "medical", "hazmat", "police"]
        severities = ["low", "medium", "high", "critical"]
        statuses = ["active", "responding", "resolved"]
        
        # Smart city area bounds
        lat_bounds = (37.7000, 37.8000)
        lon_bounds = (-122.5000, -122.4000)
        
        for i in range(num_emergencies):
            incident_id = f"emergency_{int(time.time())}_{i}"
            
            emergency_data = {
                "incident_id": incident_id,
                "incident_type": random.choice(incident_types),
                "latitude": round(random.uniform(*lat_bounds), 6),
                "longitude": round(random.uniform(*lon_bounds), 6),
                "severity": random.choice(severities),
                "status": random.choice(statuses),
                "timestamp": (datetime.now() - timedelta(minutes=random.randint(0, 120))).isoformat(),
                "description": f"Emergency incident {i+1}"
            }
            
            # Store with TTL (2 hours)
            key = f"emergency:incident:{incident_id}"
            self.redis_client.setex(key, 7200, json.dumps(emergency_data))
        
        print(f"✅ Added {num_emergencies} emergency incidents")
    
    def populate_city_analytics(self):
        """Populate city-wide analytics summary"""
        print("📊 Populating city analytics...")
        
        analytics_data = {
            "total_vehicles": len(self.redis_client.keys("vehicle:location:*")),
            "active_emergencies": len([k for k in self.redis_client.keys("emergency:incident:*") 
                                     if json.loads(self.redis_client.get(k) or '{}').get('status') == 'active']),
            "average_city_speed": round(random.uniform(25, 45), 1),
            "congested_roads": random.randint(2, 8),
            "timestamp": datetime.now().isoformat()
        }
        
        # Store with TTL (1 minute - frequently updated)
        key = "analytics:city:summary"
        self.redis_client.setex(key, 60, json.dumps(analytics_data))
        
        print("✅ Added city analytics summary")
    
    def populate_weather_data(self):
        """Populate weather data"""
        print("🌤️ Populating weather data...")
        
        weather_conditions = ["sunny", "cloudy", "rainy", "foggy", "windy"]
        
        weather_data = {
            "temperature": round(random.uniform(10, 30), 1),
            "humidity": round(random.uniform(30, 90), 1),
            "condition": random.choice(weather_conditions),
            "wind_speed": round(random.uniform(0, 25), 1),
            "visibility": round(random.uniform(1, 10), 1),
            "timestamp": datetime.now().isoformat()
        }
        
        # Store with TTL (30 minutes)
        key = "weather:current"
        self.redis_client.setex(key, 1800, json.dumps(weather_data))
        
        print("✅ Added weather data")
    
    def add_health_check_data(self):
        """Add health check data"""
        health_data = {
            "status": "healthy",
            "timestamp": datetime.now().isoformat()
        }
        
        self.redis_client.setex("health:check", 300, json.dumps(health_data))
    
    def populate_all_data(self):
        """Populate all types of test data"""
        if not self.test_connection():
            return False
        
        print("🚀 Populating Smart City test data in Redis...")
        print("=" * 50)
        
        try:
            self.populate_vehicle_data(25)
            self.populate_traffic_data(12)
            self.populate_emergency_data(6)
            self.populate_weather_data()
            self.populate_city_analytics()
            self.add_health_check_data()
            
            print("\n" + "=" * 50)
            print("🎉 Successfully populated all test data!")
            
            # Show summary
            self.show_data_summary()
            
            return True
            
        except Exception as e:
            print(f"❌ Error populating data: {e}")
            return False
    
    def show_data_summary(self):
        """Show summary of populated data"""
        print("\n📊 Data Summary:")
        
        data_types = [
            ("vehicle:location:*", "Vehicle Locations"),
            ("traffic:road:*", "Traffic Conditions"),
            ("emergency:incident:*", "Emergency Incidents"),
            ("weather:*", "Weather Data"),
            ("analytics:*", "Analytics Data"),
            ("health:*", "Health Check Data")
        ]
        
        for pattern, description in data_types:
            count = len(self.redis_client.keys(pattern))
            print(f"   {description}: {count}")
        
        total_keys = len(self.redis_client.keys("*"))
        print(f"   Total Redis Keys: {total_keys}")
    
    def clear_all_data(self):
        """Clear all test data from Redis"""
        print("🧹 Clearing all test data...")
        
        patterns = [
            "vehicle:*", "traffic:*", "emergency:*", 
            "weather:*", "analytics:*", "health:*"
        ]
        
        deleted_count = 0
        for pattern in patterns:
            keys = self.redis_client.keys(pattern)
            if keys:
                deleted_count += self.redis_client.delete(*keys)
        
        print(f"✅ Deleted {deleted_count} keys")

if __name__ == "__main__":
    import sys
    
    # Default to localhost, but allow Docker hostname
    host = "localhost"
    if len(sys.argv) > 1 and sys.argv[1] == "--docker":
        host = "redis"
    
    populator = RedisDataPopulator(host=host)
    
    if len(sys.argv) > 1 and sys.argv[1] == "--clear":
        populator.clear_all_data()
    else:
        populator.populate_all_data()
