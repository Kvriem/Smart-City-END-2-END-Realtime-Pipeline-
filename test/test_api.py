"""
API Testing Script for Smart City Real-Time API
Tests all endpoints and WebSocket connectivity
"""

import requests
import asyncio
import websockets
import json
import time
from datetime import datetime

API_BASE_URL = "http://localhost:8000"
WS_URL = "ws://localhost:8000/ws/realtime"

class SmartCityAPITester:
    def __init__(self):
        self.session = requests.Session()
        self.session.timeout = 10
    
    def test_health_endpoint(self):
        """Test basic health check"""
        print("🔍 Testing health endpoint...")
        try:
            response = self.session.get(f"{API_BASE_URL}/")
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Health check passed: {data['status']}")
                print(f"   Redis healthy: {data.get('redis_healthy', 'unknown')}")
                return True
            else:
                print(f"❌ Health check failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ Health check error: {e}")
            return False
    
    def test_detailed_health(self):
        """Test detailed health endpoint"""
        print("\n🔍 Testing detailed health endpoint...")
        try:
            response = self.session.get(f"{API_BASE_URL}/api/health")
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Detailed health check: {data['status']}")
                
                # Show component status
                components = data.get('components', {})
                for component, status in components.items():
                    print(f"   {component}: {status}")
                return True
            else:
                print(f"❌ Detailed health check failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ Detailed health check error: {e}")
            return False
    
    def test_vehicle_locations(self):
        """Test vehicle locations endpoint"""
        print("\n🚗 Testing vehicle locations endpoint...")
        try:
            response = self.session.get(f"{API_BASE_URL}/api/vehicles/locations")
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Vehicle locations endpoint working")
                print(f"   Found {len(data)} vehicles")
                
                if data:
                    # Show first vehicle as example
                    vehicle = data[0]
                    print(f"   Example vehicle: {vehicle.get('vehicle_id')} at ({vehicle.get('latitude')}, {vehicle.get('longitude')})")
                return True
            else:
                print(f"❌ Vehicle locations failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ Vehicle locations error: {e}")
            return False
    
    def test_traffic_conditions(self):
        """Test traffic conditions endpoint"""
        print("\n🚦 Testing traffic conditions endpoint...")
        # Test with a sample road ID
        road_id = "highway_101"
        try:
            response = self.session.get(f"{API_BASE_URL}/api/traffic/live/{road_id}")
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Traffic conditions endpoint working")
                print(f"   Road {road_id}: {data.get('congestion_level')} congestion")
                print(f"   Average speed: {data.get('average_speed')} km/h")
                return True
            elif response.status_code == 404:
                print(f"✅ Traffic endpoint working (no data for {road_id} - expected)")
                return True
            else:
                print(f"❌ Traffic conditions failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ Traffic conditions error: {e}")
            return False
    
    def test_emergency_incidents(self):
        """Test emergency incidents endpoint"""
        print("\n🚨 Testing emergency incidents endpoint...")
        try:
            response = self.session.get(f"{API_BASE_URL}/api/emergencies/active")
            if response.status_code == 200:
                data = response.json()
                print(f"✅ Emergency incidents endpoint working")
                print(f"   Found {len(data)} active emergencies")
                
                if data:
                    # Show first emergency as example
                    emergency = data[0]
                    print(f"   Example emergency: {emergency.get('incident_type')} - {emergency.get('severity')}")
                return True
            else:
                print(f"❌ Emergency incidents failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ Emergency incidents error: {e}")
            return False
    
    def test_city_analytics(self):
        """Test city analytics endpoint"""
        print("\n📊 Testing city analytics endpoint...")
        try:
            response = self.session.get(f"{API_BASE_URL}/api/analytics/city-summary")
            if response.status_code == 200:
                data = response.json()
                print(f"✅ City analytics endpoint working")
                print(f"   Total vehicles: {data.get('total_vehicles')}")
                print(f"   Active emergencies: {data.get('active_emergencies')}")
                print(f"   Average city speed: {data.get('average_city_speed')} km/h")
                return True
            else:
                print(f"❌ City analytics failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ City analytics error: {e}")
            return False
    
    def test_api_stats(self):
        """Test API statistics endpoint"""
        print("\n📈 Testing API statistics endpoint...")
        try:
            response = self.session.get(f"{API_BASE_URL}/api/stats")
            if response.status_code == 200:
                data = response.json()
                print(f"✅ API statistics endpoint working")
                
                # Show data points
                data_points = data.get('data_points', {})
                for data_type, count in data_points.items():
                    print(f"   {data_type}: {count}")
                
                # Show API status
                api_status = data.get('api_status', {})
                print(f"   WebSocket connections: {api_status.get('websocket_connections', 0)}")
                print(f"   Redis healthy: {api_status.get('redis_healthy', 'unknown')}")
                return True
            else:
                print(f"❌ API statistics failed: {response.status_code}")
                return False
        except Exception as e:
            print(f"❌ API statistics error: {e}")
            return False
    
    async def test_websocket(self):
        """Test WebSocket real-time connection"""
        print("\n🔌 Testing WebSocket connection...")
        try:
            async with websockets.connect(WS_URL) as websocket:
                print("✅ WebSocket connected successfully")
                
                # Wait for a few messages
                messages_received = 0
                start_time = time.time()
                
                while messages_received < 3 and time.time() - start_time < 30:
                    try:
                        message = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                        data = json.loads(message)
                        messages_received += 1
                        
                        print(f"📦 Received message {messages_received}: {data.get('type', 'unknown')}")
                        if data.get('type') == 'city_analytics':
                            analytics = data.get('data', {})
                            print(f"   Vehicles: {analytics.get('total_vehicles')}, Emergencies: {analytics.get('active_emergencies')}")
                    
                    except asyncio.TimeoutError:
                        print("⏰ Timeout waiting for WebSocket message")
                        break
                
                if messages_received > 0:
                    print(f"✅ WebSocket working - received {messages_received} messages")
                    return True
                else:
                    print("⚠️ WebSocket connected but no messages received")
                    return False
                    
        except Exception as e:
            print(f"❌ WebSocket test error: {e}")
            return False
    
    def run_all_tests(self):
        """Run all API tests"""
        print("🚀 Starting Smart City API Tests")
        print("=" * 50)
        
        tests = [
            self.test_health_endpoint,
            self.test_detailed_health,
            self.test_vehicle_locations,
            self.test_traffic_conditions,
            self.test_emergency_incidents,
            self.test_city_analytics,
            self.test_api_stats,
        ]
        
        passed = 0
        total = len(tests)
        
        for test in tests:
            if test():
                passed += 1
            time.sleep(1)  # Brief pause between tests
        
        print("\n" + "=" * 50)
        print(f"📊 REST API Tests: {passed}/{total} passed")
        
        # Test WebSocket separately (async)
        print("\n🔌 Testing WebSocket...")
        try:
            websocket_result = asyncio.run(self.test_websocket())
            if websocket_result:
                passed += 1
            total += 1
        except Exception as e:
            print(f"❌ WebSocket test failed: {e}")
            total += 1
        
        print("\n" + "=" * 50)
        print(f"🎯 Total Tests: {passed}/{total} passed")
        
        if passed == total:
            print("🎉 All tests passed! API is fully operational.")
        elif passed >= total * 0.8:
            print("✅ Most tests passed. API is mostly operational.")
        else:
            print("⚠️ Several tests failed. Please check API status.")
        
        return passed, total

if __name__ == "__main__":
    tester = SmartCityAPITester()
    tester.run_all_tests()
