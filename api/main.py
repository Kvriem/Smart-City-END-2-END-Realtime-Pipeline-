from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from typing import List, Dict, Optional, Any
import redis
import json
import asyncio
import logging
from datetime import datetime, timedelta
import uvicorn
from contextlib import asynccontextmanager

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Custom JSON encoder for datetime objects
class DateTimeEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

# Pydantic models for API responses
class VehicleLocation(BaseModel):
    vehicle_id: str
    latitude: float
    longitude: float
    timestamp: datetime
    speed: Optional[float] = None
    direction: Optional[float] = None

class TrafficCondition(BaseModel):
    road_id: str
    average_speed: float
    vehicle_count: int
    congestion_level: str
    timestamp: datetime

class EmergencyIncident(BaseModel):
    incident_id: str
    incident_type: str
    latitude: float
    longitude: float
    severity: str
    status: str
    timestamp: datetime

class CityAnalytics(BaseModel):
    total_vehicles: int
    active_emergencies: int
    average_city_speed: float
    congested_roads: int
    timestamp: datetime

# Redis connection manager
class RedisManager:
    def __init__(self):
        self.redis_client = None
        self.connection_healthy = False
    
    async def connect(self):
        """Connect to Redis with error handling"""
        try:
            self.redis_client = redis.Redis(
                host='redis', 
                port=6379, 
                decode_responses=True,
                socket_connect_timeout=5,
                socket_timeout=5
            )
            # Test connection
            await asyncio.get_event_loop().run_in_executor(None, self.redis_client.ping)
            self.connection_healthy = True
            logger.info("✅ Redis connection established")
            return True
        except Exception as e:
            logger.warning(f"⚠️ Redis connection failed: {e}")
            self.connection_healthy = False
            return False
    
    async def get_data(self, key: str) -> Optional[Any]:
        """Get data from Redis with error handling"""
        if not self.connection_healthy:
            return None
        
        try:
            data = await asyncio.get_event_loop().run_in_executor(
                None, self.redis_client.get, key
            )
            if data:
                return json.loads(data)
            return None
        except Exception as e:
            logger.error(f"Redis get error for key {key}: {e}")
            return None
    
    async def get_pattern_keys(self, pattern: str) -> List[str]:
        """Get keys matching pattern"""
        if not self.connection_healthy:
            return []
        
        try:
            keys = await asyncio.get_event_loop().run_in_executor(
                None, self.redis_client.keys, pattern
            )
            return keys or []
        except Exception as e:
            logger.error(f"Redis pattern search error for {pattern}: {e}")
            return []

# WebSocket connection manager
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info(f"WebSocket connected. Total connections: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
        logger.info(f"WebSocket disconnected. Total connections: {len(self.active_connections)}")

    async def broadcast(self, data: dict):
        """Broadcast data to all connected clients"""
        logger.info(f"🔕 Broadcast called but disabled for debugging")
        logger.info(f"🔍 Data that would be broadcast: {data}")
        return

# Global managers
redis_manager = RedisManager()
websocket_manager = ConnectionManager()

# App lifecycle management
@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("🚀 Starting Smart City API Layer...")
    await redis_manager.connect()
    
    # Start background task for real-time updates
    # task = asyncio.create_task(real_time_broadcaster())  # Temporarily disabled for debugging
    logger.info("📡 Real-time broadcaster temporarily disabled for debugging")
    
    yield
    
    # Shutdown
    logger.info("🔄 Shutting down Smart City API Layer...")
    # task.cancel()  # Commented out since task is not created

# Create FastAPI app
app = FastAPI(
    title="Smart City Real-Time API",
    description="Access real-time and historical smart city data",
    version="1.0.0",
    lifespan=lifespan
)

# Add CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Background task for real-time broadcasting
async def real_time_broadcaster():
    """Background task to broadcast real-time updates"""
    logger.info("🔕 Real-time broadcaster function disabled for debugging")
    return

# API Endpoints

@app.get("/")
async def root():
    """Health check endpoint"""
    return {
        "service": "Smart City Real-Time API",
        "status": "operational",
        "timestamp": datetime.now().isoformat(),
        "redis_healthy": redis_manager.connection_healthy
    }

@app.get("/api/vehicles/locations", response_model=List[VehicleLocation])
async def get_vehicle_locations(limit: int = 100) -> List[VehicleLocation]:
    """Get current vehicle locations from Redis speed layer"""
    try:
        # Get all vehicle location keys
        vehicle_keys = await redis_manager.get_pattern_keys("vehicle:location:*")
        
        if not vehicle_keys:
            return []
        
        # Limit results
        vehicle_keys = vehicle_keys[:limit]
        
        locations = []
        for key in vehicle_keys:
            data = await redis_manager.get_data(key)
            if data:
                try:
                    vehicle_id = key.split(":")[-1]
                    location = VehicleLocation(
                        vehicle_id=vehicle_id,
                        latitude=data.get("latitude", 0.0),
                        longitude=data.get("longitude", 0.0),
                        speed=data.get("speed"),
                        direction=data.get("direction"),
                        timestamp=datetime.fromisoformat(data.get("timestamp", datetime.now().isoformat()))
                    )
                    locations.append(location)
                except Exception as e:
                    logger.warning(f"Failed to parse vehicle data for {key}: {e}")
                    continue
        
        return locations
    
    except Exception as e:
        logger.error(f"Error fetching vehicle locations: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch vehicle locations")

@app.get("/api/traffic/live/{road_id}", response_model=TrafficCondition)
async def get_live_traffic(road_id: str) -> TrafficCondition:
    """Get live traffic conditions for a specific road"""
    try:
        traffic_data = await redis_manager.get_data(f"traffic:road:{road_id}")
        
        if not traffic_data:
            raise HTTPException(status_code=404, detail=f"No traffic data found for road {road_id}")
        
        return TrafficCondition(
            road_id=road_id,
            average_speed=traffic_data.get("average_speed", 0.0),
            vehicle_count=traffic_data.get("vehicle_count", 0),
            congestion_level=traffic_data.get("congestion_level", "unknown"),
            timestamp=datetime.fromisoformat(traffic_data.get("timestamp", datetime.now().isoformat()))
        )
    
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Error fetching traffic data for {road_id}: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch traffic data")

@app.get("/api/emergencies/active", response_model=List[EmergencyIncident])
async def get_active_emergencies() -> List[EmergencyIncident]:
    """Get all active emergency incidents"""
    try:
        emergency_keys = await redis_manager.get_pattern_keys("emergency:incident:*")
        
        if not emergency_keys:
            return []
        
        incidents = []
        for key in emergency_keys:
            data = await redis_manager.get_data(key)
            if data and data.get("status") == "active":
                try:
                    incident_id = key.split(":")[-1]
                    incident = EmergencyIncident(
                        incident_id=incident_id,
                        incident_type=data.get("incident_type", "unknown"),
                        latitude=data.get("latitude", 0.0),
                        longitude=data.get("longitude", 0.0),
                        severity=data.get("severity", "unknown"),
                        status=data.get("status", "unknown"),
                        timestamp=datetime.fromisoformat(data.get("timestamp", datetime.now().isoformat()))
                    )
                    incidents.append(incident)
                except Exception as e:
                    logger.warning(f"Failed to parse emergency data for {key}: {e}")
                    continue
        
        return incidents
    
    except Exception as e:
        logger.error(f"Error fetching emergency incidents: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch emergency incidents")

async def get_city_analytics() -> Optional[CityAnalytics]:
    """Helper function to get city-wide analytics"""
    try:
        # Get cached analytics or compute from current data
        cached_analytics = await redis_manager.get_data("analytics:city:summary")
        
        if cached_analytics:
            return CityAnalytics(
                total_vehicles=cached_analytics.get("total_vehicles", 0),
                active_emergencies=cached_analytics.get("active_emergencies", 0),
                average_city_speed=cached_analytics.get("average_city_speed", 0.0),
                congested_roads=cached_analytics.get("congested_roads", 0),
                timestamp=datetime.fromisoformat(cached_analytics.get("timestamp", datetime.now().isoformat()))
            )
        
        # Fallback: compute basic analytics from current data
        vehicle_keys = await redis_manager.get_pattern_keys("vehicle:location:*")
        emergency_keys = await redis_manager.get_pattern_keys("emergency:incident:*")
        
        # Count active emergencies
        active_emergencies = 0
        for key in emergency_keys:
            data = await redis_manager.get_data(key)
            if data and data.get("status") == "active":
                active_emergencies += 1
        
        return CityAnalytics(
            total_vehicles=len(vehicle_keys),
            active_emergencies=active_emergencies,
            average_city_speed=30.0,  # Default placeholder
            congested_roads=0,        # Default placeholder
            timestamp=datetime.now()
        )
    
    except Exception as e:
        logger.error(f"Error computing city analytics: {e}")
        return None

@app.get("/api/analytics/city-summary", response_model=CityAnalytics)
async def get_city_summary() -> CityAnalytics:
    """Get city-wide analytics summary"""
    analytics = await get_city_analytics()
    
    if not analytics:
        raise HTTPException(status_code=500, detail="Failed to compute city analytics")
    
    return analytics

# WebSocket endpoint for real-time updates
@app.websocket("/ws/realtime")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time data streaming"""
    await websocket_manager.connect(websocket)
    
    try:
        while True:
            # Keep connection alive and handle any incoming messages
            await websocket.receive_text()
    
    except WebSocketDisconnect:
        websocket_manager.disconnect(websocket)
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
        websocket_manager.disconnect(websocket)

# Additional utility endpoints

@app.get("/api/health")
async def health_check():
    """Detailed health check"""
    redis_healthy = redis_manager.connection_healthy
    
    # Test Redis connectivity
    if redis_healthy:
        try:
            test_data = await redis_manager.get_data("health:check")
            redis_responsive = True
        except:
            redis_responsive = False
    else:
        redis_responsive = False
    
    return {
        "service": "Smart City API",
        "status": "healthy" if redis_responsive else "degraded",
        "components": {
            "redis": {
                "connected": redis_healthy,
                "responsive": redis_responsive
            },
            "websockets": {
                "active_connections": len(websocket_manager.active_connections)
            }
        },
        "timestamp": datetime.now().isoformat()
    }

@app.get("/api/stats")
async def get_api_stats():
    """Get API usage statistics"""
    try:
        # Get data counts from Redis
        vehicle_count = len(await redis_manager.get_pattern_keys("vehicle:*"))
        emergency_count = len(await redis_manager.get_pattern_keys("emergency:*"))
        traffic_count = len(await redis_manager.get_pattern_keys("traffic:*"))
        
        return {
            "data_points": {
                "vehicles": vehicle_count,
                "emergencies": emergency_count,
                "traffic_conditions": traffic_count
            },
            "api_status": {
                "websocket_connections": len(websocket_manager.active_connections),
                "redis_healthy": redis_manager.connection_healthy
            },
            "timestamp": datetime.now().isoformat()
        }
    
    except Exception as e:
        logger.error(f"Error fetching API stats: {e}")
        raise HTTPException(status_code=500, detail="Failed to fetch API statistics")

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True,
        log_level="info"
    )
