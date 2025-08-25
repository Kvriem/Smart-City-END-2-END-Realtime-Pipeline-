#!/bin/bash
# Script to populate Redis with test data

# Add vehicle locations
redis-cli SET "vehicle:location:vehicle_001" '{"vehicle_id":"vehicle_001","latitude":37.7749,"longitude":-122.4194,"speed":45.5,"direction":180,"timestamp":"2025-08-25T12:35:00"}'
redis-cli SET "vehicle:location:vehicle_002" '{"vehicle_id":"vehicle_002","latitude":37.7849,"longitude":-122.4094,"speed":30.2,"direction":90,"timestamp":"2025-08-25T12:35:00"}'
redis-cli SET "vehicle:location:vehicle_003" '{"vehicle_id":"vehicle_003","latitude":37.7549,"longitude":-122.4294,"speed":55.0,"direction":270,"timestamp":"2025-08-25T12:35:00"}'

# Add emergency incidents
redis-cli SET "emergency:incident:emerg_001" '{"incident_id":"emerg_001","incident_type":"accident","latitude":37.7649,"longitude":-122.4394,"severity":"high","status":"active","timestamp":"2025-08-25T12:30:00"}'
redis-cli SET "emergency:incident:emerg_002" '{"incident_id":"emerg_002","incident_type":"fire","latitude":37.7450,"longitude":-122.4150,"severity":"critical","status":"active","timestamp":"2025-08-25T12:25:00"}'

# Add traffic conditions
redis-cli SET "traffic:road:highway_101" '{"road_id":"highway_101","average_speed":35.8,"vehicle_count":45,"congestion_level":"moderate","timestamp":"2025-08-25T12:35:00"}'
redis-cli SET "traffic:road:market_st" '{"road_id":"market_st","average_speed":15.2,"vehicle_count":78,"congestion_level":"heavy","timestamp":"2025-08-25T12:35:00"}'

# Add city analytics
redis-cli SET "analytics:city:summary" '{"total_vehicles":15,"active_emergencies":2,"average_city_speed":42.3,"congested_roads":3,"timestamp":"2025-08-25T12:35:00"}'

echo "✅ Test data populated successfully!"
