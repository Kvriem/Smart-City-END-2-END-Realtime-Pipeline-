'use client'

import React, { useEffect, useRef } from 'react'
import { MapContainer, TileLayer, Marker, Popup, useMap } from 'react-leaflet'
import L from 'leaflet'
import 'leaflet/dist/leaflet.css'
import { Vehicle, Emergency } from '@/hooks/useApiData'

// Fix for default markers in react-leaflet
delete (L.Icon.Default.prototype as any)._getIconUrl
L.Icon.Default.mergeOptions({
  iconRetinaUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon-2x.png',
  iconUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-icon.png',
  shadowUrl: 'https://cdnjs.cloudflare.com/ajax/libs/leaflet/1.7.1/images/marker-shadow.png',
})

interface SmartCityMapProps {
  vehicles: Vehicle[]
  emergencies: Emergency[]
  className?: string
}

// Custom vehicle marker
const createVehicleIcon = (speed: number) => {
  const isMoving = speed > 5
  const color = isMoving ? '#3b82f6' : '#6b7280'
  
  return L.divIcon({
    html: `
      <div class="vehicle-marker ${isMoving ? 'moving' : ''}" style="
        width: 12px; 
        height: 12px; 
        background: ${color};
        border: 2px solid white;
        border-radius: 50%;
        box-shadow: 0 2px 4px rgba(0,0,0,0.2);
        ${isMoving ? 'animation: pulse 2s infinite;' : ''}
      "></div>
    `,
    className: 'custom-marker',
    iconSize: [16, 16],
    iconAnchor: [8, 8]
  })
}

// Custom emergency marker
const createEmergencyIcon = (severity: string) => {
  const colors = {
    critical: '#dc2626',
    high: '#ea580c',
    medium: '#d97706',
    low: '#16a34a'
  }
  
  const color = colors[severity as keyof typeof colors] || '#6b7280'
  
  return L.divIcon({
    html: `
      <div class="emergency-marker" style="
        width: 16px; 
        height: 16px; 
        background: ${color};
        border: 2px solid white;
        border-radius: 50%;
        box-shadow: 0 2px 4px rgba(0,0,0,0.2);
        animation: pulse-ring 1.5s infinite;
      "></div>
    `,
    className: 'custom-marker',
    iconSize: [20, 20],
    iconAnchor: [10, 10]
  })
}

// Component to fit map bounds to show all markers
function FitBounds({ vehicles, emergencies }: { vehicles: Vehicle[], emergencies: Emergency[] }) {
  const map = useMap()
  
  useEffect(() => {
    const allPoints = [
      ...vehicles.map(v => [v.latitude, v.longitude] as [number, number]),
      ...emergencies.map(e => [e.latitude, e.longitude] as [number, number])
    ]
    
    if (allPoints.length > 0) {
      const bounds = L.latLngBounds(allPoints)
      map.fitBounds(bounds, { padding: [20, 20] })
    } else {
      // Default to San Francisco if no data
      map.setView([37.7749, -122.4194], 12)
    }
  }, [map, vehicles, emergencies])
  
  return null
}

export default function SmartCityMap({ vehicles, emergencies, className }: SmartCityMapProps) {
  const mapRef = useRef<L.Map | null>(null)

  return (
    <div className={className}>
      <MapContainer
        center={[37.7749, -122.4194]} // San Francisco
        zoom={12}
        style={{ height: '100%', width: '100%' }}
        ref={mapRef}
      >
        <TileLayer
          attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
          url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        />
        
        <FitBounds vehicles={vehicles} emergencies={emergencies} />
        
        {/* Vehicle Markers */}
        {vehicles.map((vehicle) => (
          <Marker
            key={vehicle.vehicle_id}
            position={[vehicle.latitude, vehicle.longitude]}
            icon={createVehicleIcon(vehicle.speed)}
          >
            <Popup>
              <div className="p-2">
                <h3 className="font-semibold text-sm">Vehicle {vehicle.vehicle_id}</h3>
                <div className="text-xs text-gray-600 space-y-1 mt-1">
                  <p><strong>Speed:</strong> {vehicle.speed.toFixed(1)} km/h</p>
                  <p><strong>Direction:</strong> {vehicle.direction}°</p>
                  <p><strong>Location:</strong> {vehicle.latitude.toFixed(4)}, {vehicle.longitude.toFixed(4)}</p>
                  <p><strong>Last Update:</strong> {new Date(vehicle.timestamp).toLocaleTimeString()}</p>
                </div>
              </div>
            </Popup>
          </Marker>
        ))}
        
        {/* Emergency Markers */}
        {emergencies.map((emergency) => (
          <Marker
            key={emergency.incident_id}
            position={[emergency.latitude, emergency.longitude]}
            icon={createEmergencyIcon(emergency.severity)}
          >
            <Popup>
              <div className="p-2">
                <h3 className="font-semibold text-sm">
                  {emergency.incident_type.charAt(0).toUpperCase() + emergency.incident_type.slice(1)}
                </h3>
                <div className="text-xs text-gray-600 space-y-1 mt-1">
                  <p>
                    <strong>Severity:</strong> 
                    <span className={`ml-1 px-1 py-0.5 rounded text-xs font-medium ${
                      emergency.severity === 'critical' ? 'bg-red-100 text-red-700' :
                      emergency.severity === 'high' ? 'bg-orange-100 text-orange-700' :
                      emergency.severity === 'medium' ? 'bg-yellow-100 text-yellow-700' :
                      'bg-green-100 text-green-700'
                    }`}>
                      {emergency.severity}
                    </span>
                  </p>
                  <p><strong>Status:</strong> {emergency.status}</p>
                  <p><strong>Location:</strong> {emergency.latitude.toFixed(4)}, {emergency.longitude.toFixed(4)}</p>
                  <p><strong>Reported:</strong> {new Date(emergency.timestamp).toLocaleString()}</p>
                </div>
              </div>
            </Popup>
          </Marker>
        ))}
      </MapContainer>
      
      {/* Map Legend */}
      <div className="absolute top-4 right-4 bg-white rounded-lg shadow-lg p-3 z-[1000] max-w-xs">
        <h4 className="text-sm font-semibold text-gray-900 mb-2">Map Legend</h4>
        <div className="space-y-2 text-xs">
          <div className="flex items-center space-x-2">
            <div className="w-3 h-3 bg-blue-500 rounded-full"></div>
            <span>Active Vehicles ({vehicles.length})</span>
          </div>
          <div className="flex items-center space-x-2">
            <div className="w-3 h-3 bg-gray-500 rounded-full"></div>
            <span>Stationary Vehicles</span>
          </div>
          <div className="flex items-center space-x-2">
            <div className="w-3 h-3 bg-red-500 rounded-full"></div>
            <span>Emergencies ({emergencies.length})</span>
          </div>
        </div>
        
        {vehicles.length === 0 && emergencies.length === 0 && (
          <div className="mt-2 pt-2 border-t border-gray-200">
            <p className="text-xs text-gray-500">No active data to display</p>
          </div>
        )}
      </div>
    </div>
  )
}
