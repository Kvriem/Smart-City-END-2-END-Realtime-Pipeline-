'use client'

import React from 'react'
import { Vehicle, Emergency, Analytics } from '@/hooks/useApiData'

interface StatsOverviewProps {
  analytics: Analytics | null
  vehicles: Vehicle[]
  emergencies: Emergency[]
  isLoading: boolean
}

export default function StatsOverview({ 
  analytics, 
  vehicles, 
  emergencies, 
  isLoading 
}: StatsOverviewProps) {
  const getEmergencySeverityCount = (severity: string) => {
    return emergencies.filter(e => e.severity === severity).length
  }

  if (isLoading) {
    return (
      <div className="p-6">
        <div className="animate-pulse space-y-4">
          <div className="h-4 bg-gray-200 rounded w-3/4"></div>
          <div className="h-4 bg-gray-200 rounded w-1/2"></div>
          <div className="h-4 bg-gray-200 rounded w-2/3"></div>
        </div>
      </div>
    )
  }

  return (
    <div className="p-6 space-y-6">
      <h2 className="text-lg font-semibold text-gray-900">City Overview</h2>
      
      {/* Key Metrics */}
      <div className="grid grid-cols-2 gap-4">
        <div className="stats-card">
          <div className="stats-value text-blue-600">
            {analytics?.total_vehicles || vehicles.length}
          </div>
          <div className="stats-label">Active Vehicles</div>
        </div>
        
        <div className="stats-card">
          <div className="stats-value text-red-600">
            {analytics?.active_emergencies || emergencies.length}
          </div>
          <div className="stats-label">Emergencies</div>
        </div>
        
        <div className="stats-card">
          <div className="stats-value text-green-600">
            {analytics?.average_city_speed?.toFixed(1) || '0.0'}
          </div>
          <div className="stats-label">Avg Speed (km/h)</div>
        </div>
        
        <div className="stats-card">
          <div className="stats-value text-orange-600">
            {analytics?.congested_roads || 0}
          </div>
          <div className="stats-label">Congested Roads</div>
        </div>
      </div>

      {/* Emergency Breakdown */}
      {emergencies.length > 0 && (
        <div>
          <h3 className="text-md font-medium text-gray-900 mb-3">Emergency Breakdown</h3>
          <div className="space-y-2">
            <div className="flex justify-between items-center">
              <span className="text-sm text-gray-600">Critical</span>
              <span className="text-sm font-medium text-red-600">
                {getEmergencySeverityCount('critical')}
              </span>
            </div>
            <div className="flex justify-between items-center">
              <span className="text-sm text-gray-600">High</span>
              <span className="text-sm font-medium text-orange-600">
                {getEmergencySeverityCount('high')}
              </span>
            </div>
            <div className="flex justify-between items-center">
              <span className="text-sm text-gray-600">Medium</span>
              <span className="text-sm font-medium text-yellow-600">
                {getEmergencySeverityCount('medium')}
              </span>
            </div>
            <div className="flex justify-between items-center">
              <span className="text-sm text-gray-600">Low</span>
              <span className="text-sm font-medium text-green-600">
                {getEmergencySeverityCount('low')}
              </span>
            </div>
          </div>
        </div>
      )}

      {/* Recent Activity */}
      <div>
        <h3 className="text-md font-medium text-gray-900 mb-3">Recent Activity</h3>
        <div className="space-y-3 max-h-64 overflow-y-auto">
          {vehicles.slice(0, 5).map((vehicle) => (
            <div key={vehicle.vehicle_id} className="flex items-center space-x-3 p-2 bg-blue-50 rounded">
              <div className="w-2 h-2 bg-blue-500 rounded-full"></div>
              <div className="flex-1 min-w-0">
                <p className="text-sm font-medium text-gray-900 truncate">
                  Vehicle {vehicle.vehicle_id}
                </p>
                <p className="text-xs text-gray-500">
                  Speed: {vehicle.speed} km/h • {new Date(vehicle.timestamp).toLocaleTimeString()}
                </p>
              </div>
            </div>
          ))}
          
          {emergencies.slice(0, 3).map((emergency) => (
            <div key={emergency.incident_id} className="flex items-center space-x-3 p-2 bg-red-50 rounded">
              <div className={`w-2 h-2 rounded-full ${
                emergency.severity === 'critical' ? 'bg-red-600' :
                emergency.severity === 'high' ? 'bg-orange-500' :
                emergency.severity === 'medium' ? 'bg-yellow-500' : 'bg-green-500'
              }`}></div>
              <div className="flex-1 min-w-0">
                <p className="text-sm font-medium text-gray-900 truncate">
                  {emergency.incident_type.charAt(0).toUpperCase() + emergency.incident_type.slice(1)} - {emergency.severity}
                </p>
                <p className="text-xs text-gray-500">
                  {emergency.status} • {new Date(emergency.timestamp).toLocaleTimeString()}
                </p>
              </div>
            </div>
          ))}
          
          {vehicles.length === 0 && emergencies.length === 0 && (
            <div className="text-center py-8 text-gray-500">
              <p className="text-sm">No recent activity</p>
            </div>
          )}
        </div>
      </div>

      {/* Last Updated */}
      {analytics && (
        <div className="pt-4 border-t border-gray-200">
          <p className="text-xs text-gray-500">
            Last updated: {new Date(analytics.timestamp).toLocaleString()}
          </p>
        </div>
      )}
    </div>
  )
}
