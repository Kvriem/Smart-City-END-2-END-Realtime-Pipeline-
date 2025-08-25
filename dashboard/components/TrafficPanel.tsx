'use client'

import React, { useState, useEffect } from 'react'
import { useApiData, TrafficCondition } from '@/hooks/useApiData'

interface TrafficPanelProps {
  isLoading: boolean
}

const SAMPLE_ROADS = [
  { id: 'highway_101', name: 'Highway 101' },
  { id: 'market_st', name: 'Market Street' },
  { id: 'broadway', name: 'Broadway' },
  { id: 'mission_st', name: 'Mission Street' },
  { id: 'van_ness', name: 'Van Ness Avenue' }
]

export default function TrafficPanel({ isLoading }: TrafficPanelProps) {
  const { fetchTrafficCondition } = useApiData()
  const [trafficData, setTrafficData] = useState<TrafficCondition[]>([])
  const [refreshing, setRefreshing] = useState(false)

  const getCongestionColor = (level: string) => {
    switch (level) {
      case 'light': return 'bg-green-100 text-green-800 border-green-200'
      case 'moderate': return 'bg-yellow-100 text-yellow-800 border-yellow-200'
      case 'heavy': return 'bg-red-100 text-red-800 border-red-200'
      default: return 'bg-gray-100 text-gray-800 border-gray-200'
    }
  }

  const getCongestionIcon = (level: string) => {
    switch (level) {
      case 'light':
        return (
          <svg className="w-5 h-5 text-green-600" fill="currentColor" viewBox="0 0 20 20">
            <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zm3.707-9.293a1 1 0 00-1.414-1.414L9 10.586 7.707 9.293a1 1 0 00-1.414 1.414l2 2a1 1 0 001.414 0l4-4z" clipRule="evenodd" />
          </svg>
        )
      case 'moderate':
        return (
          <svg className="w-5 h-5 text-yellow-600" fill="currentColor" viewBox="0 0 20 20">
            <path fillRule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
          </svg>
        )
      case 'heavy':
        return (
          <svg className="w-5 h-5 text-red-600" fill="currentColor" viewBox="0 0 20 20">
            <path fillRule="evenodd" d="M13.477 14.89A6 6 0 015.11 6.524l8.367 8.368zm1.414-1.414L6.524 5.109a6 6 0 018.367 8.367zM18 10a8 8 0 11-16 0 8 8 0 0116 0z" clipRule="evenodd" />
          </svg>
        )
      default:
        return (
          <svg className="w-5 h-5 text-gray-600" fill="currentColor" viewBox="0 0 20 20">
            <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd" />
          </svg>
        )
    }
  }

  const fetchAllTrafficData = async () => {
    setRefreshing(true)
    const promises = SAMPLE_ROADS.map(road => fetchTrafficCondition(road.id))
    
    try {
      const results = await Promise.allSettled(promises)
      const successfulData = results
        .map((result, index) => ({
          result,
          road: SAMPLE_ROADS[index]
        }))
        .filter(({ result }) => result.status === 'fulfilled' && result.value !== null)
        .map(({ result, road }) => ({
          ...(result as PromiseFulfilledResult<TrafficCondition>).value,
          road_name: road.name
        }))
      
      setTrafficData(successfulData)
    } catch (error) {
      console.error('Failed to fetch traffic data:', error)
    } finally {
      setRefreshing(false)
    }
  }

  useEffect(() => {
    fetchAllTrafficData()
    
    // Refresh every 60 seconds
    const interval = setInterval(fetchAllTrafficData, 60000)
    return () => clearInterval(interval)
  }, [])

  if (isLoading && trafficData.length === 0) {
    return (
      <div className="p-6">
        <div className="animate-pulse space-y-4">
          {[...Array(4)].map((_, i) => (
            <div key={i} className="border border-gray-200 rounded-lg p-4">
              <div className="h-4 bg-gray-200 rounded w-3/4 mb-2"></div>
              <div className="h-3 bg-gray-200 rounded w-1/2 mb-2"></div>
              <div className="h-3 bg-gray-200 rounded w-2/3"></div>
            </div>
          ))}
        </div>
      </div>
    )
  }

  return (
    <div className="p-6 space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-gray-900">Traffic Conditions</h2>
        <button
          onClick={fetchAllTrafficData}
          disabled={refreshing}
          className="flex items-center px-3 py-1 text-sm bg-primary-100 text-primary-700 rounded-md hover:bg-primary-200 disabled:opacity-50 transition-colors"
        >
          {refreshing ? (
            <div className="loading-spinner mr-2"></div>
          ) : (
            <svg className="w-4 h-4 mr-1" fill="none" stroke="currentColor" viewBox="0 0 24 24">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
            </svg>
          )}
          Refresh
        </button>
      </div>

      {/* Traffic Overview */}
      <div className="grid grid-cols-3 gap-2 text-center">
        <div className="bg-green-50 p-3 rounded-lg">
          <div className="text-lg font-semibold text-green-700">
            {trafficData.filter(t => t.congestion_level === 'light').length}
          </div>
          <div className="text-xs text-green-600">Light</div>
        </div>
        <div className="bg-yellow-50 p-3 rounded-lg">
          <div className="text-lg font-semibold text-yellow-700">
            {trafficData.filter(t => t.congestion_level === 'moderate').length}
          </div>
          <div className="text-xs text-yellow-600">Moderate</div>
        </div>
        <div className="bg-red-50 p-3 rounded-lg">
          <div className="text-lg font-semibold text-red-700">
            {trafficData.filter(t => t.congestion_level === 'heavy').length}
          </div>
          <div className="text-xs text-red-600">Heavy</div>
        </div>
      </div>

      {/* Road Conditions */}
      <div className="space-y-3 max-h-96 overflow-y-auto">
        {trafficData.map((road) => (
          <div
            key={road.road_id}
            className="border border-gray-200 rounded-lg p-4 hover:shadow-md transition-shadow"
          >
            <div className="flex items-center space-x-3">
              <div className="flex-shrink-0">
                {getCongestionIcon(road.congestion_level)}
              </div>
              
              <div className="flex-1 min-w-0">
                <div className="flex items-center justify-between mb-2">
                  <h3 className="text-sm font-medium text-gray-900 truncate">
                    {road.road_name || road.road_id}
                  </h3>
                  <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border ${getCongestionColor(road.congestion_level)}`}>
                    {road.congestion_level}
                  </span>
                </div>
                
                <div className="grid grid-cols-2 gap-4 text-xs text-gray-500">
                  <div>
                    <span className="font-medium">Avg Speed:</span> {road.average_speed.toFixed(1)} km/h
                  </div>
                  <div>
                    <span className="font-medium">Vehicles:</span> {road.vehicle_count}
                  </div>
                </div>
                
                <div className="mt-2 text-xs text-gray-400">
                  Last updated: {new Date(road.timestamp).toLocaleTimeString()}
                </div>
              </div>
            </div>
          </div>
        ))}
        
        {trafficData.length === 0 && !refreshing && (
          <div className="text-center py-12">
            <svg className="mx-auto h-12 w-12 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            <h3 className="mt-2 text-sm font-medium text-gray-900">No traffic data</h3>
            <p className="mt-1 text-sm text-gray-500">
              Traffic information is currently unavailable.
            </p>
          </div>
        )}
      </div>
    </div>
  )
}
