'use client'

import { useState, useEffect, useCallback } from 'react'

export interface Vehicle {
  vehicle_id: string
  latitude: number
  longitude: number
  speed: number
  direction: number
  timestamp: string
}

export interface Emergency {
  incident_id: string
  incident_type: string
  latitude: number
  longitude: number
  severity: 'low' | 'medium' | 'high' | 'critical'
  status: 'active' | 'resolved' | 'pending'
  timestamp: string
}

export interface Analytics {
  total_vehicles: number
  active_emergencies: number
  average_city_speed: number
  congested_roads: number
  timestamp: string
}

export interface TrafficCondition {
  road_id: string
  average_speed: number
  vehicle_count: number
  congestion_level: 'light' | 'moderate' | 'heavy'
  timestamp: string
}

export function useApiData() {
  const [vehicles, setVehicles] = useState<Vehicle[]>([])
  const [emergencies, setEmergencies] = useState<Emergency[]>([])
  const [analytics, setAnalytics] = useState<Analytics | null>(null)
  const [isLoading, setIsLoading] = useState(false)
  const [error, setError] = useState<Error | null>(null)

  const apiUrl = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000'

  const fetchWithTimeout = async (url: string, timeout = 10000) => {
    const controller = new AbortController()
    const timeoutId = setTimeout(() => controller.abort(), timeout)
    
    try {
      const response = await fetch(url, {
        signal: controller.signal,
        headers: {
          'Accept': 'application/json',
          'Content-Type': 'application/json',
        },
      })
      clearTimeout(timeoutId)
      
      if (!response.ok) {
        throw new Error(`HTTP ${response.status}: ${response.statusText}`)
      }
      
      return response.json()
    } catch (error) {
      clearTimeout(timeoutId)
      throw error
    }
  }

  const fetchVehicles = useCallback(async () => {
    try {
      const data = await fetchWithTimeout(`${apiUrl}/api/vehicles/locations`)
      setVehicles(Array.isArray(data) ? data : [])
    } catch (error) {
      console.error('Failed to fetch vehicles:', error)
      setVehicles([])
    }
  }, [apiUrl])

  const fetchEmergencies = useCallback(async () => {
    try {
      const data = await fetchWithTimeout(`${apiUrl}/api/emergencies/active`)
      setEmergencies(Array.isArray(data) ? data : [])
    } catch (error) {
      console.error('Failed to fetch emergencies:', error)
      setEmergencies([])
    }
  }, [apiUrl])

  const fetchAnalytics = useCallback(async () => {
    try {
      const data = await fetchWithTimeout(`${apiUrl}/api/analytics/city-summary`)
      setAnalytics(data || null)
    } catch (error) {
      console.error('Failed to fetch analytics:', error)
      setAnalytics(null)
    }
  }, [apiUrl])

  const fetchTrafficCondition = useCallback(async (roadId: string): Promise<TrafficCondition | null> => {
    try {
      const data = await fetchWithTimeout(`${apiUrl}/api/traffic/live/${roadId}`)
      return data || null
    } catch (error) {
      console.error(`Failed to fetch traffic for ${roadId}:`, error)
      return null
    }
  }, [apiUrl])

  const refreshData = useCallback(async () => {
    setIsLoading(true)
    setError(null)
    
    try {
      await Promise.allSettled([
        fetchVehicles(),
        fetchEmergencies(),
        fetchAnalytics()
      ])
    } catch (error) {
      console.error('Failed to refresh data:', error)
      setError(error instanceof Error ? error : new Error('Unknown error occurred'))
    } finally {
      setIsLoading(false)
    }
  }, [fetchVehicles, fetchEmergencies, fetchAnalytics])

  // Initial data fetch
  useEffect(() => {
    refreshData()
  }, [refreshData])

  return {
    vehicles,
    emergencies,
    analytics,
    isLoading,
    error,
    refreshData,
    fetchTrafficCondition
  }
}
