'use client'

import { useState, useEffect } from 'react'
import dynamic from 'next/dynamic'
import StatsOverview from '@/components/StatsOverview'
import EmergencyPanel from '@/components/EmergencyPanel'
import TrafficPanel from '@/components/TrafficPanel'
import ConnectionStatus from '@/components/ConnectionStatus'
import { useWebSocket } from '@/hooks/useWebSocket'
import { useApiData } from '@/hooks/useApiData'

// Dynamically import map to avoid SSR issues
const SmartCityMap = dynamic(() => import('@/components/SmartCityMap'), {
  ssr: false,
  loading: () => (
    <div className="h-full flex items-center justify-center bg-gray-100">
      <div className="text-center">
        <div className="loading-spinner mx-auto mb-4"></div>
        <p className="text-gray-600">Loading map...</p>
      </div>
    </div>
  )
})

export default function Dashboard() {
  const [selectedPanel, setSelectedPanel] = useState<'overview' | 'traffic' | 'emergency'>('overview')
  const { 
    isConnected, 
    lastMessage, 
    connectionStatus,
    connect,
    disconnect 
  } = useWebSocket()

  const {
    vehicles,
    emergencies,
    analytics,
    isLoading,
    error,
    refreshData
  } = useApiData()

  // Auto-refresh data every 30 seconds
  useEffect(() => {
    const interval = setInterval(refreshData, 30000)
    return () => clearInterval(interval)
  }, [refreshData])

  return (
    <div className="h-screen flex flex-col">
      {/* Header */}
      <header className="bg-white shadow-sm border-b border-gray-200 px-6 py-4">
        <div className="flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold text-gray-900">Smart City Dashboard</h1>
            <p className="text-sm text-gray-600">Real-time monitoring and analytics</p>
          </div>
          
          <div className="flex items-center space-x-4">
            <ConnectionStatus 
              status={connectionStatus}
              isConnected={isConnected}
              onConnect={connect}
              onDisconnect={disconnect}
            />
            
            <button
              onClick={refreshData}
              disabled={isLoading}
              className="flex items-center px-3 py-2 bg-primary-600 text-white rounded-md hover:bg-primary-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
            >
              {isLoading ? (
                <div className="loading-spinner mr-2"></div>
              ) : (
                <svg className="w-4 h-4 mr-2" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
                </svg>
              )}
              Refresh
            </button>
          </div>
        </div>
      </header>

      {/* Error Banner */}
      {error && (
        <div className="bg-red-50 border-l-4 border-red-400 p-4">
          <div className="flex">
            <div className="flex-shrink-0">
              <svg className="h-5 w-5 text-red-400" viewBox="0 0 20 20" fill="currentColor">
                <path fillRule="evenodd" d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z" clipRule="evenodd" />
              </svg>
            </div>
            <div className="ml-3">
              <p className="text-sm text-red-700">
                {error.message || 'An error occurred while fetching data'}
              </p>
            </div>
          </div>
        </div>
      )}

      {/* Main Content */}
      <div className="flex-1 flex overflow-hidden">
        {/* Left Panel */}
        <div className="w-80 bg-white shadow-sm border-r border-gray-200 flex flex-col">
          {/* Panel Navigation */}
          <div className="flex border-b border-gray-200">
            <button
              onClick={() => setSelectedPanel('overview')}
              className={`flex-1 px-4 py-3 text-sm font-medium border-b-2 transition-colors ${
                selectedPanel === 'overview' 
                  ? 'border-primary-500 text-primary-600 bg-primary-50'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              Overview
            </button>
            <button
              onClick={() => setSelectedPanel('traffic')}
              className={`flex-1 px-4 py-3 text-sm font-medium border-b-2 transition-colors ${
                selectedPanel === 'traffic' 
                  ? 'border-primary-500 text-primary-600 bg-primary-50'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              Traffic
            </button>
            <button
              onClick={() => setSelectedPanel('emergency')}
              className={`flex-1 px-4 py-3 text-sm font-medium border-b-2 transition-colors ${
                selectedPanel === 'emergency' 
                  ? 'border-primary-500 text-primary-600 bg-primary-50'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              Emergency
            </button>
          </div>

          {/* Panel Content */}
          <div className="flex-1 overflow-y-auto">
            {selectedPanel === 'overview' && (
              <StatsOverview 
                analytics={analytics}
                vehicles={vehicles}
                emergencies={emergencies}
                isLoading={isLoading}
              />
            )}
            {selectedPanel === 'traffic' && (
              <TrafficPanel 
                isLoading={isLoading}
              />
            )}
            {selectedPanel === 'emergency' && (
              <EmergencyPanel 
                emergencies={emergencies}
                isLoading={isLoading}
              />
            )}
          </div>
        </div>

        {/* Map Container */}
        <div className="flex-1 relative">
          <SmartCityMap 
            vehicles={vehicles}
            emergencies={emergencies}
            className="absolute inset-0"
          />
        </div>
      </div>

      {/* Real-time Message Indicator */}
      {lastMessage && (
        <div className="fixed bottom-4 right-4 bg-green-500 text-white px-4 py-2 rounded-lg shadow-lg animate-bounce-slow">
          <p className="text-sm">Real-time update received</p>
        </div>
      )}
    </div>
  )
}
