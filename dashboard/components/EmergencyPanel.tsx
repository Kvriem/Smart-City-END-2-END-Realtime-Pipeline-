'use client'

import React, { useState, useEffect } from 'react'
import { Emergency } from '@/hooks/useApiData'

interface EmergencyPanelProps {
  emergencies: Emergency[]
  isLoading: boolean
}

export default function EmergencyPanel({ emergencies, isLoading }: EmergencyPanelProps) {
  const [filter, setFilter] = useState<'all' | 'critical' | 'high' | 'medium' | 'low'>('all')
  const [sortBy, setSortBy] = useState<'timestamp' | 'severity'>('timestamp')

  const filteredEmergencies = emergencies
    .filter(emergency => filter === 'all' || emergency.severity === filter)
    .sort((a, b) => {
      if (sortBy === 'timestamp') {
        return new Date(b.timestamp).getTime() - new Date(a.timestamp).getTime()
      } else {
        const severityOrder = { critical: 4, high: 3, medium: 2, low: 1 }
        return severityOrder[b.severity] - severityOrder[a.severity]
      }
    })

  const getSeverityColor = (severity: string) => {
    switch (severity) {
      case 'critical': return 'bg-red-100 text-red-800 border-red-200'
      case 'high': return 'bg-orange-100 text-orange-800 border-orange-200'
      case 'medium': return 'bg-yellow-100 text-yellow-800 border-yellow-200'
      case 'low': return 'bg-green-100 text-green-800 border-green-200'
      default: return 'bg-gray-100 text-gray-800 border-gray-200'
    }
  }

  const getIncidentIcon = (type: string) => {
    switch (type.toLowerCase()) {
      case 'fire':
        return (
          <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20">
            <path fillRule="evenodd" d="M13.477 14.89A6 6 0 015.11 6.524l8.367 8.368zm1.414-1.414L6.524 5.109a6 6 0 018.367 8.367zM18 10a8 8 0 11-16 0 8 8 0 0116 0z" clipRule="evenodd" />
          </svg>
        )
      case 'accident':
        return (
          <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20">
            <path fillRule="evenodd" d="M8.257 3.099c.765-1.36 2.722-1.36 3.486 0l5.58 9.92c.75 1.334-.213 2.98-1.742 2.98H4.42c-1.53 0-2.493-1.646-1.743-2.98l5.58-9.92zM11 13a1 1 0 11-2 0 1 1 0 012 0zm-1-8a1 1 0 00-1 1v3a1 1 0 002 0V6a1 1 0 00-1-1z" clipRule="evenodd" />
          </svg>
        )
      case 'medical':
        return (
          <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20">
            <path d="M10 2L3 7v11a2 2 0 002 2h10a2 2 0 002-2V7l-7-5zM10 9a1 1 0 00-1 1v1H8a1 1 0 000 2h1v1a1 1 0 002 0v-1h1a1 1 0 000-2h-1v-1a1 1 0 00-1-1z" />
          </svg>
        )
      default:
        return (
          <svg className="w-5 h-5" fill="currentColor" viewBox="0 0 20 20">
            <path fillRule="evenodd" d="M18 10a8 8 0 11-16 0 8 8 0 0116 0zm-7-4a1 1 0 11-2 0 1 1 0 012 0zM9 9a1 1 0 000 2v3a1 1 0 001 1h1a1 1 0 100-2v-3a1 1 0 00-1-1H9z" clipRule="evenodd" />
          </svg>
        )
    }
  }

  if (isLoading) {
    return (
      <div className="p-6">
        <div className="animate-pulse space-y-4">
          {[...Array(3)].map((_, i) => (
            <div key={i} className="border border-gray-200 rounded-lg p-4">
              <div className="h-4 bg-gray-200 rounded w-3/4 mb-2"></div>
              <div className="h-3 bg-gray-200 rounded w-1/2"></div>
            </div>
          ))}
        </div>
      </div>
    )
  }

  return (
    <div className="p-6 space-y-4">
      <div className="flex items-center justify-between">
        <h2 className="text-lg font-semibold text-gray-900">Emergency Incidents</h2>
        <span className="text-sm text-gray-500">
          {filteredEmergencies.length} active
        </span>
      </div>

      {/* Filters */}
      <div className="space-y-3">
        <div>
          <label className="text-sm font-medium text-gray-700">Filter by Severity</label>
          <select
            value={filter}
            onChange={(e) => setFilter(e.target.value as any)}
            className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-primary-500 focus:ring-primary-500 text-sm"
          >
            <option value="all">All Severities</option>
            <option value="critical">Critical</option>
            <option value="high">High</option>
            <option value="medium">Medium</option>
            <option value="low">Low</option>
          </select>
        </div>
        
        <div>
          <label className="text-sm font-medium text-gray-700">Sort by</label>
          <select
            value={sortBy}
            onChange={(e) => setSortBy(e.target.value as any)}
            className="mt-1 block w-full rounded-md border-gray-300 shadow-sm focus:border-primary-500 focus:ring-primary-500 text-sm"
          >
            <option value="timestamp">Latest First</option>
            <option value="severity">Severity</option>
          </select>
        </div>
      </div>

      {/* Emergency List */}
      <div className="space-y-3 max-h-96 overflow-y-auto">
        {filteredEmergencies.map((emergency) => (
          <div
            key={emergency.incident_id}
            className="border border-gray-200 rounded-lg p-4 hover:shadow-md transition-shadow"
          >
            <div className="flex items-start space-x-3">
              <div className={`p-2 rounded-full ${getSeverityColor(emergency.severity)}`}>
                {getIncidentIcon(emergency.incident_type)}
              </div>
              
              <div className="flex-1 min-w-0">
                <div className="flex items-center space-x-2 mb-1">
                  <h3 className="text-sm font-medium text-gray-900 truncate">
                    {emergency.incident_type.charAt(0).toUpperCase() + emergency.incident_type.slice(1)}
                  </h3>
                  <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium border ${getSeverityColor(emergency.severity)}`}>
                    {emergency.severity}
                  </span>
                </div>
                
                <div className="text-xs text-gray-500 space-y-1">
                  <p>
                    <span className="font-medium">Location:</span> {emergency.latitude.toFixed(4)}, {emergency.longitude.toFixed(4)}
                  </p>
                  <p>
                    <span className="font-medium">Status:</span> {emergency.status}
                  </p>
                  <p>
                    <span className="font-medium">Time:</span> {new Date(emergency.timestamp).toLocaleString()}
                  </p>
                </div>
              </div>
              
              <div className="flex-shrink-0">
                <button className="text-xs text-primary-600 hover:text-primary-800 font-medium">
                  View Details
                </button>
              </div>
            </div>
          </div>
        ))}
        
        {filteredEmergencies.length === 0 && (
          <div className="text-center py-12">
            <svg className="mx-auto h-12 w-12 text-gray-400" fill="none" viewBox="0 0 24 24" stroke="currentColor">
              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
            </svg>
            <h3 className="mt-2 text-sm font-medium text-gray-900">No emergencies</h3>
            <p className="mt-1 text-sm text-gray-500">
              {filter === 'all' ? 'No active emergency incidents.' : `No ${filter} severity incidents.`}
            </p>
          </div>
        )}
      </div>
    </div>
  )
}
