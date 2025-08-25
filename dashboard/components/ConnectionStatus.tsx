'use client'

import React from 'react'

interface ConnectionStatusProps {
  status: 'connected' | 'disconnected' | 'connecting'
  isConnected: boolean
  onConnect: () => void
  onDisconnect: () => void
}

export default function ConnectionStatus({ 
  status, 
  isConnected, 
  onConnect, 
  onDisconnect 
}: ConnectionStatusProps) {
  const getStatusColor = () => {
    switch (status) {
      case 'connected': return 'connection-connected'
      case 'connecting': return 'connection-connecting'
      case 'disconnected': return 'connection-disconnected'
      default: return 'connection-disconnected'
    }
  }

  const getStatusText = () => {
    switch (status) {
      case 'connected': return 'Connected'
      case 'connecting': return 'Connecting...'
      case 'disconnected': return 'Disconnected'
      default: return 'Unknown'
    }
  }

  return (
    <div className="flex items-center space-x-2">
      <span className={`connection-indicator ${getStatusColor()}`}>
        <span className="flex items-center">
          <span 
            className={`h-2 w-2 rounded-full mr-1.5 ${
              isConnected ? 'bg-green-500' : 'bg-red-500'
            }`}
          />
          WebSocket {getStatusText()}
        </span>
      </span>
      
      <button
        onClick={isConnected ? onDisconnect : onConnect}
        className={`px-2 py-1 text-xs rounded ${
          isConnected 
            ? 'bg-red-100 text-red-700 hover:bg-red-200' 
            : 'bg-green-100 text-green-700 hover:bg-green-200'
        } transition-colors`}
      >
        {isConnected ? 'Disconnect' : 'Connect'}
      </button>
    </div>
  )
}
