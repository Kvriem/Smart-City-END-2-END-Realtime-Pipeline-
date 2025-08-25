'use client'

import { useState, useEffect, useRef, useCallback } from 'react'

export interface WebSocketMessage {
  type: string
  data: any
  timestamp: string
}

export function useWebSocket() {
  const [isConnected, setIsConnected] = useState(false)
  const [connectionStatus, setConnectionStatus] = useState<'connected' | 'disconnected' | 'connecting'>('disconnected')
  const [lastMessage, setLastMessage] = useState<WebSocketMessage | null>(null)
  const ws = useRef<WebSocket | null>(null)
  const reconnectTimeoutRef = useRef<NodeJS.Timeout | null>(null)

  // Force localhost IP address instead of hostname
  const wsUrl = 'ws://127.0.0.1:8000/ws/realtime'

  const connect = useCallback(() => {
    console.log('🔌 WebSocket connect called')
    console.log('📍 WebSocket URL:', wsUrl)
    
    if (ws.current?.readyState === WebSocket.OPEN) {
      console.log('✅ WebSocket already open')
      return
    }

    if (ws.current?.readyState === WebSocket.CONNECTING) {
      console.log('⏳ WebSocket already connecting')
      return
    }

    // Clean up any existing connection
    if (ws.current) {
      ws.current.close()
      ws.current = null
    }

    console.log('🔄 Setting status to connecting')
    setConnectionStatus('connecting')
    setIsConnected(false)
    
    try {
      console.log('🆕 Creating new WebSocket to:', wsUrl)
      const websocket = new WebSocket(wsUrl)
      ws.current = websocket

      const connectTimeout = setTimeout(() => {
        console.log('⏰ Connection timeout - closing WebSocket')
        websocket.close()
        setConnectionStatus('disconnected')
      }, 10000) // 10 second timeout

      websocket.onopen = (event) => {
        console.log('✅ WebSocket OPENED successfully!', event)
        clearTimeout(connectTimeout)
        setIsConnected(true)
        setConnectionStatus('connected')
        
        // Clear any pending reconnection
        if (reconnectTimeoutRef.current) {
          clearTimeout(reconnectTimeoutRef.current)
          reconnectTimeoutRef.current = null
        }
      }

      websocket.onmessage = (event) => {
        try {
          const message: WebSocketMessage = JSON.parse(event.data)
          setLastMessage(message)
          console.log('📨 WebSocket message received:', message)
        } catch (error) {
          console.error('❌ Failed to parse WebSocket message:', error)
          console.log('📄 Raw message data:', event.data)
        }
      }

      websocket.onclose = (event) => {
        console.log('🔌 WebSocket CLOSED:', {
          code: event.code,
          reason: event.reason,
          wasClean: event.wasClean
        })
        clearTimeout(connectTimeout)
        setIsConnected(false)
        setConnectionStatus('disconnected')
        
        // Auto-reconnect for unexpected closures
        if (event.code !== 1000 && event.code !== 1001) {
          console.log('🔄 Scheduling reconnection in 3 seconds...')
          reconnectTimeoutRef.current = setTimeout(() => {
            console.log('🔄 Attempting to reconnect...')
            connect()
          }, 3000)
        }
      }

      websocket.onerror = (error) => {
        console.error('❌ WebSocket ERROR:', error)
        clearTimeout(connectTimeout)
        setConnectionStatus('disconnected')
        setIsConnected(false)
      }

    } catch (error) {
      console.error('❌ Failed to create WebSocket:', error)
      setConnectionStatus('disconnected')
      setIsConnected(false)
    }
  }, [wsUrl])

  const disconnect = useCallback(() => {
    if (reconnectTimeoutRef.current) {
      clearTimeout(reconnectTimeoutRef.current)
      reconnectTimeoutRef.current = null
    }
    
    if (ws.current) {
      ws.current.close(1000, 'Manual disconnect')
      ws.current = null
    }
    
    setIsConnected(false)
    setConnectionStatus('disconnected')
  }, [])

  const sendMessage = useCallback((message: any) => {
    if (ws.current?.readyState === WebSocket.OPEN) {
      ws.current.send(JSON.stringify(message))
    } else {
      console.warn('WebSocket is not connected. Cannot send message.')
    }
  }, [])

  // Auto-connect on mount
  useEffect(() => {
    connect()
    
    return () => {
      disconnect()
    }
  }, [connect, disconnect])

  return {
    isConnected,
    connectionStatus,
    lastMessage,
    connect,
    disconnect,
    sendMessage
  }
}
