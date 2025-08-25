#!/usr/bin/env python3
"""
WebSocket Connection Test for Smart City Dashboard
"""

import asyncio
import websockets
import json
from datetime import datetime

async def test_websocket_connection():
    """Test WebSocket connection to the API"""
    uri = "ws://localhost:8000/ws/realtime"
    
    try:
        print(f"🔌 Attempting to connect to: {uri}")
        async with websockets.connect(uri) as websocket:
            print("✅ WebSocket connected successfully!")
            
            # Send a test message
            test_message = {
                "type": "ping",
                "timestamp": datetime.now().isoformat()
            }
            
            await websocket.send(json.dumps(test_message))
            print(f"📤 Sent test message: {test_message}")
            
            # Listen for messages for 10 seconds
            try:
                response = await asyncio.wait_for(websocket.recv(), timeout=10.0)
                print(f"📥 Received response: {response}")
            except asyncio.TimeoutError:
                print("⏰ No response received within 10 seconds (this is normal)")
            
            print("✅ WebSocket connection test completed successfully!")
            
    except Exception as e:
        print(f"❌ WebSocket connection failed: {e}")
        return False
    
    return True

if __name__ == "__main__":
    result = asyncio.run(test_websocket_connection())
    if result:
        print("\n🎉 WebSocket endpoint is working correctly!")
        print("The dashboard should now be able to connect to WebSocket.")
    else:
        print("\n⚠️ WebSocket connection issues detected.")
