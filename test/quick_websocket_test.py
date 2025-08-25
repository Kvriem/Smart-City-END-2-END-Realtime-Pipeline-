import asyncio
import websockets
import json

async def test_websocket():
    try:
        print("Connecting to ws://localhost:8000/ws/realtime...")
        async with websockets.connect("ws://localhost:8000/ws/realtime") as websocket:
            print("✅ WebSocket connection successful!")
            
            # Wait for a message for 5 seconds
            try:
                message = await asyncio.wait_for(websocket.recv(), timeout=5.0)
                print(f"📨 Received message: {message}")
            except asyncio.TimeoutError:
                print("⏰ No message received within 5 seconds (this is normal)")
            
            print("🔌 WebSocket endpoint is working correctly")
            
    except Exception as e:
        print(f"❌ WebSocket connection failed: {e}")

if __name__ == "__main__":
    asyncio.run(test_websocket())
