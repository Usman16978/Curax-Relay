"""
Curax Cloud Relay - link by Bot ID + API Key. One URL, no PC IP.
Deploy ONCE to Render/Railway; that single URL is used by ALL users.
Each user has a unique Bot ID + API Key, so the relay routes alerts correctly.

Deploy to Render.com:
  1. Create a free Web Service; connect your repo or upload relay_cloud/.
  2. Build: pip install -r requirements.txt
  3. Start: python server.py (or use Procfile: web: python server.py)
  4. Set env var PORT (Render sets this automatically).
  5. Your URL will be like https://curax-relay.onrender.com — use that as Server URL everywhere.
"""
import asyncio
import json
import os

try:
    import websockets
except ImportError:
    print("pip install websockets")
    raise

# bot_id -> (api_key, websocket) for phones
clients = {}

async def handle_ws(ws):
    bot_id, api_key = None, None
    try:
        msg = await asyncio.wait_for(ws.recv(), timeout=10.0)
        data = json.loads(msg)
        bot_id = data.get("bot_id", "").strip()
        api_key = data.get("api_key", "").strip()
        # Desktop sends one message: action="alert", type, message
        if data.get("action") == "alert":
            bid = data.get("bot_id", "").strip()
            akey = data.get("api_key", "").strip()
            if bid in clients and clients[bid][0] == akey:
                await clients[bid][1].send(json.dumps({"type": data.get("type", "alert"), "message": data.get("message", "")}))
            await ws.close(1000, "ok")
            return
        # Phone: register
        if not bot_id or not api_key:
            await ws.close(4000, "bot_id and api_key required")
            return
        clients[bot_id] = (api_key, ws)
        print(f"  Bot linked: {bot_id}")
        async for _ in ws:
            pass
    except asyncio.TimeoutError:
        await ws.close(4001, "send bot_id and api_key first")
    except Exception as e:
        print(f"  WS error: {e}")
    finally:
        if bot_id and clients.get(bot_id, (None, None))[1] == ws:
            del clients[bot_id]
            print(f"  Bot unlinked: {bot_id}")

async def main():
    port = int(os.environ.get("PORT", 5050))
    async with websockets.serve(handle_ws, "0.0.0.0", port):
        print(f"Cloud relay: WebSocket port {port}")
        print("Link by Bot ID + API Key. No PC IP needed.")
        await asyncio.Future()

if __name__ == "__main__":
    asyncio.run(main())
