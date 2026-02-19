"""
Curax Cloud Relay - link by Bot ID + API Key. One URL, no PC IP.
Deploy ONCE to Render/Railway; that single URL is used by ALL users.
Each user has a unique Bot ID + API Key, so the relay routes alerts correctly.
When the app is closed, alerts are pushed via FCM if FCM_SERVER_KEY is set.
"""

import asyncio
import json
import os
import urllib.request

try:
    import websockets
except ImportError:
    print("pip install websockets")
    raise

# bot_id -> (api_key, websocket_or_None, fcm_token_or_None)
# When app disconnects we keep (api_key, None, fcm_token) so we can still send via FCM.
clients = {}

FCM_SERVER_KEY = os.environ.get("FCM_SERVER_KEY", "").strip()


def _send_fcm_sync(token: str, alert_type: str, message: str) -> bool:
    """Send FCM message via legacy API. Runs in thread."""
    if not FCM_SERVER_KEY or not token:
        return False
    url = "https://fcm.googleapis.com/fcm/send"
    body = json.dumps(
        {
            "to": token,
            "priority": "high",
            "content_available": True,
            "data": {"type": alert_type, "message": message},
        }
    ).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=body,
        headers={
            "Authorization": f"key={FCM_SERVER_KEY}",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            return 200 <= r.status < 300
    except Exception as e:
        print(f"  FCM send error: {e}")
        return False


async def send_fcm(token: str, alert_type: str, message: str) -> bool:
    if not token:
        return False
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _send_fcm_sync, token, alert_type, message)


async def process_request(*args):
    """Allow Render HTTP health checks (HEAD/GET) on the same port.

    Supports both old and new websockets callback signatures.
    """
    upgrade = ""

    if len(args) == 2 and hasattr(args[1], "headers"):
        # New signature: (connection, request)
        connection, request = args
        upgrade = request.headers.get("Upgrade", "")
        if upgrade.lower() != "websocket":
            return connection.respond(200, "ok")
        return None

    if len(args) == 2:
        # Legacy signature: (path, request_headers)
        _path, request_headers = args
        upgrade = request_headers.get("Upgrade", "")
        if upgrade.lower() != "websocket":
            return 200, [("Content-Type", "text/plain")], b"ok"
        return None

    return None


async def handle_ws(ws):
    bot_id, api_key = None, None
    try:
        msg = await asyncio.wait_for(ws.recv(), timeout=10.0)
        data = json.loads(msg)
        bot_id = data.get("bot_id", "").strip()
        api_key = data.get("api_key", "").strip()

        # Desktop sends one message: action="alert", bot_id, api_key, type, message
        if data.get("action") == "alert":
            bid = data.get("bot_id", "").strip()
            akey = data.get("api_key", "").strip()
            atype = data.get("type", "alert")
            amsg = data.get("message", "")
            payload = json.dumps({"type": atype, "message": amsg})

            try:
                if bid in clients and clients[bid][0] == akey:
                    entry = clients[bid]
                    ws_sock = entry[1]
                    fcm_token = entry[2]
                    sent = False

                    # FCM-first delivery path (works when phone screen/app is off).
                    if fcm_token and await send_fcm(fcm_token, atype, amsg):
                        sent = True
                        print(f"  -> Pushed via FCM: {bid}")

                    # WebSocket fallback if FCM token missing or send failed.
                    if not sent and ws_sock is not None:
                        try:
                            await ws_sock.send(payload)
                            sent = True
                            print(f"  -> Forwarded to app (WebSocket): {bid}")
                        except Exception as e:
                            print(f"  WebSocket send error: {e}")
                            clients[bid] = (entry[0], None, entry[2])

                    if not sent:
                        print(f"  -> No app connected for {bid}, alert not delivered")
            except Exception as e:
                print(f"  Forward alert error: {e}")

            try:
                await ws.close(1000, "ok")
            except Exception:
                pass
            return

        # Phone: register (bot_id, api_key, optional fcm_token)
        if not bot_id or not api_key:
            try:
                await ws.close(4000, "bot_id and api_key required")
            except Exception:
                pass
            return

        fcm_token = data.get("fcm_token", "").strip() or None
        old = clients.get(bot_id)
        clients[bot_id] = (api_key, ws, fcm_token or (old[2] if old else None))
        print(f"  Bot linked: {bot_id}" + (" (FCM token stored)" if fcm_token else ""))

        async for _ in ws:
            pass

    except asyncio.TimeoutError:
        try:
            await ws.close(4001, "send bot_id and api_key first")
        except Exception:
            pass
    except Exception as e:
        print(f"  WS error: {e}")
        try:
            await ws.close(1000, "ok")
        except Exception:
            pass
    finally:
        if bot_id and bot_id in clients and clients[bot_id][1] == ws:
            # Keep entry for FCM when app is closed; set ws to None.
            clients[bot_id] = (clients[bot_id][0], None, clients[bot_id][2])
            print(f"  Bot disconnected (FCM still active): {bot_id}")


async def main():
    port = int(os.environ.get("PORT", 5050))
    async with websockets.serve(handle_ws, "0.0.0.0", port, process_request=process_request):
        print(f"Cloud relay: WebSocket port {port}")
        print(
            "Link by Bot ID + API Key. FCM push when app closed: "
            + ("enabled" if FCM_SERVER_KEY else "disabled (set FCM_SERVER_KEY)")
        )
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
