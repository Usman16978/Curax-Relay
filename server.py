import asyncio
import json
import os
import urllib.request
from typing import Dict, Optional, Tuple

from aiohttp import WSMsgType, web

# bot_id -> (api_key, websocket_or_None, fcm_token_or_None)
# When app disconnects we keep (api_key, None, fcm_token) so we can still send via FCM.
clients: Dict[str, Tuple[str, Optional[web.WebSocketResponse], Optional[str]]] = {}

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


async def handle_websocket(request: web.Request) -> web.WebSocketResponse:
    ws = web.WebSocketResponse(heartbeat=25)
    await ws.prepare(request)

    bot_id: Optional[str] = None

    try:
        first = await asyncio.wait_for(ws.receive(), timeout=10.0)
        if first.type != WSMsgType.TEXT:
            await ws.close(code=4000, message=b"text json required")
            return ws

        data = json.loads(first.data)
        bot_id = (data.get("bot_id") or "").strip()
        api_key = (data.get("api_key") or "").strip()

        # Desktop sends one message: action="alert", bot_id, api_key, type, message
        if data.get("action") == "alert":
            bid = (data.get("bot_id") or "").strip()
            akey = (data.get("api_key") or "").strip()
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
                    if not sent and ws_sock is not None and not ws_sock.closed:
                        try:
                            await ws_sock.send_str(payload)
                            sent = True
                            print(f"  -> Forwarded to app (WebSocket): {bid}")
                        except Exception as e:
                            print(f"  WebSocket send error: {e}")
                            clients[bid] = (entry[0], None, entry[2])

                    if not sent:
                        print(f"  -> No app connected for {bid}, alert not delivered")
            except Exception as e:
                print(f"  Forward alert error: {e}")

            await ws.close(code=1000, message=b"ok")
            return ws

        # Phone: register (bot_id, api_key, optional fcm_token)
        if not bot_id or not api_key:
            await ws.close(code=4000, message=b"bot_id and api_key required")
            return ws

        fcm_token = (data.get("fcm_token") or "").strip() or None
        old = clients.get(bot_id)
        clients[bot_id] = (api_key, ws, fcm_token or (old[2] if old else None))
        print(f"  Bot linked: {bot_id}" + (" (FCM token stored)" if fcm_token else ""))

        async for msg in ws:
            if msg.type in (WSMsgType.CLOSED, WSMsgType.CLOSE, WSMsgType.ERROR):
                break

    except asyncio.TimeoutError:
        await ws.close(code=4001, message=b"send bot_id and api_key first")
    except Exception as e:
        print(f"  WS error: {e}")
        await ws.close(code=1000, message=b"ok")
    finally:
        if bot_id and bot_id in clients and clients[bot_id][1] is ws:
            # Keep entry for FCM when app is closed; set ws to None.
            clients[bot_id] = (clients[bot_id][0], None, clients[bot_id][2])
            print(f"  Bot disconnected (FCM still active): {bot_id}")

    return ws


async def root_handler(request: web.Request) -> web.StreamResponse:
    if request.headers.get("Upgrade", "").lower() == "websocket":
        return await handle_websocket(request)
    return web.Response(text="ok")


async def on_startup(_: web.Application) -> None:
    port = int(os.environ.get("PORT", 5050))
    print(f"Cloud relay: WebSocket+HTTP port {port}")
    print(
        "Link by Bot ID + API Key. FCM push when app closed: "
        + ("enabled" if FCM_SERVER_KEY else "disabled (set FCM_SERVER_KEY)")
    )


def main() -> None:
    port = int(os.environ.get("PORT", 5050))
    app = web.Application()
    app.on_startup.append(on_startup)
    app.router.add_route("*", "/", root_handler)
    web.run_app(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
