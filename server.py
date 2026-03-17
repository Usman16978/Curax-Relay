import asyncio
import json
import os
import urllib.request
import urllib.error
from typing import Dict, Optional, Tuple

from aiohttp import WSMsgType, web
from google.auth.transport.requests import Request
from google.oauth2 import service_account

# bot_id -> (api_key, websocket_or_None, fcm_token_or_None)
# When app disconnects we keep (api_key, None, fcm_token) so we can still send via FCM.
clients: Dict[str, Tuple[str, Optional[web.WebSocketResponse], Optional[str]]] = {}

FIREBASE_PROJECT_ID = os.environ.get("FIREBASE_PROJECT_ID", "").strip()
FIREBASE_SERVICE_ACCOUNT_JSON = os.environ.get("FIREBASE_SERVICE_ACCOUNT_JSON", "").strip()
FIREBASE_SCOPE = "https://www.googleapis.com/auth/firebase.messaging"

_fcm_creds = None


def _get_fcm_credentials():
    global _fcm_creds
    if _fcm_creds is not None:
        return _fcm_creds

    if not FIREBASE_SERVICE_ACCOUNT_JSON:
        return None

    try:
        info = json.loads(FIREBASE_SERVICE_ACCOUNT_JSON)
        _fcm_creds = service_account.Credentials.from_service_account_info(
            info,
            scopes=[FIREBASE_SCOPE],
        )
        return _fcm_creds
    except Exception as e:
        print(f"  FCM credentials load error: {e}")
        return None


def _get_access_token() -> Optional[str]:
    creds = _get_fcm_credentials()
    if creds is None:
        return None
    try:
        if not creds.valid or creds.expired or not creds.token:
            creds.refresh(Request())
        return creds.token
    except Exception as e:
        print(f"  FCM token refresh error: {e}")
        return None


def _send_fcm_sync(token: str, alert_type: str, message: str) -> bool:
    """Send FCM message via HTTP v1. Returns True only when accepted by FCM."""
    if not FIREBASE_PROJECT_ID or not token:
        return False

    access_token = _get_access_token()
    if not access_token:
        return False

    url = f"https://fcm.googleapis.com/v1/projects/{FIREBASE_PROJECT_ID}/messages:send"
    # Send notification + data so alerts still show on lock screen even if
    # background data delivery is delayed by OEM/Doze.
    body = {
        "message": {
            "token": token,
            "data": {"type": alert_type, "message": message},
            "notification": {
                "title": "Medicine reminder"
                if "medicine" in alert_type.lower() or alert_type in ("time", "pre")
                else "Alert",
                "body": message,
            },
            "android": {
                "priority": "high",
                "ttl": "120s",
                "notification": {
                    "channel_id": "curax_alert_channel",
                    "sound": "default",
                    "visibility": "public"
                }
            },
        }
    }

    req = urllib.request.Request(
        url,
        data=json.dumps(body).encode("utf-8"),
        headers={
            "Authorization": f"Bearer {access_token}",
            "Content-Type": "application/json; charset=UTF-8",
        },
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as r:
            if not (200 <= r.status < 300):
                return False
            _ = r.read()
            return True
    except urllib.error.HTTPError as e:
        try:
            body = e.read().decode("utf-8", errors="ignore")
        except Exception:
            body = ""
        print(f"  FCM send error: HTTP {e.code} {e.reason} body={body}")
        return False
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

        # Backend sends: action="alert", bot_id, api_key, type, message, fcm_token (from DB).
        # Alert goes directly via FCM when we have fcm_token � no app socket needed. Socket is optional fallback.
        if data.get("action") == "alert":
            bid = (data.get("bot_id") or "").strip()
            akey = (data.get("api_key") or "").strip()
            atype = data.get("type", "alert")
            amsg = data.get("message", "")
            payload = json.dumps({"type": atype, "message": amsg})
            # Prefer fcm_token from payload (backend sends from DB) so FCM works even if app never connected to relay.
            payload_fcm = (data.get("fcm_token") or "").strip() or None

            try:
                sent = False
                entry = clients.get(bid) if (bid in clients and clients[bid][0] == akey) else None
                ws_sock = entry[1] if entry else None
                mem_fcm = entry[2] if entry else None
                fcm_token = payload_fcm or mem_fcm

                if not fcm_token:
                    print(f"  -> No FCM token for {bid} (backend must get token from app save-credentials / FCM active)")

                # Primary path: FCM first (works when screen off / FCM active; no socket needed).
                if fcm_token:
                    if await send_fcm(fcm_token, atype, amsg):
                        sent = True
                        print(f"  -> Pushed via FCM: {bid}")
                    else:
                        print(f"  -> FCM send failed for {bid} (check token/credentials)")

                # Fallback: WebSocket only if FCM failed or no token.
                if not sent and ws_sock is not None and not ws_sock.closed:
                    try:
                        await ws_sock.send_str(payload)
                        sent = True
                        print(f"  -> Forwarded via WebSocket: {bid}")
                    except Exception as e:
                        print(f"  WebSocket send error: {e}")
                        if entry is not None:
                            clients[bid] = (entry[0], None, entry[2])

                if not sent:
                    print(f"  -> Alert not delivered for {bid} (no FCM token or WebSocket)")
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
        print(f"  Bot linked: {bot_id}" + (" (FCM token stored)" if fcm_token else " (NO FCM token)"))
        if fcm_token:
            print(f"  FCM token len={len(fcm_token)} prefix={fcm_token[:12]}")

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


async def status_handler(request: web.Request) -> web.StreamResponse:
    """GET /status ? { "fcm_configured": true|false }. Safe to call to verify FCM env vars are set (no secrets)."""
    creds_ready = bool(FIREBASE_PROJECT_ID and FIREBASE_SERVICE_ACCOUNT_JSON)
    return web.json_response({"fcm_configured": creds_ready, "relay": "ok"})


async def root_handler(request: web.Request) -> web.StreamResponse:
    if request.headers.get("Upgrade", "").lower() == "websocket":
        return await handle_websocket(request)
    return web.Response(text="ok")


async def on_startup(_: web.Application) -> None:
    port = int(os.environ.get("PORT", 5050))
    creds_ready = bool(FIREBASE_PROJECT_ID and FIREBASE_SERVICE_ACCOUNT_JSON)
    print(f"Cloud relay: WebSocket+HTTP port {port}")
    print(
        "Link by Bot ID + API Key. FCM v1 push: "
        + ("enabled" if creds_ready else "disabled (set FIREBASE_PROJECT_ID + FIREBASE_SERVICE_ACCOUNT_JSON)")
    )


def main() -> None:
    port = int(os.environ.get("PORT", 5050))
    app = web.Application()
    app.on_startup.append(on_startup)
    app.router.add_get("/status", status_handler)
    app.router.add_route("*", "/", root_handler)
    web.run_app(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
