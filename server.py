"""
CuraX Cloud Relay — WebSocket-only alert bridge (no FCM / push).

Flow:
  1. Phone app opens wss://<relay>/ and sends {"bot_id","api_key"} → registered in `clients`.
  2. Backend (Vercel) sends one-shot WS: {"action":"alert","bot_id","api_key","type","message",...}
  3. Relay forwards JSON to that phone's open WebSocket → in-app popup (AlertConnectionService).

Used for: user→admin (+15/+30 missed dose, stock, expiry) and admin hub alerts.
Dose reminders stay on-device (AlarmManager); relay is not used for routine dose times.
"""
from __future__ import annotations

import asyncio
import json
import os
from typing import Dict, Optional, Tuple

from aiohttp import WSMsgType, web

# bot_id -> (api_key, websocket)
clients: Dict[str, Tuple[str, Optional[web.WebSocketResponse]]] = {}


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

        # Backend inject: one-shot alert delivery to a linked app.
        if data.get("action") == "alert":
            bid = (data.get("bot_id") or "").strip()
            akey = (data.get("api_key") or "").strip()
            atype = data.get("type", "alert")
            amsg = data.get("message", "")
            alert_obj = {"type": atype, "message": amsg}
            un_in = (data.get("user_name") or "").strip()
            if un_in:
                alert_obj["user_name"] = un_in
            payload = json.dumps(alert_obj)
            print(f"  [relay] alert for bot_id={bid[:8]}… type={atype}")
            try:
                sent = False
                entry = clients.get(bid) if (bid in clients and clients[bid][0] == akey) else None
                ws_sock = entry[1] if entry else None
                if ws_sock is not None and not ws_sock.closed:
                    try:
                        await ws_sock.send_str(payload)
                        sent = True
                        print(f"  [relay] -> WebSocket delivered to {bid[:8]}…")
                    except Exception as e:
                        print(f"  [relay] WebSocket send error: {e}")
                        if entry is not None:
                            clients[bid] = (entry[0], None)
                if not sent:
                    print(f"  [relay] -> not delivered ({bid[:8]}… not connected)")
            except Exception as e:
                print(f"  [relay] forward error: {e}")

            await ws.close(code=1000, message=b"ok")
            return ws

        # Phone: long-lived register (bot_id + api_key).
        if not bot_id or not api_key:
            await ws.close(code=4000, message=b"bot_id and api_key required")
            return ws

        clients[bot_id] = (api_key, ws)
        print(f"  [relay] linked {bot_id[:8]}… ({len(clients)} client(s))")

        async for msg in ws:
            if msg.type in (WSMsgType.CLOSED, WSMsgType.CLOSE, WSMsgType.ERROR):
                break

    except asyncio.TimeoutError:
        await ws.close(code=4001, message=b"send bot_id and api_key first")
    except Exception as e:
        print(f"  [relay] WS error: {e}")
        await ws.close(code=1000, message=b"ok")
    finally:
        if bot_id and bot_id in clients and clients[bot_id][1] is ws:
            del clients[bot_id]
            print(f"  [relay] unlinked {bot_id[:8]}…")

    return ws


async def status_handler(request: web.Request) -> web.StreamResponse:
    linked = len(clients)
    return web.json_response({
        "relay": "ok",
        "delivery": "websocket",
        "linked_clients": linked,
    })


async def root_handler(request: web.Request) -> web.StreamResponse:
    if request.headers.get("Upgrade", "").lower() == "websocket":
        return await handle_websocket(request)
    return web.Response(
        text="CuraX relay (WebSocket). Apps: wss://… + {bot_id,api_key}. Backend: {action:alert,…}",
    )


async def on_startup(_: web.Application) -> None:
    port = int(os.environ.get("PORT", 5050))
    print(f"CuraX relay: WebSocket on port {port} (no FCM)")


def main() -> None:
    port = int(os.environ.get("PORT", 5050))
    app = web.Application()
    app.on_startup.append(on_startup)
    app.router.add_get("/status", status_handler)
    app.router.add_route("*", "/", root_handler)
    web.run_app(app, host="0.0.0.0", port=port)


if __name__ == "__main__":
    main()
