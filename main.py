from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import random
import time
from contextlib import suppress
from typing import Any, Awaitable, Callable
from urllib.parse import urlencode, urlparse

import websockets
from websockets.exceptions import ConnectionClosed, ConnectionClosedError, ConnectionClosedOK


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)

logger = logging.getLogger(__name__)

PING_EVERY_S = 3000
HA_STARTUP_REQUEST_TIMEOUT_S = 30.0
HA_COMMAND_REQUEST_TIMEOUT_S = 4.0


def scan_entities_stub() -> list[dict[str, Any]]:
    return [
        {
            "entity_id": "switch.balcony_pump",
            "state": "off",
            "attributes": {"friendly_name": "Balcony Pump"},
        },
        {
            "entity_id": "sensor.balcony_moisture",
            "state": "22",
            "attributes": {"friendly_name": "Balcony Moisture"},
        },
    ]


def normalize_entity_payload(entity: dict[str, Any]) -> dict[str, Any] | None:
    entity_id = entity.get("entity_id")
    if not isinstance(entity_id, str) or not entity_id:
        return None

    state = entity.get("state")
    attributes = entity.get("attributes")

    return {
        "entity_id": entity_id,
        "state": str(state) if state is not None else "unknown",
        "attributes": attributes if isinstance(attributes, dict) else {},
    }


def normalize_ha_websocket_url(raw_url: str) -> str:
    parsed = urlparse(raw_url)
    if parsed.scheme not in {"http", "https", "ws", "wss"}:
        raise ValueError("Home Assistant URL must start with http://, https://, ws:// or wss://")

    scheme = parsed.scheme
    if scheme == "http":
        scheme = "ws"
    elif scheme == "https":
        scheme = "wss"

    path = parsed.path.rstrip("/")
    if not path or path == "/":
        path = "/api/websocket"
    elif not path.endswith("/api/websocket"):
        path = f"{path}/api/websocket"

    return parsed._replace(scheme=scheme, path=path, params="", query="", fragment="").geturl()


class BackendConnection:
    def __init__(self, ws: websockets.WebSocketClientProtocol) -> None:
        self._ws = ws
        self._send_lock = asyncio.Lock()

    async def recv_json(self) -> dict[str, Any]:
        raw = await self._ws.recv()
        return json.loads(raw)

    async def send_json(self, payload: dict[str, Any]) -> None:
        async with self._send_lock:
            await self._ws.send(json.dumps(payload))


class HomeAssistantSession:
    def __init__(
        self,
        ws: websockets.WebSocketClientProtocol,
        *,
        on_state_update: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> None:
        self._ws = ws
        self._on_state_update = on_state_update
        self._next_request_id = 1
        self._pending: dict[int, asyncio.Future[Any]] = {}
        self._send_lock = asyncio.Lock()
        self._reader_task = asyncio.create_task(self._reader_loop())

    @classmethod
    async def connect(
        cls,
        ha_url: str,
        token: str,
        *,
        on_state_update: Callable[[dict[str, Any]], Awaitable[None]],
    ) -> HomeAssistantSession:
        logger.info("Connecting to Home Assistant at %s", ha_url)
        ws = await websockets.connect(
            ha_url,
            ping_interval=20,
            ping_timeout=20,
            open_timeout=None,
            close_timeout=None,
        )

        raw = await ws.recv()
        msg = json.loads(raw)
        if msg.get("type") != "auth_required":
            await ws.close()
            raise RuntimeError(f"Unexpected Home Assistant auth handshake: {msg}")

        await ws.send(json.dumps({"type": "auth", "access_token": token}))
        raw = await ws.recv()
        msg = json.loads(raw)
        if msg.get("type") != "auth_ok":
            await ws.close()
            raise RuntimeError(f"Home Assistant auth failed: {msg}")

        logger.info("Authenticated with Home Assistant")
        return cls(ws, on_state_update=on_state_update)

    async def close(self) -> None:
        self._reader_task.cancel()
        await asyncio.gather(self._reader_task, return_exceptions=True)
        await self._ws.close()

    async def wait_until_closed(self) -> None:
        try:
            await self._reader_task
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            raise RuntimeError("Home Assistant websocket closed") from exc
        raise RuntimeError("Home Assistant websocket closed")

    async def get_states(self) -> list[dict[str, Any]]:
        result = await self.request(
            {"type": "get_states"},
            timeout_s=HA_STARTUP_REQUEST_TIMEOUT_S,
        )
        entities: list[dict[str, Any]] = []
        if not isinstance(result, list):
            return entities

        for item in result:
            if isinstance(item, dict):
                entity = normalize_entity_payload(item)
                if entity is not None:
                    entities.append(entity)
        return entities

    async def subscribe_state_changes(self) -> None:
        await self.request(
            {"type": "subscribe_events", "event_type": "state_changed"},
            timeout_s=HA_STARTUP_REQUEST_TIMEOUT_S,
        )
        logger.info("Subscribed to Home Assistant state_changed events")

    async def call_service(self, domain: str, service: str, data: dict[str, Any]) -> Any:
        payload: dict[str, Any] = {
            "type": "call_service",
            "domain": domain,
            "service": service,
        }
        if data:
            service_data = dict(data)
            target: dict[str, Any] = {}

            for key in ("entity_id", "device_id", "area_id", "label_id", "floor_id"):
                value = service_data.pop(key, None)
                if value is not None:
                    target[key] = value

            if service_data:
                payload["service_data"] = service_data
            if target:
                payload["target"] = target
        return await self.request(payload, timeout_s=HA_COMMAND_REQUEST_TIMEOUT_S)

    async def request(self, payload: dict[str, Any], *, timeout_s: float | None = None) -> Any:
        request_id = self._next_request_id
        self._next_request_id += 1
        loop = asyncio.get_running_loop()
        future: asyncio.Future[Any] = loop.create_future()
        self._pending[request_id] = future

        try:
            async with self._send_lock:
                await self._ws.send(json.dumps({"id": request_id, **payload}))
            if timeout_s is None:
                return await future
            return await asyncio.wait_for(asyncio.shield(future), timeout=timeout_s)
        except asyncio.TimeoutError as exc:
            future.cancel()
            raise RuntimeError("Home Assistant request timed out") from exc
        finally:
            self._pending.pop(request_id, None)

    async def _reader_loop(self) -> None:
        try:
            while True:
                raw = await self._ws.recv()
                msg = json.loads(raw)
                msg_type = msg.get("type")

                if msg_type == "result":
                    request_id = msg.get("id")
                    future = self._pending.get(request_id)
                    if future is None or future.done():
                        continue
                    if msg.get("success", False):
                        future.set_result(msg.get("result"))
                    else:
                        error = msg.get("error")
                        if isinstance(error, dict):
                            message = error.get("message") or error.get("code") or "Home Assistant request failed"
                        else:
                            message = str(error or "Home Assistant request failed")
                        future.set_exception(RuntimeError(message))
                    continue

                if msg_type == "event":
                    await self._handle_event(msg)
                    continue

                logger.debug("Ignoring Home Assistant websocket message: %s", msg)
        except asyncio.CancelledError:
            raise
        finally:
            error = RuntimeError("Home Assistant websocket closed")
            for future in list(self._pending.values()):
                if not future.done():
                    future.set_exception(error)

    async def _handle_event(self, msg: dict[str, Any]) -> None:
        event = msg.get("event")
        if not isinstance(event, dict) or event.get("event_type") != "state_changed":
            return

        data = event.get("data")
        if not isinstance(data, dict):
            return

        new_state = data.get("new_state")
        if not isinstance(new_state, dict):
            return

        entity = normalize_entity_payload(new_state)
        if entity is None:
            return

        await self._on_state_update(entity)


async def execute_ha_command(
    backend: BackendConnection,
    ha_session: HomeAssistantSession | None,
    *,
    cmd_id: Any,
    name: Any,
    args: Any,
) -> None:
    if not isinstance(cmd_id, str):
        logger.warning("Skipping command without string id: %s", {"id": cmd_id, "name": name})
        return

    try:
        if name != "ha.call_service":
            raise RuntimeError(f"Unsupported command {name!r}")
        if ha_session is None:
            raise RuntimeError("Agent is not connected to Home Assistant")
        if not isinstance(args, dict):
            raise RuntimeError("Command args must be an object")

        domain = args.get("domain")
        service = args.get("service")
        data = args.get("data", {})

        if not isinstance(domain, str) or not isinstance(service, str):
            raise RuntimeError("ha.call_service requires string domain and service")
        if not isinstance(data, dict):
            raise RuntimeError("ha.call_service data must be an object")

        result = await ha_session.call_service(domain, service, data)
        await backend.send_json(
            {
                "type": "ack",
                "id": cmd_id,
                "ok": True,
                "result": {
                    "executed": name,
                    "ha_result": result,
                },
            }
        )
    except Exception as exc:
        logger.warning("Command %s failed: %s", name, exc)
        await backend.send_json(
            {
                "type": "ack",
                "id": cmd_id,
                "ok": False,
                "error": str(exc),
            }
        )


async def execute_stub_command(
    backend: BackendConnection,
    agent_id: str,
    *,
    cmd_id: Any,
    name: Any,
    args: Any,
) -> None:
    if not isinstance(cmd_id, str):
        logger.warning("Skipping stub command without string id: %s", {"id": cmd_id, "name": name})
        return

    logger.info("[agent %s] EXEC %s args=%s", agent_id, name, args)
    await backend.send_json(
        {
            "type": "ack",
            "id": cmd_id,
            "ok": True,
            "result": {"executed": name},
        }
    )

    if name != "ha.call_service" or not isinstance(args, dict):
        return

    service = args.get("service")
    data = args.get("data", {})
    entity_id = data.get("entity_id") if isinstance(data, dict) else None

    next_state: str | None = None
    if service == "turn_on":
        next_state = "on"
    elif service == "turn_off":
        next_state = "off"

    if isinstance(entity_id, str) and next_state is not None:
        await backend.send_json(
            {
                "type": "state_update",
                "entity": {
                    "entity_id": entity_id,
                    "state": next_state,
                    "attributes": {},
                },
            }
        )


async def execute_command(
    backend: BackendConnection,
    agent_id: str,
    *,
    ha_session: HomeAssistantSession | None,
    cmd_id: Any,
    name: Any,
    args: Any,
) -> None:
    if ha_session is not None:
        await execute_ha_command(
            backend,
            ha_session,
            cmd_id=cmd_id,
            name=name,
            args=args,
        )
        return

    await execute_stub_command(
        backend,
        agent_id,
        cmd_id=cmd_id,
        name=name,
        args=args,
    )


async def execute_command_task(
    backend: BackendConnection,
    agent_id: str,
    *,
    ha_session: HomeAssistantSession | None,
    cmd_id: Any,
    name: Any,
    args: Any,
) -> None:
    try:
        await execute_command(
            backend,
            agent_id,
            ha_session=ha_session,
            cmd_id=cmd_id,
            name=name,
            args=args,
        )
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Unexpected command execution failure")


async def run_agent(
    backend: BackendConnection,
    agent_id: str,
    *,
    ha_session: HomeAssistantSession | None,
) -> None:
    ha_watch_task = (
        asyncio.create_task(ha_session.wait_until_closed())
        if ha_session is not None
        else None
    )
    command_queue: asyncio.Queue[tuple[Any, Any, Any]] = asyncio.Queue()

    async def ping_loop() -> None:
        try:
            while True:
                await asyncio.sleep(PING_EVERY_S)
                await backend.send_json({"type": "ping", "ts": time.time()})
                logger.info("Sent ping to cloud for agent %s", agent_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.debug("Ping loop stopped due to send error", exc_info=True)

    async def command_worker() -> None:
        while True:
            cmd_id, name, args = await command_queue.get()
            try:
                await execute_command_task(
                    backend,
                    agent_id,
                    ha_session=ha_session,
                    cmd_id=cmd_id,
                    name=name,
                    args=args,
                )
            finally:
                command_queue.task_done()

    ping_task = asyncio.create_task(ping_loop())
    command_worker_task = asyncio.create_task(command_worker())

    try:
        while True:
            backend_recv_task = asyncio.create_task(backend.recv_json())
            wait_tasks: set[asyncio.Task[Any]] = {backend_recv_task}
            if ha_watch_task is not None:
                wait_tasks.add(ha_watch_task)

            done, _ = await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_COMPLETED)

            if ha_watch_task is not None and ha_watch_task in done:
                backend_recv_task.cancel()
                with suppress(asyncio.CancelledError):
                    await backend_recv_task
                await ha_watch_task

            msg = await backend_recv_task
            logger.info("Received message from cloud: %s", msg)
            msg_type = msg.get("type")

            if msg_type == "command":
                cmd_id = msg.get("id")
                name = msg.get("name")
                args = msg.get("args", {})

                await command_queue.put((cmd_id, name, args))
                continue

            if msg_type == "ping":
                await backend.send_json({"type": "pong", "ts": time.time()})
                continue

            if msg_type == "pong":
                continue

            logger.warning("[agent %s] unknown message type=%s msg=%s", agent_id, msg_type, msg)
    finally:
        if ha_watch_task is not None:
            ha_watch_task.cancel()
            await asyncio.gather(ha_watch_task, return_exceptions=True)

        command_worker_task.cancel()
        await asyncio.gather(command_worker_task, return_exceptions=True)

        ping_task.cancel()
        with suppress(asyncio.CancelledError):
            await ping_task


async def connect_and_run(url: str, agent_id: str, *, ha_url: str | None, ha_token: str | None) -> None:
    logger.info("Connecting agent %s to %s", agent_id, url)
    async with websockets.connect(
        url,
        ping_interval=None,
        ping_timeout=None,
        open_timeout=None,
        close_timeout=None,
    ) as ws:
        backend = BackendConnection(ws)
        ha_session: HomeAssistantSession | None = None

        try:
            if ha_url and ha_token:
                async def forward_state_update(entity: dict[str, Any]) -> None:
                    await backend.send_json({"type": "state_update", "entity": entity})

                ha_session = await HomeAssistantSession.connect(
                    ha_url,
                    ha_token,
                    on_state_update=forward_state_update,
                )
                snapshot_entities = await ha_session.get_states()
                await backend.send_json({"type": "entities_snapshot", "entities": snapshot_entities})
                logger.info("Sent Home Assistant entities_snapshot (%d entities)", len(snapshot_entities))
                await ha_session.subscribe_state_changes()
            else:
                snapshot_entities = scan_entities_stub()
                await backend.send_json({"type": "entities_snapshot", "entities": snapshot_entities})
                logger.info("Sent stub entities_snapshot (%d entities)", len(snapshot_entities))

            await run_agent(backend, agent_id, ha_session=ha_session)
        finally:
            if ha_session is not None:
                await ha_session.close()


async def async_main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--base-url", default="ws://localhost:8000")
    parser.add_argument("--id", default="dev1")
    parser.add_argument("--name", default="")
    parser.add_argument("--description", default="")
    parser.add_argument("--ha-url", default=os.getenv("HA_URL", ""))
    parser.add_argument("--ha-token", default=os.getenv("HA_TOKEN", ""))
    args = parser.parse_args()

    query = urlencode(
        {k: v for k, v in {"name": args.name, "description": args.description}.items() if v}
    )

    url = f"{args.base_url.rstrip('/')}/ws/agent/{args.id}"
    if query:
        url = f"{url}?{query}"

    ha_url = args.ha_url or None
    ha_token = args.ha_token or None
    if bool(ha_url) != bool(ha_token):
        raise SystemExit("Provide both --ha-url and --ha-token, or neither for stub mode.")

    normalized_ha_url = normalize_ha_websocket_url(ha_url) if ha_url else None

    attempt = 0
    while True:
        try:
            attempt = 0
            await connect_and_run(url, args.id, ha_url=normalized_ha_url, ha_token=ha_token)
        except (ConnectionClosedOK, ConnectionClosedError, ConnectionClosed) as exc:
            code = getattr(exc, "code", None)
            reason = getattr(exc, "reason", None)
            logger.warning("WebSocket closed (code=%s, reason=%r). Reconnecting...", code, reason)
        except OSError as exc:
            logger.warning("Network/OS error: %s. Reconnecting...", exc)
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Unexpected error. Reconnecting...")

        attempt += 1
        backoff = min(30.0, 2.0 ** min(attempt, 5))
        jitter = random.uniform(0.0, 0.5)
        await asyncio.sleep(backoff + jitter)


def main() -> None:
    asyncio.run(async_main())


if __name__ == "__main__":
    main()
