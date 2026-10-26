from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
import re
import uuid
import urllib.request
import urllib.error
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
HA_COMMAND_REQUEST_TIMEOUT_S = 10.0


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
        "attributes": {**(attributes if isinstance(attributes, dict) else {}),
                       **({"_ha_last_updated": entity["last_updated"]} if entity.get("last_updated") else {})},
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
        self.http_url = ""
        self.token = ""
        self.registry = {}
        self.on_inventory = None
        self.on_activity = None
        self._registry_refresh_task = None
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
            max_size=16 * 1024 * 1024,
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
        session = cls(ws, on_state_update=on_state_update)
        parsed = urlparse(ha_url)
        session.http_url = parsed._replace(scheme="https" if parsed.scheme == "wss" else "http", path="", query="", fragment="").geturl()
        session.token = token
        return session

    async def close(self) -> None:
        if self._registry_refresh_task:
            self._registry_refresh_task.cancel()
            await asyncio.gather(self._registry_refresh_task, return_exceptions=True)
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
                    entities.append(self.enrich(entity))
        return entities

    def enrich(self, entity):
        return {**entity, "attributes": {**entity["attributes"], **self.registry.get(entity["entity_id"], {})}}

    async def inventory_metadata(self):
        config = await self.request({"type": "get_config"}, timeout_s=30)
        try:
            areas = {a["area_id"]: a["name"] for a in await self.request({"type": "config/area_registry/list"}, timeout_s=30)}
            devices = {d["id"]: d for d in await self.request({"type": "config/device_registry/list"}, timeout_s=30)}
            entities = await self.request({"type": "config/entity_registry/list"}, timeout_s=30)
            self.registry = {}
            for entity in entities:
                device = devices.get(entity.get("device_id"), {})
                area = entity.get("area_id") or device.get("area_id")
                self.registry[entity["entity_id"]] = {k: v for k, v in {
                    "device_id": entity.get("device_id"), "area_id": area,
                    "area_name": areas.get(area), "device_name": device.get("name_by_user") or device.get("name"),
                    "registry_id": entity.get("id"), "unique_id": entity.get("unique_id"),
                }.items() if v is not None}
        except RuntimeError:
            logger.warning("Device registry unavailable; publishing entity functions without room metadata")
        metadata = {"type": "metadata", "timezone": config.get("time_zone"), "ha_version": config.get("version"), "stub": False}
        if config.get("latitude") is not None and config.get("longitude") is not None:
            metadata["location"] = {"name": str(config.get("location_name") or "Home Assistant home")[:100],
                                    "latitude": config["latitude"], "longitude": config["longitude"],
                                    "elevation": config.get("elevation", 0)}
        return metadata

    async def config_http(self, method, config_id, data=None):
        url = f"{self.http_url}/api/config/automation/config/{config_id}"
        def perform():
            request = urllib.request.Request(url, method=method, data=json.dumps(data).encode() if data is not None else None,
                headers={"Authorization": "Bearer " + self.token, "Content-Type": "application/json"})
            try:
                with urllib.request.urlopen(request, timeout=8) as response:
                    return json.load(response)
            except urllib.error.HTTPError as exc:
                if exc.code == 404 and method == "GET":
                    return None
                raise RuntimeError(f"Home Assistant configuration rejected ({exc.code}); check automation configuration and administrator permissions") from None
        return await asyncio.to_thread(perform)

    async def apply_automation(self, args):
        config_id = args.get("config_id", "")
        if not re.fullmatch(r"semantic_home_[a-f0-9]{32}", config_id):
            raise RuntimeError("Only Semantic Home automations may be managed")
        rule_id = str(uuid.UUID(config_id.removeprefix("semantic_home_")))
        marker = f"Managed by Semantic Home; id={rule_id};"
        existing = await self.config_http("GET", config_id)
        if existing and not existing.get("description", "").startswith(marker):
            raise RuntimeError("Refusing to modify an automation without the matching ownership marker")
        registry = await self.request({"type": "config/entity_registry/list"}, timeout_s=8)
        entity_id = next((e["entity_id"] for e in registry if e.get("unique_id") == config_id and e["entity_id"].startswith("automation.")), None)
        if args.get("delete"):
            if entity_id:
                await self.call_service("automation", "turn_off", {"entity_id": entity_id, "stop_actions": True})
            if existing:
                await self.config_http("DELETE", config_id)
            return {"deleted": True, "config_id": config_id}
        config = args.get("config", {})
        if config.get("id") != config_id or not config.get("description", "").startswith(marker):
            raise RuntimeError("Invalid automation ownership marker")
        ha_config = await self.request({"type": "get_config"}, timeout_s=8)
        if config.get("initial_state") and ha_config.get("time_zone") != args.get("timezone"):
            raise RuntimeError("Household and Home Assistant timezones differ")
        current = {e["entity_id"]: e.get("id") for e in registry}
        for entity, expected in config.get("variables", {}).get("semantic_home_bindings", {}).items():
            if expected and current.get(entity) != expected:
                if entity_id:
                    await self.call_service("automation", "turn_off", {"entity_id": entity_id, "stop_actions": True})
                raise RuntimeError("A device identity changed; reselect it before activating this automation")
        if existing != config:
            await self.config_http("POST", config_id, config)
            await self.call_service("automation", "reload", {"id": config_id})
        registry = await self.request({"type": "config/entity_registry/list"}, timeout_s=8)
        entity_id = next((e["entity_id"] for e in registry if e.get("unique_id") == config_id and e["entity_id"].startswith("automation.")), None)
        if not entity_id:
            raise RuntimeError("Configuration saved, but Home Assistant did not load the automation")
        enabled = bool(config.get("initial_state"))
        await self.call_service("automation", "turn_on" if enabled else "turn_off", {"entity_id": entity_id, **({} if enabled else {"stop_actions": True})})
        states = await self.get_states()
        actual = next((s["state"] for s in states if s["entity_id"] == entity_id), None)
        if actual != ("on" if enabled else "off"):
            raise RuntimeError("Home Assistant did not confirm the requested activation state")
        return {"config_id": config_id, "entity_id": entity_id, "state": actual}

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

    async def watch_inventory(self):
        for event in ("entity_registry_updated", "device_registry_updated", "area_registry_updated", "core_config_updated", "semantic_home_action"):
            await self.request({"type": "subscribe_events", "event_type": event}, timeout_s=8)

    async def refresh_inventory(self):
        try:
            await asyncio.sleep(.2)
            metadata = await self.inventory_metadata()
            registry = await self.request({"type": "config/entity_registry/list"}, timeout_s=8)
            identities = {e["entity_id"]: e.get("id") for e in registry}
            for entity in registry:
                config_id = entity.get("unique_id", "")
                if not entity["entity_id"].startswith("automation.") or not re.fullmatch(r"semantic_home_[a-f0-9]{32}", config_id):
                    continue
                config = await self.config_http("GET", config_id)
                if not config or not config.get("description", "").startswith("Managed by Semantic Home;"):
                    continue
                bindings = config.get("variables", {}).get("semantic_home_bindings", {})
                if any(expected and identities.get(eid) != expected for eid, expected in bindings.items()):
                    await self.call_service("automation", "turn_off", {"entity_id": entity["entity_id"], "stop_actions": True})
            if self.on_inventory:
                await self.on_inventory(metadata, await self.get_states())
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.warning("Could not refresh device inventory; it will be retried on reconnection")

    async def _handle_event(self, msg: dict[str, Any]) -> None:
        event = msg.get("event")
        if not isinstance(event, dict):
            return
        if event.get("event_type") == "semantic_home_action":
            if self.on_activity:
                await self.on_activity(event.get("data", {}))
            return
        if event.get("event_type") in {"entity_registry_updated", "device_registry_updated", "area_registry_updated", "core_config_updated"}:
            if self._registry_refresh_task is None or self._registry_refresh_task.done():
                self._registry_refresh_task = asyncio.create_task(self.refresh_inventory())
            return
        if event.get("event_type") != "state_changed":
            return

        data = event.get("data")
        if not isinstance(data, dict):
            return

        new_state = data.get("new_state")
        if not isinstance(new_state, dict):
            if isinstance(data.get("entity_id"), str):
                await self._on_state_update(self.enrich({"entity_id": data["entity_id"], "state": "unavailable", "attributes": {}}))
            return

        entity = normalize_entity_payload(new_state)
        if entity is None:
            return

        await self._on_state_update(self.enrich(entity))


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
        if name == "automation.apply":
            if ha_session is None or not isinstance(args, dict):
                raise RuntimeError("A real Home Assistant connection is required to deploy automations")
            result = await ha_session.apply_automation(args)
            await backend.send_json({"type": "ack", "id": cmd_id, "ok": True, "result": result})
            return
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

    if name != "ha.call_service":
        await backend.send_json({"type": "ack", "id": cmd_id, "ok": False, "error": "Stub mode cannot deploy automations"})
        return
    logger.info("[agent %s] Simulated command %s", agent_id, name)
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
    command_queue = asyncio.Queue(maxsize=32)

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
            cmd_id, name, args, expires_at = await command_queue.get()
            try:
                if not isinstance(expires_at, (int, float)) or time.time() >= expires_at:
                    await backend.send_json({"type": "ack", "id": cmd_id, "ok": False, "error": "Command expired"})
                    continue
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
            logger.debug("Received backend message type=%s", msg.get("type"))
            msg_type = msg.get("type")

            if msg_type == "command":
                cmd_id = msg.get("id")
                name = msg.get("name")
                args = msg.get("args", {})

                try:
                    command_queue.put_nowait((cmd_id, name, args, msg.get("expires_at")))
                except asyncio.QueueFull:
                    await backend.send_json({"type": "ack", "id": cmd_id, "ok": False, "error": "Agent busy"})
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


def _backend_connect_headers(beta_key: str, agent_token: str = "") -> list[tuple[str, str]] | None:
    if agent_token:
        return [("Authorization", "Bearer " + agent_token)]
    if not beta_key:
        return None
    return [("X-Semantic-Home-Beta-Key", beta_key)]


def _redact_backend_url_for_logs(url: str) -> str:
    parsed = urlparse(url)
    return parsed._replace(query="", fragment="").geturl()


async def connect_and_run(
    url: str,
    agent_id: str,
    *,
    ha_url: str | None,
    ha_token: str | None,
    beta_key: str,
    agent_token: str = "",
) -> None:
    logger.info("Connecting agent %s to %s", agent_id, _redact_backend_url_for_logs(url))
    async with websockets.connect(
        url,
        max_size=16 * 1024 * 1024,
        additional_headers=_backend_connect_headers(beta_key, agent_token),
        ping_interval=None,
        ping_timeout=None,
        open_timeout=None,
        close_timeout=None,
    ) as ws:
        backend = BackendConnection(ws)
        ha_session: HomeAssistantSession | None = None

        try:
            if ha_url and ha_token:
                initializing = True
                buffered = {}
                async def forward_state_update(entity: dict[str, Any]) -> None:
                    if initializing:
                        buffered[entity["entity_id"]] = entity
                    else:
                        await backend.send_json({"type": "state_update", "entity": entity})

                ha_session = await HomeAssistantSession.connect(
                    ha_url,
                    ha_token,
                    on_state_update=forward_state_update,
                )
                await backend.send_json(await ha_session.inventory_metadata())
                await ha_session.subscribe_state_changes()
                snapshot_entities = await ha_session.get_states()
                await backend.send_json({"type": "entities_snapshot", "entities": snapshot_entities})
                while buffered:
                    _, entity = buffered.popitem()
                    await backend.send_json({"type": "state_update", "entity": entity})
                initializing = False
                async def forward_inventory(metadata, entities):
                    await backend.send_json(metadata)
                    await backend.send_json({"type": "entities_snapshot", "entities": entities})
                ha_session.on_inventory = forward_inventory
                async def forward_activity(data):
                    await backend.send_json({"type": "automation_event", "data": data})
                ha_session.on_activity = forward_activity
                await ha_session.watch_inventory()
                logger.info("Sent Home Assistant entities_snapshot (%d entities)", len(snapshot_entities))
            else:
                await backend.send_json({"type": "metadata", "stub": True})
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
    parser.add_argument("--agent-token", default=os.getenv("SEMANTIC_HOME_AGENT_TOKEN", ""))
    parser.add_argument("--beta-key", default=os.getenv("SEMANTIC_HOME_BETA_KEY", ""))
    args = parser.parse_args()
    if not args.agent_token.strip():
        parser.error("Create an individual connection in Semantic Home and set SEMANTIC_HOME_AGENT_TOKEN")

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
    beta_key = args.beta_key.strip()

    attempt = 0
    while True:
        try:
            await connect_and_run(
                url,
                args.id,
                ha_url=normalized_ha_url,
                ha_token=ha_token,
                beta_key=beta_key,
                agent_token=args.agent_token.strip(),
            )
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
