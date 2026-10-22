from __future__ import annotations

import asyncio
import importlib.util
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("semantic_agent", Path(__file__).parents[1] / "main.py")
agent = importlib.util.module_from_spec(spec)
spec.loader.exec_module(agent)

CONFIG_ID = "semantic_home_012345670123412381230123456789ab"
MARKER = "Managed by Semantic Home; id=01234567-0123-4123-8123-0123456789ab; version=1"


class AgentAutomationTests(unittest.IsolatedAsyncioTestCase):
    def fake(self, *, existing=None, actual="on", registry_id="registry-1", zone="Europe/Berlin"):
        session = object.__new__(agent.HomeAssistantSession)
        session.writes = []
        session.calls = []
        async def config_http(method, config_id, data=None):
            if method == "GET": return existing
            session.writes.append((method, config_id, data))
            return {"result": "ok"}
        async def request(payload, **kwargs):
            if payload["type"] == "get_config": return {"time_zone": zone}
            return [{"entity_id": "automation.keller", "unique_id": CONFIG_ID}, {"entity_id": "switch.keller", "id": registry_id}]
        async def call_service(domain, service, data): session.calls.append((domain, service, data))
        async def get_states(): return [{"entity_id": "automation.keller", "state": actual}]
        session.config_http, session.request, session.call_service, session.get_states = config_http, request, call_service, get_states
        return session

    def args(self, enabled=True):
        return {"config_id": CONFIG_ID, "timezone": "Europe/Berlin", "config": {"id": CONFIG_ID, "description": MARKER,
                "initial_state": enabled, "variables": {"semantic_home_bindings": {"switch.keller": "registry-1"}}}}

    async def test_deployment_confirms_loaded_state(self):
        session = self.fake()
        result = await session.apply_automation(self.args())
        self.assertEqual(result["state"], "on")
        self.assertEqual(session.writes[0][0], "POST")
        self.assertIn(("automation", "reload", {"id": CONFIG_ID}), session.calls)

    async def test_same_version_is_idempotent(self):
        args = self.args()
        session = self.fake(existing=args["config"])
        await session.apply_automation(args)
        self.assertEqual(session.writes, [])

    async def test_foreign_configs_and_foreign_ids_are_never_overwritten(self):
        session = self.fake(existing={"description": "My own HA automation"})
        with self.assertRaises(RuntimeError): await session.apply_automation(self.args())
        self.assertEqual(session.writes, [])
        with self.assertRaises(RuntimeError): await session.apply_automation({"config_id": "foreign_automation"})

    async def test_changed_device_identity_disables_old_rule_and_rejects_replacement(self):
        session = self.fake(registry_id="replacement-device")
        with self.assertRaisesRegex(RuntimeError, "identity changed"):
            await session.apply_automation(self.args())
        self.assertEqual(session.writes, [])
        self.assertTrue(any(call[1] == "turn_off" for call in session.calls))

    async def test_inactive_or_wrong_timezone_is_not_reported_active(self):
        with self.assertRaises(RuntimeError): await self.fake(actual="off").apply_automation(self.args())
        with self.assertRaises(RuntimeError): await self.fake(zone="UTC").apply_automation(self.args())
        result = await self.fake(zone="UTC", actual="off").apply_automation(self.args(enabled=False))
        self.assertEqual(result["state"], "off")

    async def test_delete_is_owned_and_idempotent(self):
        session = self.fake(existing={"description": MARKER})
        result = await session.apply_automation({"config_id": CONFIG_ID, "delete": True})
        self.assertTrue(result["deleted"])
        self.assertEqual(session.writes[0][0], "DELETE")
        session = self.fake()
        await session.apply_automation({"config_id": CONFIG_ID, "delete": True})
        self.assertEqual(session.writes, [])

    async def test_stub_cannot_claim_real_deployment(self):
        class Backend:
            messages = []
            async def send_json(self, data): self.messages.append(data)
        backend = Backend()
        await agent.execute_stub_command(backend, "test", cmd_id="x", name="automation.apply", args={})
        self.assertFalse(backend.messages[-1]["ok"])

    async def test_expired_queued_commands_are_rejected(self):
        class Backend:
            def __init__(self): self.messages = []; self.received = False
            async def recv_json(self):
                if not self.received:
                    self.received = True
                    return {"type": "command", "id": "expired", "name": "ha.call_service", "args": {}, "expires_at": 1}
                await asyncio.sleep(10)
            async def send_json(self, data): self.messages.append(data)
        backend = Backend()
        task = asyncio.create_task(agent.run_agent(backend, "test", ha_session=None))
        for _ in range(20):
            await asyncio.sleep(.01)
            if backend.messages: break
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)
        self.assertEqual(backend.messages[0]["error"], "Command expired")


if __name__ == "__main__":
    unittest.main()
