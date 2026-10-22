# Semantic Home Agent

The local bridge between one Home Assistant installation and its authenticated Semantic Home household. It imports device/room metadata and state changes, executes supported device actions, and manages only Semantic Home-owned HA automations.

## Pair this version

This release requires an **individual agent credential**, created in the app under **Connections**. The previous shared beta key does not authorize a connection to the new backend. Update frontend, backend and agent together.

Run the updated checkout locally:

```bash
uv sync
export HA_URL="http://YOUR-HOME-ASSISTANT:8123"
export HA_TOKEN="YOUR-LOCAL-HA-ADMINISTRATOR-TOKEN"
export SEMANTIC_HOME_AGENT_TOKEN="ONE-TIME-TOKEN-FROM-CONNECTIONS"
uv run python main.py --base-url "wss://YOUR-SEMANTIC-HOME/api" --id "AGENT-ID-FROM-CONNECTIONS"
```

For local development the backend base URL is `ws://localhost:8000/api/v1`, or the frontend proxy `ws://localhost:5173/api`. To install this checkout as a command, use `uv tool install .` inside this directory. Published packages/repositories must contain these changes before using their install URLs.

HA_TOKEN stays on the local agent. It is never sent to Semantic Home. Automation configuration uses HA's administrator-only configuration API; an HA token without the required permission will produce an error rather than a false activation success.

## Behaviour

- authenticated outbound WebSocket; no inbound router port forwarding is required for the agent
- initial inventory, incremental state changes, registry-change refresh and reconnect
- each device keeps the identity of its household and connection
- expired queued commands are rejected and the command queue is bounded
- configuration IDs must match `semantic_home_` plus a UUID, with the matching ownership marker
- deployment checks timezones, registry identities, loaded automation entity and activation state
- repeated deployment updates the same local automation; other HA configurations are not overwritten
- local HA automations persist independently of the agent/cloud connection
- activity distinguishes a requested service call from observed device state; disconnected history can be incomplete

Before removing a configured device, pause affected rules. On detected registry replacement the agent disables affected app-owned rules; generated templates also guard known physical device IDs. Renames are reconciled through registry identity. Device support remains limited to functions provided by the HA integration.

## Simulation

Omit both HA_URL and HA_TOKEN for a small simulated inventory. Pairing credentials are still required. Simulated service calls can update demo state; **stub mode cannot deploy or claim successful real automations**.

## Tests

```bash
uv run python -m unittest discover -s tests -v
```

The tests use simulated HA responses and do not connect to a real household.
