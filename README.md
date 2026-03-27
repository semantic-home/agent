# Semantic Home Agent

The Semantic Home Agent is a small local process that connects your Home Assistant instance to the Semantic Home backend.

It does the following:

1. It opens a websocket connection to the backend.
2. It syncs Home Assistant entities into the backend.
3. It executes backend commands as Home Assistant service calls.

## Before You Start

You need:

1. Python 3.10 or newer.
2. [`uv`](https://docs.astral.sh/uv/) installed.
3. A Home Assistant URL.
4. A Home Assistant long-lived access token.
5. A backend websocket base URL.

Example values for the public alpha:

```bash
export HA_URL="http://server.home:8123"
export HA_TOKEN="<your-home-assistant-token>"
export SEMANTIC_HOME_BASE_URL="wss://semantic-home.myhometech.de/api/v1"
```

## Quick Start

For onboarding and first-time setup, the cleanest option is to run the agent directly from the Git repository without installing it permanently:

```bash
uvx --from git+https://gitlab.myhometech.de/home-automation/agent.git \
  home-automation-agent \
  --base-url "$SEMANTIC_HOME_BASE_URL" \
  --id home1
```

The agent will read `HA_URL` and `HA_TOKEN` from your environment automatically.

## Install Once

If you want a persistent local install instead of `uvx`, install the CLI once and run it normally:

```bash
uv tool install --from git+https://gitlab.myhometech.de/home-automation/agent.git \
  home-automation-agent
```

Then start it with:

```bash
home-automation-agent --base-url "$SEMANTIC_HOME_BASE_URL" --id home1
```

## Run Without Home Assistant

If you omit `HA_URL` and `HA_TOKEN`, the agent starts in stub mode and publishes demo entities. This is useful for local UI work and onboarding flows.

```bash
uvx --from git+https://gitlab.myhometech.de/home-automation/agent.git \
  home-automation-agent \
  --base-url "$SEMANTIC_HOME_BASE_URL" \
  --id demo-home
```

## Command Line Options

```text
--base-url       Backend websocket base URL. Default: ws://localhost:8000
--id             Agent identifier. Default: dev1
--name           Optional display name shown in the backend
--description    Optional description shown in the backend
--ha-url         Home Assistant URL. Optional if HA_URL is set
--ha-token       Home Assistant token. Optional if HA_TOKEN is set
```

If you provide one of `--ha-url` or `--ha-token`, you must provide both.

## Security

Do not put Home Assistant tokens into source files, screenshots, commits, or README examples.

Use environment variables instead:

```bash
export HA_URL="http://server.home:8123"
export HA_TOKEN="<your-home-assistant-token>"
```

If a token is exposed, revoke it in Home Assistant and create a new one.

## Development

For local development inside the repository:

```bash
uv sync
uv run python main.py --base-url "$SEMANTIC_HOME_BASE_URL" --id home1
```
