# llm-gateway — own API with own limits

OpenAI-compatible proxy for the `tmiland-lab/opencode` self-hosting fork.
One endpoint, our quotas (per-key requests/minute + daily budgets in local
sqlite), routed to free-tier provider keys + local Ollama. No third party
can throttle or reshape what we expose.

## Run (this PC, localhost only)

```sh
mkdir -p ~/.config/llm-gateway ~/.local/share/llm-gateway
cp config.example.json ~/.config/llm-gateway/config.json
python3 -c "import secrets; print(secrets.token_hex(32))"  # -> client key
# put the key into config.json clients + chmod 600, then:
printf 'GROQ_API_KEY=\nCEREBRAS_API_KEY=\nOPENROUTER_API_KEY=\nGEMINI_API_KEY=\n' > ~/.config/llm-gateway/env
chmod 600 ~/.config/llm-gateway/config.json ~/.config/llm-gateway/env
python3 ~/llm-gateway/gateway.py
```

systemd (user): see `llm-gateway.service` — `systemctl --user enable --now llm-gateway`.

## Web dashboard (local)

The gateway serves its own UI — no build step, no JS deps:

- `http://localhost:4143/ui` — routes (ready/missing key), models,
  today's per-client usage, last 20 requests (auto-refresh 30s).

## Docker (all-local option)

```sh
cp config.docker.json config/config.json  # then put your client key in it
docker compose up --build -d              # UI at http://localhost:4143/ui
```

`config/` is gitignored (keys stay local). The container reaches host
Ollama via `host.docker.internal`. Data (sqlite) lives in the `gwdata`
volume.

## Use from opencode (`~/.config/opencode/opencode.jsonc`)

```jsonc
"provider": {
  "gateway": {
    "npm": "@ai-sdk/openai-compatible",
    "name": "Own gateway",
    "options": {
      "baseURL": "http://localhost:4143/v1",
      "apiKey": "<the client key from config.json>"
    },
    "models": {
      "local/gpt-oss:20b": { "name": "Local 20B (free, same box)" },
      "groq/llama-3.3-70b-versatile": { "name": "Groq 70B (free tier)" }
    }
  }
}
```

Then `/models` → gateway models, or `"model": "gateway/groq/llama-3.3-70b-versatile"`.

## Free keys (2 min each, all have free tiers)

- Groq: https://console.groq.com/keys
- Cerebras: https://cloud.cerebras.ai/ → API keys
- OpenRouter: https://openrouter.ai/keys (`:free` models cost nothing)
- Google AI Studio: https://aistudio.google.com/apikey

Paste into `~/.config/llm-gateway/env`, restart the service. Routes
without a key answer 402 with the exact missing var — nothing breaks.

## Own limits (tune in config.json)

- `rpm` / `daily_requests` per client key; `429` with the spent budget.
- `GET /v1/usage` (gateway key): today's per-client counters.
- sqlite: `~/.local/share/llm-gateway/usage.db`, table `hits`.
