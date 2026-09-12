# Free models — live status (verified 2026-09-12, CET night)

Proven through this gateway unless noted. Free IDs rot in weeks —
re-probe before trusting any row older than ~2 weeks.

## Daily drivers (proven: stream + answer + tools)

| Gateway model | Speed | Notes |
| --- | --- | --- |
| `openrouter/cohere/north-mini-code:free` | ~0.6s | Recommended default. Passed file-read+compute E2E. Needs output headroom (tiny budgets truncate). |
| `gemini/gemini-3.6-flash` | ~2.3s | First genuine TUI answer. Multi-turn safe since extra_content fix. |
| `gemini/gemini-3.7-flash` | ~4.3s | Current Flash workhorse. Stream-proven; TUI loop pending user run. |
| `kilo/cohere/north-mini-code:free` | fast | Same model, NO KEY (anon 200/hr/IP). Redundancy when OpenRouter 429s. |

## Heavy / slow (proven, use for hard tasks)

| Gateway model | Speed | Notes |
| --- | --- | --- |
| `openrouter/nvidia/nemotron-3-super-120b-a12b:free` | slow | User-confirmed working in TUI. Thinks long, queues on free tier. |
| `openrouter/nvidia/nemotron-3-ultra-550b-a55b:free` | ~1.6s stream | Stream-proven; full E2E pending. Kilo mirror 502'd once (Nvidia overloaded). |

## Fallback (free forever, weak)

| Gateway model | Notes |
| --- | --- |
| `local/qwen2.5-coder:14b` | 0.1s via gateway. Fails vague prompts (`tool: invalid`); fine for explicit mechanical work. |

## Marginal (answers empty without headroom)

- `openrouter/nvidia/nemotron-3-nano-omni-30b-a3b-reasoning:free` — empty content on short budgets; untested large.

## Blocked by account, not by code

- `cerebras/*` — `payment_required` on all inference (needs billing/tab or rotation).
- `do/*` — 402 account gate (needs console terms/enablement or support).
- `groq/*` — no key + Cloudflare geo-block from this network.
- `openrouter/google/gemma-4-31b-it:free` — temporary 429; also our OR key hits the shared 50/day free quota (ease off probing).

## Retired IDs (do not use)

- `openrouter/qwen/qwen3-coder:free`, `openrouter/z-ai/glm-5.2:free`, `gemini/gemini-2.5-flash|pro` (new keys), llm7 `gpt-oss`, `cerebras/llama-3.3-70b`.

## Reserve (untested)

- llm7 turbo IDs (key `unused`, anonymous), OVHCloud anonymous ~2/min, email signups: requesty (200/day), sambanova (20/day), cohere trial, mistral Labs, vercel-ai-gateway ($5 credit).
