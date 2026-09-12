#!/usr/bin/env python3
"""llm-gateway: own API with own limits (tmiland-lab self-hosting edition).

Single-file, stdlib-only OpenAI-compatible proxy. Routes virtual models
(prefix/model) to upstream providers (free-tier keys + local Ollama),
enforcing OUR quotas: per-client-key requests/minute + daily request
budget, all accounting in a local sqlite db. No third party can throttle
or reshape what we expose.

Endpoints:
  GET  /healthz
  GET  /v1/models            (virtual catalog: configured + live Ollama list)
  POST /v1/chat/completions  (streaming SSE passthrough + blocking)
  GET  /v1/usage             (today's per-key counters, JSON)

Config: ~/.config/llm-gateway/config.json (see config.example.json).
Secrets: env vars only (see llm-gateway.service EnvironmentFile).
  Never commit keys — the repo carries the template, never values.
"""

import http.client
import http.server
import json
import os
import socketserver
import sqlite3
import sys
import time
import urllib.parse
import urllib.request

CONFIG_PATH = os.environ.get("GATEWAY_CONFIG",
                             os.path.expanduser("~/.config/llm-gateway/config.json"))
DB_PATH = os.environ.get("GATEWAY_DB",
                         os.path.expanduser("~/.local/share/llm-gateway/usage.db"))


def load_config():
    with open(CONFIG_PATH) as f:
        cfg = json.load(f)
    # Resolve "ENV:NAME" placeholders to environment values (may be empty
    # when the user hasn't added that free key yet — route then 402s with
    # a hint instead of failing cryptically).
    for route in cfg.get("routes", []):
        ref = route.get("api_key_env")
        route["api_key"] = os.environ.get(ref, "") if ref else ""
    return cfg


def db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "CREATE TABLE IF NOT EXISTS hits("
        "ts INTEGER, client TEXT, route TEXT, model TEXT, "
        "status INTEGER, ms INTEGER)"
    )
    for col in ("in_tok INTEGER DEFAULT 0", "out_tok INTEGER DEFAULT 0"):
        try:
            conn.execute(f"ALTER TABLE hits ADD COLUMN {col}")
        except Exception:
            pass
    return conn


UI_PAGE = """<!DOCTYPE html>
<html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>llm-gateway — own API</title>
<style>
body{font-family:system-ui,sans-serif;background:#0d1117;color:#e6edf3;max-width:900px;margin:2em auto;padding:0 1em}
h1{font-size:1.3em}h2{font-size:1.05em;margin-top:1.6em;border-bottom:1px solid #30363d;padding-bottom:.3em}
table{border-collapse:collapse;width:100%;font-size:.9em}
td,th{border:1px solid #30363d;padding:.35em .6em;text-align:left}
.ok{color:#3fb950}.bad{color:#f85149}.mut{color:#8b949e}
code{background:#161b22;padding:.1em .35em;border-radius:4px;font-size:.9em}
#st{font-size:.85em}
</style></head><body>
<h1>llm-gateway <span class="mut">— own API, own limits</span></h1>
<p id="st" class="mut">loading…</p>
<h2>Routes</h2><table id="routes"><tr><th>prefix</th><th>name</th><th>status</th></tr></table>
<h2>Models (<span id="nmodels">0</span>)</h2><table id="models"><tr><th>id</th><th>via</th></tr></table>
<h2>Usage today</h2><table id="usage"><tr><th>client</th><th>requests</th></tr></table>
<h2>Recent requests</h2><table id="recent"><tr><th>time</th><th>client</th><th>model</th><th>status</th><th>ms</th></tr></table>
<script>
async function load(){
  const r = await fetch('/api/status'); const s = await r.json();
  document.getElementById('st').textContent = 'updated ' + new Date().toLocaleTimeString();
  const esc = x => String(x).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
  document.getElementById('routes').innerHTML = '<tr><th>prefix</th><th>name</th><th>status</th></tr>' +
    s.routes.map(x => '<tr><td><code>'+esc(x.prefix)+'</code></td><td>'+esc(x.name)+'</td><td class="'+(x.ready?'ok':'bad')+'">'+(x.ready?'ready':'needs '+esc(x.need||''))+'</td></tr>').join('');
  document.getElementById('nmodels').textContent = s.models.length;
  document.getElementById('models').innerHTML = '<tr><th>id</th><th>via</th></tr>' +
    s.models.map(x => '<tr><td><code>'+esc(x.id)+'</code></td><td>'+esc(x.owned_by)+'</td></tr>').join('');
  document.getElementById('usage').innerHTML = '<tr><th>client</th><th>model</th><th>req</th><th>in/out tok</th><th>est $</th></tr>' +
    (s.usage_today.map(x => '<tr><td>'+esc(x.client)+'</td><td><code>'+esc(x.model||x.route)+'</code></td><td>'+x.requests+'</td><td>'+x.in_tokens+'/'+x.out_tokens+'</td><td>$'+x.est_usd.toFixed(4)+'</td></tr>').join('') || '<tr><td colspan=5 class=mut>none yet</td></tr>');
  document.getElementById('recent').innerHTML = '<tr><th>time</th><th>client</th><th>model</th><th>status</th><th>ms</th></tr>' +
    (s.recent.map(x => '<tr><td>'+new Date(x.at*1000).toLocaleTimeString()+'</td><td>'+esc(x.client)+'</td><td><code>'+esc(x.model)+'</code></td><td class="'+(x.status===200?'ok':'bad')+'">'+x.status+'</td><td>'+x.ms+'</td></tr>').join('') || '<tr><td colspan=5 class=mut>none yet</td></tr>');
}
load(); setInterval(load, 30000);
</script></body></html>"""


class Handler(http.server.BaseHTTPRequestHandler):
    server_version = "llm-gateway/1.0"
    protocol_version = "HTTP/1.1"

    # -- helpers ------------------------------------------------------
    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _html(self, code, page):
        body = page.encode()
        self.send_response(code)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _auth_client(self):
        auth = self.headers.get("Authorization", "")
        token = auth[7:] if auth.startswith("Bearer ") else ""
        client = self.server.cfg.get("clients", {}).get(token)
        return client

    def _over_quota(self, client_name, client):
        now = int(time.time())
        window_start = now - 60
        day_start = now - (now % 86400)
        conn = db()
        rpm = conn.execute(
            "SELECT COUNT(*) FROM hits WHERE client=? AND ts>=?",
            (client_name, window_start),
        ).fetchone()[0]
        daily = conn.execute(
            "SELECT COUNT(*) FROM hits WHERE client=? AND ts>=?",
            (client_name, day_start),
        ).fetchone()[0]
        conn.close()
        if rpm >= client.get("rpm", 60):
            return f"rate limit: {rpm} requests in the last minute (own limit: {client.get('rpm', 60)}/min)"
        if daily >= client.get("daily_requests", 1000):
            return f"daily budget spent: {daily} requests (own limit: {client.get('daily_requests', 1000)}/day)"
        token_cap = client.get("daily_tokens")
        if token_cap:
            tally = db()
            try:
                toks = tally.execute(
                    "SELECT COALESCE(SUM(in_tok),0)+COALESCE(SUM(out_tok),0) FROM hits WHERE client=? AND ts>=?",
                    (client_name, day_start),
                ).fetchone()[0]
            finally:
                tally.close()
            if toks >= token_cap:
                return f"daily token budget spent: {toks} tokens (own limit: {token_cap}/day)"
        return None

    def _log(self, client_name, route, model, status, ms, intok=0, outtok=0):
        try:
            conn = db()
            conn.execute(
                "INSERT INTO hits VALUES(?,?,?,?,?,?,?,?)",
                (int(time.time()), client_name, route, model, status, ms, intok, outtok),
            )
            conn.commit()
            conn.close()
        except Exception:
            pass

    @staticmethod
    def _usage_of(payload):
        """Pull (in_tokens, out_tokens) out of an OpenAI-style body."""
        try:
            u = payload.get("usage") or {}
            return int(u.get("prompt_tokens") or 0), int(u.get("completion_tokens") or 0)
        except Exception:
            return 0, 0

    def _route_for(self, model):
        for route in self.server.cfg.get("routes", []):
            if model.startswith(route["prefix"]):
                return route, model[len(route["prefix"]):]
        return None, model

    # -- routes --------------------------------------------------------
    def do_GET(self):
        if self.path == "/healthz":
            return self._json(200, {"ok": True})
        if self.path == "/v1/usage":
            client = self._auth_client()
            if not client:
                return self._json(401, {"error": "bad gateway api key"})
            agg = {}
            for row in self._today_spend():
                a = agg.setdefault(row["client"], {"client": row["client"], "requests": 0,
                                                   "in_tokens": 0, "out_tokens": 0, "est_usd": 0.0})
                a["requests"] += row["requests"]
                a["in_tokens"] += row["in_tokens"]
                a["out_tokens"] += row["out_tokens"]
                a["est_usd"] = round(a["est_usd"] + row["est_usd"], 4)
            return self._json(200, {"today": list(agg.values())})

        if self.path == "/v1/models":
            return self._json(200, {"object": "list",
                                    "data": self._virtual_models()})
        if self.path == "/api/status":
            usage = self._today_spend()
            conn = db()
            recent = conn.execute(
                "SELECT ts, client, route, model, status, ms FROM hits "
                "ORDER BY ts DESC LIMIT 20",
            ).fetchall()
            conn.close()
            routes = []
            for r in self.server.cfg.get("routes", []):
                routes.append({
                    "prefix": r["prefix"], "name": r.get("name", "?"),
                    "ready": (not r.get("api_key_env")) or bool(r.get("api_key")),
                    "need": r.get("api_key_env"),
                })
            return self._json(200, {
                "models": self._virtual_models(),
                "routes": routes,
                "usage_today": usage,
                "recent": [{"at": t, "client": c, "route": ro, "model": m,
                            "status": s, "ms": ms}
                           for t, c, ro, m, s, ms in recent],
            })
        if self.path == "/ui" or self.path == "/ui/":
            return self._html(200, UI_PAGE)
        return self._json(404, {"error": "not found"})

    def _price_for(self, route_prefix, model):
        """Per-model prices (substring match on the model id), falling back
        to the route default. DO's kimi costs ~16x its 120b — one number
        per route would lie."""
        for r in self.server.cfg.get("routes", []):
            if r["prefix"] != route_prefix:
                continue
            for key, p in (r.get("model_prices") or {}).items():
                if key in (model or ""):
                    return p
            return r.get("price_per_mtok") or {"in": 0, "out": 0}
        return {"in": 0, "out": 0}

    def _today_spend(self):
        """Per-(client, route, model) token sums with per-model pricing."""
        now = int(time.time())
        conn = db()
        rows = conn.execute(
            "SELECT client, route, model, COALESCE(SUM(in_tok),0), COALESCE(SUM(out_tok),0), COUNT(*) FROM hits WHERE ts>=? GROUP BY client, route, model",
            (now - (now % 86400),),
        ).fetchall()
        conn.close()
        out = []
        for c, ro, m, i, o, n in rows:
            p = self._price_for(ro, m)
            out.append({"client": c, "route": ro, "model": m, "requests": n,
                        "in_tokens": i, "out_tokens": o,
                        "est_usd": round(i / 1e6 * p["in"] + o / 1e6 * p["out"], 4)})
        return out

    def _virtual_models(self):
        data = []
        for route in self.server.cfg.get("routes", []):
            if route["prefix"] == "local/":
                for m in self._ollama_models(route):
                    data.append({"id": "local/" + m, "object": "model",
                                 "owned_by": "ollama"})
            else:
                for m in route.get("models", []):
                    data.append({"id": route["prefix"] + m, "object": "model",
                                 "owned_by": route.get("name", "upstream")})
        return data

    def _ollama_models(self, route):
        try:
            parts = urllib.parse.urlsplit(route["base"])
            host = parts.hostname or "127.0.0.1"
            port = parts.port or 11434
            conn = http.client.HTTPConnection(host, port, timeout=5)
            conn.request("GET", "/api/tags")
            resp = conn.getresponse()
            payload = json.loads(resp.read().decode())
            return [m["name"] for m in payload.get("models", [])]
        except Exception:
            return []

    def do_POST(self):
        if self.path != "/v1/chat/completions":
            return self._json(404, {"error": "not found"})
        client = self._auth_client()
        if not client:
            return self._json(401, {"error": "bad gateway api key"})
        name = client["name"]
        problem = self._over_quota(name, client)
        if problem:
            return self._json(429, {"error": problem})

        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b"{}"
        try:
            body = json.loads(raw.decode())
        except Exception:
            return self._json(400, {"error": "invalid JSON body"})
        model = body.get("model", "")
        route, upstream_model = self._route_for(model)
        if route is None:
            return self._json(404, {"error": f"no route for model '{model}'"})
        if route.get("api_key_env") and not route.get("api_key"):
            return self._json(402, {"error": (
                f"route '{route['prefix']}' needs a free provider key: "
                f"set {route['api_key_env']} and restart the gateway")})
        body["model"] = upstream_model
        # Fork: thinking models (OpenRouter free reasoning flood) stream
        # endless reasoning deltas the client never displays, so the user
        # sees "no answer" until abort. Excluding reasoning returns final
        # content + tool calls directly — what an agent loop needs.
        if route.get("drop_reasoning") and "reasoning" not in body:
            body["reasoning"] = {"exclude": True}
        if body.get("stream") and "stream_options" not in body:
            body["stream_options"] = {"include_usage": True}
        out = json.dumps(body).encode()

        parts = urllib.parse.urlsplit(route["base"])
        https = parts.scheme == "https"
        host = parts.hostname or "127.0.0.1"
        port = parts.port or (443 if https else 80)
        base_path = parts.path.rstrip("/")
        t0 = time.time()

        try:
            conn_cls = http.client.HTTPSConnection if https else http.client.HTTPConnection
            conn = conn_cls(host, port, timeout=300)
            headers = {"Content-Type": "application/json"}
            if route.get("api_key"):
                headers["Authorization"] = "Bearer " + route["api_key"]
            for extra in route.get("headers", {}).items():
                headers[extra[0]] = extra[1]
            conn.request("POST", base_path + "/chat/completions",
                         body=out, headers=headers)
            resp = conn.getresponse()
            status = resp.status
            if body.get("stream") and route.get("burst"):
                self._burst(resp, body, route, model, name, t0)
            elif body.get("stream"):
                self.send_response(status)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "keep-alive")
                self.end_headers()
                try:
                    # SSE: forward line-by-line so the TUI sees tokens
                    # immediately. read(n) would buffer up to n bytes
                    # before flushing anything (silence → user aborts).
                    while True:
                        line = resp.readline(65536)
                        if not line:
                            break
                        self.wfile.write(line)
                        self.wfile.flush()
                except (BrokenPipeError, ConnectionResetError):
                    pass  # client (user abort) went away; nothing to answer
                finally:
                    # SSE has no Content-Length: close so clients see EOF
                    # instead of hanging on keep-alive.
                    self.close_connection = True
            else:
                payload = resp.read()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                intok = outtok = 0
                if status == 200:
                    try:
                        intok, outtok = self._usage_of(json.loads(payload.decode()))
                    except Exception:
                        pass
                self._log(name, route["prefix"], model, status,
                          int((time.time() - t0) * 1000), intok, outtok)
                return
            self._log(name, route["prefix"], model, status,
                      int((time.time() - t0) * 1000))
        except Exception as e:
            self._log(name, route.get("prefix", "?"), model, 502,
                      int((time.time() - t0) * 1000))
            try:
                self._json(502, {"error": f"upstream unreachable: {e}"})
            except BrokenPipeError:
                pass

    def _burst(self, resp, body, route, model, client_name, t0):
        """Burst mode for flaky free-tier streams: consume the whole upstream
        SSE response, then replay the complete answer to the client as one
        fast burst. Kills mid-stream stalls and partial tool-call corruption
        in one move. Local/fast routes keep the live relay (progress matters
        more than robustness there)."""
        deadline = t0 + 280
        text_parts = []
        tools = {}
        finish = None
        first = None
        in_tok = out_tok = 0
        # Provider extensions (e.g. Google thought_signature for thinking
        # models) must survive reassembly: opencode sends them back on the
        # next turn, and upstream 400s when they're missing. Shallow-merge,
        # latest wins.
        extra_content = {}
        try:
            if resp.status != 200:
                payload = resp.read()
                self.send_response(resp.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                self._log(client_name, route["prefix"], model, resp.status,
                          int((time.time() - t0) * 1000))
                return
            while True:
                if time.time() > deadline:
                    raise TimeoutError("upstream too slow (>280s)")
                line = resp.readline(1048576)
                if not line:
                    break
                s = line.decode("utf-8", "replace").strip()
                if s == "data: [DONE]":
                    break
                if not s.startswith("data: "):
                    continue
                try:
                    d = json.loads(s[6:])
                except Exception:
                    continue
                if first is None and isinstance(d, dict) and ("id" in d or "created" in d):
                    first = d
                ch = (d.get("choices") or [{}])[0]
                delta = ch.get("delta") or {}
                if isinstance(delta.get("content"), str):
                    text_parts.append(delta["content"])
                ec = delta.get("extra_content") or ch.get("extra_content") or d.get("extra_content")
                if isinstance(ec, dict):
                    extra_content.update(ec)
                for tc in delta.get("tool_calls") or []:
                    i = tc.get("index", 0)
                    e = tools.setdefault(i, {"id": None, "type": "function",
                                             "name": None, "arguments": ""})
                    if tc.get("id"):
                        e["id"] = tc["id"]
                    if tc.get("type"):
                        e["type"] = tc["type"]
                    fn = tc.get("function") or {}
                    if fn.get("name"):
                        e["name"] = fn["name"]
                    if isinstance(fn.get("arguments"), str):
                        e["arguments"] += fn["arguments"]
                if ch.get("finish_reason"):
                    finish = ch["finish_reason"]
                if d.get("usage"):
                    in_tok, out_tok = self._usage_of(d)
        except Exception as e:
            self._log(client_name, route.get("prefix", "?"), model, 504,
                      int((time.time() - t0) * 1000))
            try:
                self._json(504, {"error": f"upstream stalled/failed: {e}"})
            except BrokenPipeError:
                pass
            return

        msg_id = (first or {}).get("id", "chatcmpl-gw-%d" % int(time.time()))
        created = (first or {}).get("created", int(time.time()))

        def emit(delta, finish_reason=None):
            chunk = {
                "id": msg_id, "object": "chat.completion.chunk",
                "created": created, "model": body["model"],
                "choices": [{"index": 0, "delta": delta,
                             "finish_reason": finish_reason}],
            }
            if extra_content:
                chunk["extra_content"] = extra_content
            return ("data: " + json.dumps(chunk) + "\n\n").encode()

        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self.send_header("Cache-Control", "no-cache")
            self.send_header("Connection", "keep-alive")
            self.end_headers()
            text = "".join(text_parts)
            if text:
                self.wfile.write(emit({"role": "assistant", "content": text}))
            for i in sorted(tools):
                e = tools[i]
                self.wfile.write(emit({"tool_calls": [{
                    "index": i, "id": e["id"], "type": e["type"],
                    "function": {"name": e["name"],
                                 "arguments": e["arguments"]}}]}))
            self.wfile.write(emit({}, finish or "stop"))
            self.wfile.write(b"data: [DONE]\n\n")
            self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError):
            pass
        finally:
            self.close_connection = True
        self._log(client_name, route["prefix"], model, 200,
                  int((time.time() - t0) * 1000), in_tok, out_tok)

    def log_message(self, fmt, *args):
        sys.stderr.write("[%s] %s\n" % (time.strftime("%H:%M:%S"), fmt % args))


def main():
    cfg = load_config()
    listen = cfg.get("listen", "127.0.0.1:4143")
    host, _, port = listen.rpartition(":")
    socketserver.ThreadingTCPServer.allow_reuse_address = True
    with socketserver.ThreadingTCPServer((host, int(port)), Handler) as httpd:
        httpd.cfg = cfg
        print(f"llm-gateway on {listen} ({len(cfg.get('routes', []))} routes)", flush=True)
        httpd.serve_forever()


if __name__ == "__main__":
    main()
