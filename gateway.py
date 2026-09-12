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

CONFIG_PATH = os.path.expanduser("~/.config/llm-gateway/config.json")
DB_PATH = os.path.expanduser("~/.local/share/llm-gateway/usage.db")


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
    return conn


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
        return None

    def _log(self, client_name, route, model, status, ms):
        try:
            conn = db()
            conn.execute(
                "INSERT INTO hits VALUES(?,?,?,?,?,?)",
                (int(time.time()), client_name, route, model, status, ms),
            )
            conn.commit()
            conn.close()
        except Exception:
            pass

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
            now = int(time.time())
            conn = db()
            rows = conn.execute(
                "SELECT client, COUNT(*), MAX(ts) FROM hits WHERE ts>=? GROUP BY client",
                (now - (now % 86400),),
            ).fetchall()
            conn.close()
            return self._json(200, {
                "today": [{"client": c, "requests": n, "last": t} for c, n, t in rows],
            })
        if self.path == "/v1/models":
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
            return self._json(200, {"object": "list", "data": data})
        return self._json(404, {"error": "not found"})

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
            if body.get("stream"):
                self.send_response(status)
                self.send_header("Content-Type", "text/event-stream")
                self.send_header("Cache-Control", "no-cache")
                self.send_header("Connection", "keep-alive")
                self.end_headers()
                while True:
                    chunk = resp.read(65536)
                    if not chunk:
                        break
                    self.wfile.write(chunk)
                    self.wfile.flush()
            else:
                payload = resp.read()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            self._log(name, route["prefix"], model, status,
                      int((time.time() - t0) * 1000))
        except Exception as e:
            self._log(name, route.get("prefix", "?"), model, 502,
                      int((time.time() - t0) * 1000))
            try:
                self._json(502, {"error": f"upstream unreachable: {e}"})
            except BrokenPipeError:
                pass

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
