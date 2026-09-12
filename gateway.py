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
import secrets
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
# Upstream provider keys live here (0600), never in config.json or git.
ENV_PATH = os.environ.get("GATEWAY_ENV",
                          os.path.expanduser("~/.config/llm-gateway/env"))


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
:root{--bg:#0d1117;--panel:#161b22;--border:#30363d;--text:#e6edf3;--mut:#8b949e;--acc:#d29922;--ok:#3fb950;--bad:#f85149;--hover:#1c2128;--chip:#0d1117;--link:#d29922}
[data-theme="light"]{--bg:#f0f0f1;--panel:#ffffff;--border:#c3c4c7;--text:#3c434a;--mut:#646970;--acc:#2271b1;--ok:#008a20;--bad:#d63638;--hover:#f6f7f7;--chip:#f6f7f7;--link:#2271b1}
*{box-sizing:border-box}
body{font-size:17px;font-family:system-ui,-apple-system,"Segoe UI",sans-serif;background:var(--bg);color:var(--text);max-width:1080px;margin:0 auto;padding:1.5em 1em 3em}
header{display:flex;align-items:baseline;gap:.6em;flex-wrap:wrap}
h1{font-size:1.6em;margin:0}
.sub{color:var(--mut);font-size:.9em}
.live{display:inline-block;width:.55em;height:.55em;border-radius:50%;background:var(--ok);margin-right:.35em;box-shadow:0 0 6px var(--ok)}
#st{color:var(--mut);font-size:.82em}
.tabs{display:flex;gap:.4em;margin:1.2em 0 0;flex-wrap:wrap}
.tabs button{background:var(--panel);color:var(--mut);border:1px solid var(--border);border-radius:8px 8px 0 0;padding:.5em 1em;font-size:.9em;cursor:pointer;border-bottom:none}
.tabs button.on{background:#1c2128;color:var(--text);font-weight:600}
.tab{display:none}
.tab.on{display:block}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:.7em;margin:1.2em 0 .4em}
.card{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:.7em .9em}
.card .v{font-size:1.6em;font-weight:650}
.card .l{color:var(--mut);font-size:.78em;margin-top:.15em}
h2{font-size:1.1em;margin:1.8em 0 .6em;color:var(--mut);text-transform:uppercase;letter-spacing:.06em;font-weight:600}
table{border-collapse:collapse;width:100%;font-size:1em;background:var(--panel);border:1px solid var(--border);border-radius:10px;overflow:hidden}
td,th{border-bottom:1px solid var(--border);padding:.45em .7em;text-align:left}
tr:last-child td{border-bottom:none}
th{color:var(--mut);font-weight:600;font-size:.8em;text-transform:uppercase;letter-spacing:.04em}
tr:hover td{background:#1c2128}
.ok{color:var(--ok)}.bad{color:var(--bad)}.mut{color:var(--mut)}
.pill{display:inline-block;padding:.1em .55em;border-radius:99px;font-size:.8em;font-weight:600}
.pill.ok{background:rgba(63,185,80,.14)}
.pill.bad{background:rgba(248,81,73,.14)}
code{background:#0d1117;border:1px solid var(--border);padding:.1em .4em;border-radius:6px;font-size:.95em}
input[type=checkbox]{accent-color:var(--acc);width:1em;height:1em;cursor:pointer}
input[type=text],input[type=password],input[type=number]{background:#0d1117;border:1px solid var(--border);color:var(--text);border-radius:6px;padding:.4em .6em;font-size:.88em}
button.act{background:#1c2128;color:var(--text);border:1px solid var(--border);border-radius:6px;padding:.45em 1em;font-size:.95em;cursor:pointer}
button.act:hover{border-color:var(--acc)}
button.act:disabled{opacity:.5;cursor:wait}
.search{width:100%;margin:.4em 0 .2em}
.chain{background:var(--panel);border:1px solid var(--border);border-radius:10px;padding:.7em .9em;margin:.5em 0;font-size:1em}
.chain .arrow{color:var(--acc);font-weight:700}
.tnote{font-size:.8em;color:var(--mut);margin-top:.4em}
.keyrow{display:flex;gap:.5em;align-items:center;flex-wrap:wrap;margin:.4em 0}
footer{margin-top:2.5em;color:var(--mut);font-size:.8em}
</style></head><body>
<header><h1><span class="live"></span>llm-gateway</h1><span class="sub">own API, own limits</span><button class="act" id="themebtn" style="margin-left:auto" onclick="toggleTheme()">light</button></header>
<p id="st">loading…</p>
<div class="tabs">
<button id="t-overview" class="on" onclick="showTab('overview')">Overview</button>
<button id="t-models" onclick="showTab('models')">Models</button>
<button id="t-activity" onclick="showTab('activity')">Activity</button>
<button id="t-keys" onclick="showTab('keys')">Keys</button>
</div>
<div class="tab on" id="tab-overview">
<div class="cards">
<div class="card"><div class="v" id="c-models">–</div><div class="l">models</div></div>
<div class="card"><div class="v" id="c-routes">–</div><div class="l">routes ready</div></div>
<div class="card"><div class="v" id="c-req">–</div><div class="l">requests today</div></div>
<div class="card"><div class="v" id="c-spend">–</div><div class="l">est. spend today</div></div>
</div>
<h2>Failover chains (cheapest first)</h2><div id="chains"></div>
<h2>Routes (toggle to enable/disable)</h2><table id="routes"></table>
</div>
<div class="tab" id="tab-models">
<h2>Models (<span id="nmodels">0</span>)</h2>
<input class="search" id="q" type="text" placeholder="filter models…" oninput="renderModels()">
<table id="models"></table>
<div class="tnote">Tick a model off to hide it from <code>/v1/models</code> and refuse calls with 403. Test runs a tiny live call (logged as client <code>dashboard</code>).</div>
</div>
<div class="tab" id="tab-activity">
<h2>Usage today</h2><table id="usage"></table>
<h2>Recent requests</h2><table id="recent"></table>
</div>
<div class="tab" id="tab-keys">
<h2>Provider keys</h2><div id="keyforms"></div>
<div class="tnote">Keys are stored in the 0600 env file on the server, never in git. Only the last 4 characters are ever displayed.</div>
<h2>Gateway clients</h2><table id="clients"></table>
<div class="keyrow"><input id="newname" type="text" placeholder="client name"><input id="newrpm" type="number" value="60" style="width:6em" title="req/min"><input id="newdaily" type="number" value="2000" style="width:8em" title="req/day"><input id="newtok" type="number" value="2000000" style="width:9em" title="tokens/day"><button class="act" onclick="addClient()">add client</button></div>
<div id="newtoken" class="tnote"></div>
</div>
<footer>llm-gateway · static surface on GitHub, stateful proxy here · localhost trust domain</footer>
<script>
let S = null;
function showTab(n){
  document.querySelectorAll('.tab').forEach(e => e.classList.remove('on'));
  document.querySelectorAll('.tabs button').forEach(e => e.classList.remove('on'));
  document.getElementById('tab-'+n).classList.add('on');
  document.getElementById('t-'+n).classList.add('on');
}
async function load(){
  const r = await fetch('/api/status'); const s = await r.json(); S = s;
  document.getElementById('st').textContent = 'updated ' + new Date().toLocaleTimeString();
  const esc = x => String(x).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
  const ready = s.routes.filter(x => x.enabled && x.ready).length;
  document.getElementById('c-models').textContent = s.models.length;
  document.getElementById('c-routes').textContent = ready + '/' + s.routes.length;
  let treq = 0, tspend = 0;
  s.usage_today.forEach(x => { treq += x.requests; tspend += x.est_usd; });
  document.getElementById('c-req').textContent = treq;
  document.getElementById('c-spend').textContent = '$' + (tspend < 0.01 ? tspend.toFixed(6) : tspend.toFixed(2));
  document.getElementById('chains').innerHTML = Object.keys(s.mirrors||{}).map(g => {
    const ms = s.mirrors[g].map(m => '<code>'+esc(m.model)+'</code> <span class="mut">$'+(((m.price||{}).in||0)+((m.price||{}).out||0)).toFixed(2)+'/M</span>' + (m.ready && m.enabled !== false ? '' : ' <span class="pill bad">'+esc(m.why||'off')+'</span>')).join(' <span class="arrow">→</span> ');
    return '<div class="chain"><code>'+esc(g)+'</code><br>'+ms+'</div>';
  }).join('') || '<div class="mut">no mirror groups</div>';
  document.getElementById('routes').innerHTML = '<tr><th>on</th><th>prefix</th><th>name</th><th>status</th></tr>' +
    s.routes.map(x => '<tr><td><input type="checkbox"'+(x.enabled?' checked':'')+' onchange="gwToggle(\\''+esc(x.prefix)+'\\',this.checked)"></td><td><code>'+esc(x.prefix)+'</code></td><td>'+esc(x.name)+'</td><td><span class="pill '+(x.ready?'ok':'bad')+'">'+(x.ready?(x.enabled?'ready':'off'):'needs '+esc(x.need||''))+'</span></td></tr>').join('');
  renderModels();
  document.getElementById('usage').innerHTML = '<tr><th>client</th><th>model</th><th>req</th><th>in/out tok</th><th>est $</th></tr>' +
    (s.usage_today.map(x => '<tr><td>'+esc(x.client)+'</td><td><code>'+esc(x.model||x.route)+'</code></td><td>'+x.requests+'</td><td>'+x.in_tokens+'/'+x.out_tokens+'</td><td>$'+Number(x.est_usd).toFixed(6)+'</td></tr>').join('') || '<tr><td colspan=5 class=mut>none yet</td></tr>');
  document.getElementById('recent').innerHTML = '<tr><th>time</th><th>client</th><th>model</th><th>status</th><th>ms</th></tr>' +
    (s.recent.map(x => '<tr><td>'+new Date(x.at*1000).toLocaleTimeString()+'</td><td>'+esc(x.client)+'</td><td><code>'+esc(x.model)+'</code></td><td class="'+(x.status===200?'ok':'bad')+'">'+x.status+'</td><td>'+x.ms+'</td></tr>').join('') || '<tr><td colspan=5 class=mut>none yet</td></tr>');
  renderKeys();
}
function renderModels(){
  if(!S) return;
  const esc = x => String(x).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
  const owner = {};
  S.routes.forEach(x => (x.models||[]).forEach(m => owner[m] = x.prefix));
  const q = (document.getElementById('q').value||'').toLowerCase();
  let rows = S.models.filter(x => x.id.toLowerCase().includes(q)).map(x => '<tr><td><input type="checkbox" checked onchange="gwModelToggle(\\''+esc(owner[x.id]||'')+'\\',\\''+esc(x.id)+'\\',this.checked)"></td><td><code>'+esc(x.id)+'</code></td><td>'+esc(x.owned_by)+'</td><td><button class="act" onclick="testModel(\\''+esc(x.id)+'\\',this)">test</button> <span class="tres mut"></span></td></tr>').join('');
  S.routes.forEach(x => (x.disabled_models||[]).forEach(m => {
    if(!m.toLowerCase().includes(q)) return;
    rows += '<tr><td><input type="checkbox" onchange="gwModelToggle(\\''+esc(x.prefix)+'\\',\\''+esc(m)+'\\',this.checked)"></td><td><code>'+esc(m)+'</code></td><td class="mut">disabled</td><td></td></tr>';
  }));
  document.getElementById('models').innerHTML = '<tr><th>on</th><th>id</th><th>via</th><th></th></tr>' + (rows || '<tr><td colspan=4 class=mut>none</td></tr>');
  document.getElementById('nmodels').textContent = S.models.length;
}
async function api(path, body){
  const r = await fetch(path, {method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(body)});
  const j = await r.json().catch(() => ({}));
  if(!r.ok) alert('failed: ' + (j.error || r.status));
  return {ok: r.ok, j};
}
async function setCfg(body){ await api('/api/config', body); load(); }
window.gwToggle = (prefix, on) => setCfg({route: prefix, disabled: !on});
window.gwModelToggle = (prefix, id, on) => setCfg({route: prefix, model: id, disabled: !on});
window.testModel = async (id, btn) => {
  btn.disabled = true;
  const cell = btn.parentElement.querySelector('.tres');
  cell.textContent = '…';
  const {ok, j} = await api('/api/test', {model: id});
  if(ok && j.ok){ cell.textContent = j.winner.split('/').pop() + ' ' + (j.attempts.find(a=>a.ok)||{}).ms + 'ms ✓'; cell.className = 'tres ok'; }
  else { const a = (j.attempts||[]).find(a=>!a.ok); cell.textContent = (a ? (a.error||'') : 'failed').slice(0,60); cell.className = 'tres bad'; }
  btn.disabled = false;
};
function renderKeys(){
  const esc = x => String(x).replace(/[&<>"]/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
  document.getElementById('keyforms').innerHTML = S.routes.filter(x => x.need).map(x =>
    '<div class="keyrow"><code>'+esc(x.prefix)+'</code><span class="mut">'+esc(x.need)+'</span>' +
    (x.key_hint ? '<span class="pill ok">set '+esc(x.key_hint)+'</span>' : '<span class="pill bad">missing</span>') +
    ' <input type="password" id="k-'+esc(x.prefix)+'" placeholder="paste new key"><button class="act" onclick="saveKey(\\''+esc(x.prefix)+'\\')">save</button></div>'
  ).join('') || '<div class="mut">no keyed routes</div>';
  api('/api/clients', {action:'list'}).then(({j}) => {
    document.getElementById('clients').innerHTML = '<tr><th>client</th><th>rpm</th><th>req/day (used)</th><th>tok/day (used)</th><th>spent $</th><th>key</th><th></th></tr>' +
      ((j.clients||[]).map(c => '<tr><td>'+esc(c.name)+'</td><td><input type="number" id="rpm-'+esc(c.name)+'" value="'+c.rpm+'" style="width:5em"></td><td><input type="number" id="req-'+esc(c.name)+'" value="'+c.daily_requests+'" style="width:7em"><br><span class="mut">used '+c.used_req+'</span></td><td><input type="number" id="tok-'+esc(c.name)+'" value="'+c.daily_tokens+'" style="width:8em"><br><span class="mut">used '+c.used_tok+'</span></td><td>$'+Number(c.spent_usd).toFixed(4)+'</td><td class="mut">'+esc(c.key_hint)+'</td><td><button class="act" onclick="saveClient(\\''+esc(c.name)+'\\')">save</button> <button class="act" onclick="revokeClient(\\''+esc(c.name)+'\\')">revoke</button></td></tr>').join('') || '<tr><td colspan=7 class=mut>none</td></tr>');
  });
}
window.saveKey = async (prefix) => {
  const v = document.getElementById('k-'+prefix).value;
  if(!v) return;
  const {ok} = await api('/api/keys', {route: prefix, key: v});
  if(ok) load();
};
window.addClient = async () => {
  const {ok, j} = await api('/api/clients', {action:'add', name: document.getElementById('newname').value, rpm: +document.getElementById('newrpm').value, daily_requests: +document.getElementById('newdaily').value, daily_tokens: +document.getElementById('newtok').value});
  if(ok){ document.getElementById('newtoken').innerHTML = 'new client key (shown once): <code>'+esc(j.token)+'</code>'; load(); }
};
window.saveClient = async (name) => {
  const {ok} = await api('/api/clients', {action:'update', name,
    rpm: +document.getElementById('rpm-'+name).value,
    daily_requests: +document.getElementById('req-'+name).value,
    daily_tokens: +document.getElementById('tok-'+name).value});
  if(ok) load();
};
window.revokeClient = async (name) => { if(confirm('revoke '+name+'?')){ await api('/api/clients', {action:'revoke', name}); load(); } };
function applyTheme(t){ document.documentElement.setAttribute('data-theme', t); try{ localStorage.setItem('gw-theme', t); }catch(e){} const b = document.getElementById('themebtn'); if(b) b.textContent = t === 'light' ? 'dark' : 'light'; }
window.toggleTheme = () => applyTheme(document.documentElement.getAttribute('data-theme') === 'light' ? 'dark' : 'light');
let savedTheme = 'dark'; try{ savedTheme = localStorage.getItem('gw-theme') || 'dark'; }catch(e){}
applyTheme(savedTheme);
load(); setInterval(load, 30000);
</script></body></html>

"""




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
                a["est_usd"] = round(a["est_usd"] + row["est_usd"], 6)
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
                if r["prefix"] == "local/":
                    mids = ["local/" + m for m in self._ollama_models(r)]
                else:
                    mids = [r["prefix"] + m for m in r.get("models", [])]
                key = r.get("api_key") or ""
                routes.append({
                    "prefix": r["prefix"], "name": r.get("name", "?"),
                    "ready": (not r.get("api_key_env")) or bool(key),
                    "need": r.get("api_key_env"),
                    "key_hint": ("…" + key[-4:]) if key else None,
                    "enabled": self._route_enabled(r),
                    "models": mids,
                    "disabled_models": r.get("disabled_models") or [],
                })
            mirrors = {}
            for group, members in self.server.cfg.get("mirrors", {}).items():
                mirrors[group] = self._mirror_plan(members)
            return self._json(200, {
                "models": self._virtual_models(),
                "routes": routes,
                "mirrors": mirrors,
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
                        "est_usd": round(i / 1e6 * p["in"] + o / 1e6 * p["out"], 6)})
        return out

    def _route_enabled(self, route):
        return not route.get("disabled")

    def _model_enabled(self, route, virtual_id):
        return virtual_id not in (route.get("disabled_models") or [])

    def _virtual_models(self):
        data = []
        for route in self.server.cfg.get("routes", []):
            if not self._route_enabled(route):
                continue
            if route["prefix"] == "local/":
                for m in self._ollama_models(route):
                    vid = "local/" + m
                    if self._model_enabled(route, vid):
                        data.append({"id": vid, "object": "model",
                                     "owned_by": "ollama"})
            else:
                for m in route.get("models", []):
                    vid = route["prefix"] + m
                    if self._model_enabled(route, vid):
                        data.append({"id": vid, "object": "model",
                                     "owned_by": route.get("name", "upstream")})
        for key in self.server.cfg.get("mirrors", {}):
            data.append({"id": key, "object": "model", "owned_by": "mirror"})
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

    def _save_config(self):
        """Persist routes/clients/mirrors, never upstream api_key secrets
        (those live in the env file and only in memory)."""
        cfg = self.server.cfg
        clean_routes = []
        for r in cfg.get("routes", []):
            c = {k: v for k, v in r.items() if k != "api_key"}
            clean_routes.append(c)
        with open(CONFIG_PATH, "w") as f:
            json.dump({"listen": cfg.get("listen"), "clients": cfg.get("clients"),
                       "routes": clean_routes, "mirrors": cfg.get("mirrors", {})},
                      f, indent=2)
        os.chmod(CONFIG_PATH, 0o600)

    def _read_json(self):
        length = int(self.headers.get("Content-Length", 0))
        return json.loads((self.rfile.read(length) if length else b"{}").decode())

    def _write_env(self, var, value):
        """Upsert one VAR=value line in the 0600 env file."""
        lines = []
        if os.path.exists(ENV_PATH):
            with open(ENV_PATH) as f:
                lines = f.read().splitlines()
        lines = [l for l in lines if not l.startswith(var + "=") and l.strip() != var + "="]
        if value:
            lines.append(f"{var}={value}")
        os.makedirs(os.path.dirname(ENV_PATH), exist_ok=True)
        with open(ENV_PATH, "w") as f:
            f.write("\n".join(lines) + "\n")
        os.chmod(ENV_PATH, 0o600)

    def do_POST(self):
        if self.path == "/api/test":
            try:
                req = self._read_json()
            except Exception:
                return self._json(400, {"error": "invalid JSON body"})
            return self._test_model(req.get("model", ""))
        if self.path == "/api/keys":
            try:
                req = self._read_json()
            except Exception:
                return self._json(400, {"error": "invalid JSON body"})
            route = next((r for r in self.server.cfg.get("routes", [])
                          if r["prefix"] == req.get("route")), None)
            if route is None or not route.get("api_key_env"):
                return self._json(404, {"error": "route takes no key"})
            key = (req.get("key") or "").strip()
            if not key:
                return self._json(400, {"error": "empty key"})
            try:
                self._write_env(route["api_key_env"], key)
            except Exception as e:
                return self._json(500, {"error": f"env write failed: {e}"})
            route["api_key"] = key
            return self._json(200, {"ok": True, "hint": "…" + key[-4:]})
        if self.path == "/api/clients":
            try:
                req = self._read_json()
            except Exception:
                return self._json(400, {"error": "invalid JSON body"})
            clients = self.server.cfg.setdefault("clients", {})
            action = req.get("action")
            if action == "add":
                name = (req.get("name") or "").strip()
                if not name:
                    return self._json(400, {"error": "name required"})
                if any(c.get("name") == name for c in clients.values()):
                    return self._json(400, {"error": "name taken"})
                token = secrets.token_hex(32)
                clients[token] = {"name": name,
                                  "rpm": int(req.get("rpm") or 60),
                                  "daily_requests": int(req.get("daily_requests") or 1000),
                                  "daily_tokens": int(req.get("daily_tokens") or 2000000)}
                try:
                    self._save_config()
                except Exception as e:
                    del clients[token]
                    return self._json(500, {"error": f"persist failed: {e}"})
                return self._json(200, {"ok": True, "token": token,
                                        "note": "shown once — copy it now"})
            if action == "update":
                name = req.get("name")
                targets = [c for c in clients.values() if c.get("name") == name]
                if not targets:
                    return self._json(404, {"error": "unknown client"})
                for field in ("rpm", "daily_requests", "daily_tokens"):
                    if req.get(field) is not None:
                        try:
                            targets[0][field] = int(req[field])
                        except (TypeError, ValueError):
                            return self._json(400, {"error": f"bad {field}"})
                try:
                    self._save_config()
                except Exception as e:
                    return self._json(500, {"error": f"persist failed: {e}"})
                return self._json(200, {"ok": True})
            if action == "revoke":
                name = req.get("name")
                gone = [t for t, c in clients.items() if c.get("name") == name]
                if not gone:
                    return self._json(404, {"error": "unknown client"})
                for t in gone:
                    del clients[t]
                try:
                    self._save_config()
                except Exception as e:
                    return self._json(500, {"error": f"persist failed: {e}"})
                return self._json(200, {"ok": True})
            if action == "list":
                spent = {}
                for row in self._today_spend():
                    a = spent.setdefault(row["client"], {"tok": 0, "usd": 0.0, "req": 0})
                    a["tok"] += row["in_tokens"] + row["out_tokens"]
                    a["usd"] = round(a["usd"] + row["est_usd"], 6)
                    a["req"] += row["requests"]
                return self._json(200, {"clients": [
                    {"name": c.get("name"), "rpm": c.get("rpm"),
                     "daily_requests": c.get("daily_requests"),
                     "daily_tokens": c.get("daily_tokens"),
                     "key_hint": "…" + t[-4:],
                     "used_req": spent.get(c.get("name"), {}).get("req", 0),
                     "used_tok": spent.get(c.get("name"), {}).get("tok", 0),
                     "spent_usd": spent.get(c.get("name"), {}).get("usd", 0.0)}
                    for t, c in clients.items()]})
            return self._json(400, {"error": "action: add | update | revoke | list"})
        if self.path == "/api/config":
            try:
                req = self._read_json()
            except Exception:
                return self._json(400, {"error": "invalid JSON body"})
            route = next((r for r in self.server.cfg.get("routes", [])
                          if r["prefix"] == req.get("route")), None)
            if route is None:
                return self._json(404, {"error": "unknown route"})
            if "disabled" in req:
                route["disabled"] = bool(req["disabled"])
            if "model" in req:
                dis = set(route.get("disabled_models") or [])
                if req.get("disabled"):
                    dis.add(req["model"])
                else:
                    dis.discard(req["model"])
                route["disabled_models"] = sorted(dis)
            try:
                self._save_config()
            except Exception as e:
                return self._json(500, {"error": f"persist failed: {e}"})
            return self._json(200, {"ok": True})
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
        # NOTE: routing happens in the mirror/candidate block below (which
        # also owns the 402/404 answers); nothing may return early here.
        if body.get("stream") and "stream_options" not in body:
            body["stream_options"] = {"include_usage": True}

        t0 = time.time()

        # Mirror groups (auto/<name>): try each member upstream in order,
        # failing over on 429/5xx or connection errors. Single models take
        # the same path with one candidate, so behavior is unchanged.
        candidates = []
        skipped_disabled = False
        for member in self.server.cfg.get("mirrors", {}).get(model, [model]):
            r, um = self._route_for(member)
            if r is None:
                continue
            if not self._route_enabled(r) or not self._model_enabled(r, member):
                skipped_disabled = True
                continue
            if r.get("api_key_env") and not r.get("api_key"):
                candidates.append((r, um, (402, json.dumps({"error": (
                    f"route '{r['prefix']}' needs a free provider key: "
                    f"set {r['api_key_env']} and restart the gateway")}).encode())))
                continue
            candidates.append((r, um, None))
        if not candidates:
            if skipped_disabled:
                return self._json(403, {"error": f"model '{model}' is disabled in the gateway dashboard"})
            return self._json(404, {"error": f"no route for model '{model}'"})
        if len(candidates) == 1 and candidates[0][2] is not None:
            status, payload = candidates[0][2]
            return self._json(status, json.loads(payload.decode()))

        # Cheapest first: free routes sort to the front automatically;
        # config order breaks price ties. Quota failover unchanged
        # (429/5xx → next cheapest). Paid routes never join mirror
        # groups uninvited — free→paid escalation would be surprise spend.
        def _cost(c):
            r, um, _ = c
            p = self._price_for(r["prefix"], um)
            return p["in"] + p["out"]
        candidates = sorted(candidates, key=_cost)

        last_fail = None
        for route, upstream_model, prefail in candidates:
            if prefail is not None:
                last_fail = (402, prefail)
                self._log(name, route["prefix"], model, 402,
                          int((time.time() - t0) * 1000))
                continue
            cand_body = dict(body)
            cand_body["model"] = upstream_model
            # Per-route thinking suppression (OpenRouter free reasoning
            # flood): excluding reasoning returns final content + tool
            # calls directly — what an agent loop needs.
            if route.get("drop_reasoning") and "reasoning" not in cand_body:
                cand_body["reasoning"] = {"exclude": True}
            cand_out = json.dumps(cand_body).encode()
            try:
                resp = self._post_upstream(route, cand_out)
            except Exception as e:
                last_fail = (502, json.dumps({"error": f"upstream unreachable: {e}"}).encode())
                self._log(name, route["prefix"], model, 502,
                          int((time.time() - t0) * 1000))
                continue
            if resp.status == 429 or resp.status >= 500:
                payload = resp.read()
                last_fail = (resp.status, payload)
                self._log(name, route["prefix"], model, resp.status,
                          int((time.time() - t0) * 1000))
                continue
            if body.get("stream") and route.get("burst"):
                if self._burst(resp, cand_body, route, model, name, t0):
                    return
                last_fail = (504, json.dumps({"error": "upstream stalled/failed"}).encode())
                continue
            elif body.get("stream"):
                self.send_response(resp.status)
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
                self.send_response(resp.status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
                intok = outtok = 0
                if resp.status == 200:
                    try:
                        intok, outtok = self._usage_of(json.loads(payload.decode()))
                    except Exception:
                        pass
                self._log(name, route["prefix"], model, resp.status,
                          int((time.time() - t0) * 1000), intok, outtok)
                return
            self._log(name, route["prefix"], model, resp.status,
                      int((time.time() - t0) * 1000))
            return
        # Every candidate failed with 429/5xx/unreachable: forward the last
        # upstream verdict instead of a generic error.
        if last_fail is not None:
            status, payload = last_fail
            try:
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)
            except BrokenPipeError:
                pass
            return

    def _mirror_plan(self, members):
        """Ordered failover plan for a mirror group: cheapest first, with
        per-member price and reachability. Powers the dashboard display and
        documents the order the chat path tries."""
        plan = []
        for member in members:
            r, um = self._route_for(member)
            if r is None:
                plan.append({"model": member, "route": None, "price": None,
                             "ready": False, "why": "no route"})
                continue
            p = self._price_for(r["prefix"], um)
            ready = (not r.get("api_key_env")) or bool(r.get("api_key"))
            enabled = self._route_enabled(r) and self._model_enabled(r, member)
            plan.append({"model": member, "route": r["prefix"],
                         "price": p, "ready": ready, "enabled": enabled,
                         "why": None if (ready and enabled) else "disabled" if enabled else "needs key"})
        plan.sort(key=lambda e: ((e["price"] or {}).get("in", 0) +
                                 (e["price"] or {}).get("out", 0)))
        return plan

    def _test_model(self, model):
        """One-click probe from the dashboard: tiny blocking call through
        the normal candidate machinery (first success wins, same failover).
        Logged as client 'dashboard' so tests show up in spend honestly."""
        members = self.server.cfg.get("mirrors", {}).get(model, [model])
        attempts = []
        t0 = time.time()
        for member in members:
            r, um = self._route_for(member)
            if r is None:
                attempts.append({"model": member, "ok": False, "error": "no route"})
                continue
            if not self._route_enabled(r) or not self._model_enabled(r, member):
                attempts.append({"model": member, "ok": False, "error": "disabled"})
                continue
            if r.get("api_key_env") and not r.get("api_key"):
                attempts.append({"model": member, "ok": False,
                                 "error": f"needs {r['api_key_env']}"})
                continue
            out = json.dumps({"model": um, "max_tokens": 16, "stream": False,
                              "messages": [{"role": "user",
                                            "content": "reply with exactly: GW-TEST"}]}).encode()
            a0 = time.time()
            try:
                resp = self._post_upstream(r, out)
                payload = resp.read()
                ms = int((time.time() - a0) * 1000)
                if resp.status != 200:
                    attempts.append({"model": member, "ok": False,
                                     "error": f"HTTP {resp.status}",
                                     "detail": payload.decode()[:160], "ms": ms})
                    self._log("dashboard", r["prefix"], model, resp.status, ms)
                    continue
                d = json.loads(payload.decode())
                c = d["choices"][0]
                attempts.append({"model": member, "ok": True, "ms": ms,
                                 "finish": c.get("finish_reason"),
                                 "text": (c["message"].get("content") or "")[:80]})
                self._log("dashboard", r["prefix"], model, 200, ms,
                          *self._usage_of(d))
                return self._json(200, {"ok": True, "winner": member,
                                        "total_ms": int((time.time() - t0) * 1000),
                                        "attempts": attempts})
            except Exception as e:
                ms = int((time.time() - a0) * 1000)
                attempts.append({"model": member, "ok": False,
                                 "error": f"unreachable: {e}", "ms": ms})
                self._log("dashboard", r["prefix"], model, 502, ms)
                continue
        return self._json(200, {"ok": False, "attempts": attempts})

    def _post_upstream(self, route, out):
        """POST body bytes to the route's upstream, return the response.
        Raises on connection failure; caller checks status."""
        parts = urllib.parse.urlsplit(route["base"])
        https = parts.scheme == "https"
        host = parts.hostname or "127.0.0.1"
        port = parts.port or (443 if https else 80)
        base_path = parts.path.rstrip("/")
        conn_cls = http.client.HTTPSConnection if https else http.client.HTTPConnection
        conn = conn_cls(host, port, timeout=300)
        headers = {"Content-Type": "application/json"}
        if route.get("api_key"):
            headers["Authorization"] = "Bearer " + route["api_key"]
        for extra in route.get("headers", {}).items():
            headers[extra[0]] = extra[1]
        conn.request("POST", base_path + "/chat/completions",
                     body=out, headers=headers)
        return conn.getresponse()

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
        # Returns True when the client got a response, False when the
        # caller should fail over to the next mirror (nothing sent).
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
                return True
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
            return False

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
        return True

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
