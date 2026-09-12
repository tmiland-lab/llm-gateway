#!/bin/sh
# Pre-deploy smoke gate: boots the gateway on a throwaway port with temp
# config/state, exercises every endpoint, then tears it down. Live service
# is NEVER touched. Exit nonzero on any failure.
# Usage: sh script/smoke.sh [path/to/gateway.py]
set -u
GW="${1:-gateway.py}"
PORT=4144
BASE="http://127.0.0.1:$PORT"
TMP=$(mktemp -d)
PASS=0
FAIL=0

ok() { PASS=$((PASS + 1)); echo "ok: $1"; }
bad() { FAIL=$((FAIL + 1)); echo "FAIL: $1"; }

trap 'kill $PID 2>/dev/null; rm -rf "$TMP"' EXIT INT TERM

python3 -c "import ast; ast.parse(open('$GW').read())" || { echo "FAIL: python syntax"; exit 1; }
echo "ok: python syntax"

cat > "$TMP/config.json" <<EOF
{
  "listen": "127.0.0.1:$PORT",
  "clients": {"smoke": {"name": "smoke", "rpm": 60, "daily_requests": 2000, "daily_tokens": 2000000}},
  "routes": [
    {"name": "ollama", "prefix": "local/", "base": "http://localhost:11434/v1",
     "api_key_env": null, "models": [], "price_per_mtok": {"in": 0, "out": 0}},
    {"name": "smoke-upstream", "prefix": "t/", "base": "http://127.0.0.1:9/v1",
     "api_key_env": null, "models": ["x"], "price_per_mtok": {"in": 0, "out": 0}}
  ],
  "mirrors": {"auto/smoke": ["local/qwen2.5-coder:1.5b", "t/x"]}
}
EOF
touch "$TMP/env"
GATEWAY_CONFIG="$TMP/config.json" GATEWAY_DB="$TMP/usage.db" GATEWAY_ENV="$TMP/env" \
  nohup python3 "$GW" > "$TMP/srv.log" 2>&1 &
PID=$!

for i in $(seq 1 30); do
  curl -s -m 2 -o /dev/null "$BASE/healthz" 2>/dev/null && break
  sleep 1
  if [ "$i" = "30" ]; then echo "FAIL: server never came up"; sed -n '1,10p' "$TMP/srv.log"; exit 1; fi
done
echo "ok: boots, healthz 200"

BODY=$(curl -s -m 10 "$BASE/ui")
case "$BODY" in
  *'class="layout"'*) ok "ui shell layout" ;;
  *) bad "ui shell layout" ;;
esac
case "$BODY" in
  *'</html>') ok "ui closes html" ;;
  *) bad "ui closes html" ;;
esac
echo "$BODY" | python3 -c "
import sys
h = sys.stdin.read()
open('$TMP/ui.js','w').write(h[h.index('<script>')+8:h.index('</script>')])"
node --check "$TMP/ui.js" 2>/dev/null && ok "served JS parses" || bad "served JS parses"

curl -s -m 10 "$BASE/v1/models" | python3 -c "import json,sys; assert len(json.load(sys.stdin)['data'])>0" 2>/dev/null && ok "models non-empty" || bad "models non-empty"
curl -s -m 10 "$BASE/api/status" | python3 -c "import json,sys; s=json.load(sys.stdin); assert all(k in s for k in ('models','routes','mirrors','usage_today','recent')); assert 'auto/smoke' in s['mirrors']" 2>/dev/null && ok "status shape + mirrors" || bad "status shape + mirrors"
curl -s -m 10 -o /dev/null -w "%{http_code}" "$BASE/v1/usage" | grep -q 401 && ok "usage 401 w/o key" || bad "usage 401 w/o key"
curl -s -m 10 -o /dev/null -w "%{http_code}" -X POST "$BASE/v1/chat/completions" -H "Authorization: Bearer wrong" -d '{}' | grep -q 401 && ok "chat 401 bad key" || bad "chat 401 bad key"
curl -s -m 10 -X POST "$BASE/api/config" -H 'Content-Type: application/json' -d '{"route":"t/","disabled":true}' | grep -q '"ok": true' && ok "config toggle writes" || bad "config toggle writes"
curl -s -m 10 "$BASE/v1/models" | python3 -c "import json,sys; assert not any(m['id']=='t/x' for m in json.load(sys.stdin)['data'])" 2>/dev/null && ok "disabled hidden from catalog" || bad "disabled hidden from catalog"
curl -s -m 10 -X POST "$BASE/api/config" -H 'Content-Type: application/json' -d '{"route":"t/","disabled":false}' >/dev/null || bad "config re-enable"

CHAT=$(curl -s -m 120 -X POST "$BASE/v1/chat/completions" -H "Authorization: Bearer smoke" -H 'Content-Type: application/json' -d '{"model":"local/qwen2.5-coder:1.5b","max_tokens":16,"messages":[{"role":"user","content":"say hi"}]}')
echo "$CHAT" | python3 -c "import json,sys; assert json.load(sys.stdin)['choices'][0]['message'].get('content')" 2>/dev/null && ok "local blocking chat answers" || bad "local blocking chat answers: $(echo "$CHAT" | head -c 120)"
curl -s -m 150 -N -X POST "$BASE/v1/chat/completions" -H "Authorization: Bearer smoke" -H 'Content-Type: application/json' -d '{"model":"local/qwen2.5-coder:1.5b","max_tokens":16,"stream":true,"messages":[{"role":"user","content":"say hi"}]}' -o "$TMP/sse.txt" -w 'stream=%{http_code}\n' | grep -q "stream=200" && ok "local stream 200" || bad "local stream 200"
grep -q "data: \[DONE\]" "$TMP/sse.txt" 2>/dev/null && ok "stream terminates [DONE]" || bad "stream terminates [DONE]"
curl -s -m 10 "$BASE/v1/usage" -H "Authorization: Bearer smoke" | python3 -c "import json,sys; t=json.load(sys.stdin)['today'][0]; assert t['in_tokens']>0 and t['out_tokens']>0, t" 2>/dev/null && ok "token accounting live" || bad "token accounting live"

echo "---"
echo "PASS=$PASS FAIL=$FAIL"
[ "$FAIL" = "0" ]
