#!/usr/bin/env bash
# Attach a domain to the relay: point RELAY_DOMAIN/TURN_DOMAIN/EXTERNAL_IP in
# .env, bring the stack up (Caddy auto-issues a Let's Encrypt cert), then restart
# coturn so TLS-TURN (turns:5349) uses that cert.
#
# Usage:  ./add-domain.sh <domain> [acme-email]
# Prereq: an A record for <domain> pointing at this VPS's public IP.
set -euo pipefail
cd "$(dirname "$0")"

DOMAIN="${1:-}"
EMAIL="${2:-}"
if [ -z "$DOMAIN" ]; then
	echo "Usage: $0 <domain> [acme-email]" >&2
	exit 1
fi

ENV_FILE=.env
if [ ! -f "$ENV_FILE" ]; then
	echo "Missing .env — run: cp .env.example .env  and fill the secrets first." >&2
	exit 1
fi

# Safety: this (standalone) mode runs Caddy on 80/443. If something already
# listens there, do NOT stomp the running app — steer to proxied mode.
if { ss -tulnH 2>/dev/null || netstat -tuln 2>/dev/null; } | awk '{print $5}' \
	| grep -Eq "[:.](80|443)$"; then
	echo "!! Port 80/443 is already in use on this host (an existing app/proxy)." >&2
	echo "   Standalone Caddy would fail to bind. Use PROXIED mode instead:" >&2
	echo "     docker compose -f docker-compose.yml -f docker-compose.proxied.yml up -d --build" >&2
	echo "   then front the relay with your existing proxy (see README). Aborting to stay safe." >&2
	exit 1
fi

# ── Public IP ──
PUBIP="$(curl -fsS https://api.ipify.org 2>/dev/null || curl -fsS https://ifconfig.me 2>/dev/null || true)"
if [ -z "$PUBIP" ]; then
	read -rp "Could not auto-detect public IP. Enter this VPS's public IP: " PUBIP
fi
echo ">> Public IP: $PUBIP"

# ── DNS sanity check ──
RESOLVED="$(getent hosts "$DOMAIN" 2>/dev/null | awk '{print $1}' | head -n1 || true)"
if [ "$RESOLVED" != "$PUBIP" ]; then
	echo "!! WARNING: $DOMAIN resolves to '${RESOLVED:-<none>}', not $PUBIP."
	echo "   Create an A record  $DOMAIN -> $PUBIP  first, or Let's Encrypt will fail."
	read -rp "   Continue anyway? [y/N] " yn
	[ "${yn:-N}" = "y" ] || [ "${yn:-N}" = "Y" ] || exit 1
fi

# ── Write domain settings into .env (idempotent) ──
set_kv() {
	local k="$1" v="$2"
	if grep -q "^${k}=" "$ENV_FILE"; then
		sed -i.bak "s|^${k}=.*|${k}=${v}|" "$ENV_FILE"
	else
		echo "${k}=${v}" >>"$ENV_FILE"
	fi
}
set_kv RELAY_DOMAIN "$DOMAIN"
set_kv TURN_DOMAIN "$DOMAIN"
set_kv EXTERNAL_IP "$PUBIP"
[ -n "$EMAIL" ] && set_kv ACME_EMAIL "$EMAIL"
rm -f "${ENV_FILE}.bak"
echo ">> .env updated (RELAY_DOMAIN, TURN_DOMAIN, EXTERNAL_IP${EMAIL:+, ACME_EMAIL})."

# ── Bring the stack up (builds the relay image on first run) ──
echo ">> docker compose up -d --build ..."
docker compose up -d --build

# ── Wait for Caddy to obtain the certificate ──
echo ">> Waiting for Caddy to issue a certificate for $DOMAIN (up to ~120s)..."
issued=0
for _ in $(seq 1 40); do
	if docker compose exec -T caddy sh -c "ls /data/caddy/certificates/*/$DOMAIN/$DOMAIN.crt" >/dev/null 2>&1; then
		issued=1
		break
	fi
	sleep 3
done

if [ "$issued" = 1 ]; then
	echo ">> Certificate issued. Restarting coturn to enable turns:5349 ..."
	docker compose restart coturn
else
	echo "!! Cert not detected yet. Check:  docker compose logs caddy"
	echo "   Once issued, run:  docker compose restart coturn"
fi

cat <<EOF

──────────────────────────────────────────────
Deployment for https://$DOMAIN is up. Verify:

  curl -s https://$DOMAIN/health                    # -> ok
  curl -s https://$DOMAIN/ice-servers-public | jq   # STUN + turn:$DOMAIN:3478

Point the client/host relay URL at:  https://$DOMAIN  (WebSocket: wss://$DOMAIN)
TURN over TLS (turns:5349) is active if the coturn log shows "TLS enabled".
──────────────────────────────────────────────
EOF
