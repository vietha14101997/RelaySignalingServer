#!/bin/sh
# coturn entrypoint: inject runtime-only settings (secret/realm/external-ip) and,
# if Caddy has already issued a cert for TURN_DOMAIN, enable TLS-TURN (turns:5349)
# using that cert. Runs each time the container starts, so restarting coturn after
# a cert is issued/renewed is all that's needed to pick it up.
set -eu

BASE=/etc/coturn/turnserver.conf
RUNTIME=/tmp/turnserver.runtime.conf
CADDY_CERT_DIR=/caddy-data/caddy/certificates
CERT_OUT=/etc/coturn/certs

: "${TURN_SECRET:?TURN_SECRET is required}"
: "${TURN_DOMAIN:?TURN_DOMAIN is required}"

cp "$BASE" "$RUNTIME"
{
	echo ""
	echo "# --- injected at runtime by entrypoint.sh ---"
	echo "static-auth-secret=${TURN_SECRET}"
	echo "realm=${TURN_DOMAIN}"
	if [ -n "${EXTERNAL_IP:-}" ]; then
		echo "external-ip=${EXTERNAL_IP}"
	fi
} >>"$RUNTIME"

# Cert discovery, in order:
#  1) COTURN_CERT_DIR (proxied mode): a mounted dir with fullchain.pem + privkey.pem
#     e.g. bind /etc/letsencrypt/live/<domain> from the host's existing proxy.
#  2) Caddy-issued cert (standalone mode): /caddy-data/caddy/certificates/.../<domain>.crt|.key
CRT=""
KEY=""
if [ -n "${COTURN_CERT_DIR:-}" ] && [ -f "${COTURN_CERT_DIR}/fullchain.pem" ] && [ -f "${COTURN_CERT_DIR}/privkey.pem" ]; then
	CRT="${COTURN_CERT_DIR}/fullchain.pem"
	KEY="${COTURN_CERT_DIR}/privkey.pem"
else
	CRT=$(find "$CADDY_CERT_DIR" -type f -name "${TURN_DOMAIN}.crt" 2>/dev/null | head -n1 || true)
	KEY=$(find "$CADDY_CERT_DIR" -type f -name "${TURN_DOMAIN}.key" 2>/dev/null | head -n1 || true)
fi

if [ -n "$CRT" ] && [ -n "$KEY" ]; then
	mkdir -p "$CERT_OUT"
	cp "$CRT" "$CERT_OUT/fullchain.pem"
	cp "$KEY" "$CERT_OUT/privkey.pem"
	chmod 600 "$CERT_OUT/fullchain.pem" "$CERT_OUT/privkey.pem"
	{
		echo "tls-listening-port=5349"
		echo "cert=${CERT_OUT}/fullchain.pem"
		echo "pkey=${CERT_OUT}/privkey.pem"
	} >>"$RUNTIME"
	echo "[coturn] TLS enabled for ${TURN_DOMAIN} (turns:5349)"
else
	echo "[coturn] No Caddy cert for ${TURN_DOMAIN} yet — serving plain TURN (3478 udp/tcp)."
	echo "[coturn] Re-run 'docker compose restart coturn' after the cert is issued."
fi

chmod 600 "$RUNTIME"
exec turnserver -c "$RUNTIME"
