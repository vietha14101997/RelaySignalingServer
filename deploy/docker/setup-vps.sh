#!/usr/bin/env bash
# SAFE VPS setup for a box that may already run other production apps.
# - Installs Docker Engine + Compose plugin ONLY if absent (never reinstalls).
# - Does NOT enable/reset the firewall (that could cut a running app or SSH).
#   Firewall rules are opt-in and ADDITIVE, applied only when ufw is already active.
# - Installs a coturn cert-refresh cron (unique name; standalone mode only).
#
# Run as root:  sudo ./preflight-check.sh   # look first!
#               sudo ./setup-vps.sh [--apply-firewall]
set -euo pipefail

APPLY_FW=0
[ "${1:-}" = "--apply-firewall" ] && APPLY_FW=1

if [ "$(id -u)" -ne 0 ]; then
	echo "Run as root:  sudo ./setup-vps.sh [--apply-firewall]" >&2
	exit 1
fi
COMPOSE_DIR="$(cd "$(dirname "$0")" && pwd)"

echo ">> Tip: run ./preflight-check.sh first to inspect conflicts (read-only)."

# ── Docker Engine + Compose plugin (install only if missing) ──
if command -v docker >/dev/null 2>&1; then
	echo ">> Docker already installed ($(docker --version)) — NOT reinstalling."
	if ! docker compose version >/dev/null 2>&1; then
		echo ">> compose plugin missing — installing just the plugin..."
		apt-get update && apt-get install -y docker-compose-plugin
	fi
else
	echo ">> Docker not found — installing Docker Engine (official repo)..."
	apt-get update
	apt-get install -y ca-certificates curl
	install -m 0755 -d /etc/apt/keyrings
	curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
	chmod a+r /etc/apt/keyrings/docker.asc
	# shellcheck disable=SC1091
	. /etc/os-release
	echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu ${VERSION_CODENAME} stable" \
		>/etc/apt/sources.list.d/docker.list
	apt-get update
	apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
fi
docker --version && docker compose version

# ── Firewall: NEVER auto-enable. Only add rules if ufw is already active + opted in ──
FW_PORTS=("22/tcp" "80/tcp" "443/tcp" "443/udp" "3478/tcp" "3478/udp" "5349/tcp" "49160:49200/udp")
echo
echo ">> Firewall: this stack needs these ports open at the host/cloud firewall:"
printf '     %s\n' "${FW_PORTS[@]}"
echo "   (If you use a standalone reverse proxy already, you may NOT need 80/443 here — see proxied mode.)"

if command -v ufw >/dev/null 2>&1 && ufw status 2>/dev/null | grep -q "Status: active"; then
	if [ "$APPLY_FW" -eq 1 ]; then
		echo ">> ufw is active + --apply-firewall given → adding rules (additive, non-destructive)..."
		for r in "${FW_PORTS[@]}"; do ufw allow "$r"; done
		ufw status verbose
	else
		echo ">> ufw is active. Re-run with --apply-firewall to ADD the rules above (won't reset existing ones)."
	fi
else
	echo ">> ufw inactive or absent → NOT touching the firewall (avoids cutting the running app/SSH)."
	echo "   Open the ports above via your cloud provider's firewall or your existing iptables setup."
fi

# ── Cert-refresh cron (standalone/Caddy mode): restart coturn weekly to re-read renewed certs ──
CRON=/etc/cron.d/remotescreen-coturn-cert
cat >"$CRON" <<EOF
# Restart coturn weekly so it re-reads Caddy-renewed TLS certs (standalone mode).
0 4 * * 1 root cd ${COMPOSE_DIR} && docker compose restart coturn >/dev/null 2>&1
EOF
chmod 0644 "$CRON"
echo ">> Installed weekly coturn cert-refresh cron: $CRON (harmless in proxied mode)."

cat <<EOF

Done — nothing destructive was performed.
Next:
  cp .env.example .env    # fill JWT_SECRET / POSTGRES_PASSWORD / TURN_SECRET
  STANDALONE (80/443 free):  ./add-domain.sh <domain> [email]
  PROXIED   (proxy exists):  docker compose -f docker-compose.yml -f docker-compose.proxied.yml up -d --build
                              then point your proxy at 127.0.0.1:\${RELAY_HOST_PORT:-8443} (README)
EOF
