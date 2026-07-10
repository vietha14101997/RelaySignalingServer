#!/usr/bin/env bash
# READ-ONLY preflight for a VPS that already runs other production apps.
# Changes NOTHING. Reports installed tools, port conflicts, existing Docker /
# Postgres / reverse-proxy, and recommends standalone vs proxied mode.
# Run:  sudo ./preflight-check.sh   (sudo → shows which process owns each port)
set -u

CONFLICTS=0
note() { printf '  %s\n' "$*"; }
ok()   { printf '  \033[32m✓\033[0m %s\n' "$*"; }
warn() { printf '  \033[33m! \033[0m%s\n' "$*"; CONFLICTS=$((CONFLICTS+1)); }
hdr()  { printf '\n\033[1m== %s ==\033[0m\n' "$*"; }

have() { command -v "$1" >/dev/null 2>&1; }

# Prefer ss; fall back to netstat. Column 5 = local address (…:PORT).
listeners() { { ss -tulpnH 2>/dev/null || netstat -tulpn 2>/dev/null; } ; }
port_owner() { listeners | awk -v p="$1" '$0 ~ ("[:.]" p "([^0-9]|$)")' | head -3; }
port_used()  { listeners | awk '{print $5}' | grep -Eq "[:.]$1$"; }

echo "RemoteScreen relay — VPS preflight (read-only). $(date)"
[ "$(id -u)" -ne 0 ] && note "(tip: run with sudo to see process names behind ports)"

hdr "OS"
if [ -r /etc/os-release ]; then . /etc/os-release; note "$PRETTY_NAME  ($(uname -r), $(uname -m))"; fi

hdr "Docker (will NOT be reinstalled if present)"
if have docker; then
	ok "docker: $(docker --version 2>/dev/null)"
	if docker info >/dev/null 2>&1; then ok "docker daemon: running"
	else warn "docker installed but daemon not reachable (need sudo? not started?)"; fi
	if docker compose version >/dev/null 2>&1; then ok "compose plugin: $(docker compose version --short 2>/dev/null)"
	else warn "docker compose plugin missing (setup-vps.sh installs it)"; fi
else
	note "docker NOT installed — setup-vps.sh will install Docker Engine + compose."
fi

hdr "Existing Docker footprint (yours + others)"
if have docker && docker info >/dev/null 2>&1; then
	note "Running containers + published ports:"
	docker ps --format '   {{.Names}}\t{{.Image}}\t{{.Ports}}' 2>/dev/null | sed 's/^/  /' || true
	note "Compose projects:"; docker compose ls 2>/dev/null | sed 's/^/   /' || true
	if docker ps --format '{{.Names}}' 2>/dev/null | grep -q '^remotescreen-relay'; then
		warn "a 'remotescreen-relay*' stack already exists — this would update it, not add a 2nd."
	else ok "no existing 'remotescreen-relay' project (safe to create fresh)"; fi
	note "Our volumes are project-prefixed (remotescreen-relay_*) → no clash with other stacks."
else
	note "(docker not usable yet — skipping container inspection)"
fi

hdr "Port conflicts"
note "Our stack needs these host ports:"
for p in 80 443; do
	if port_used "$p"; then warn "tcp/$p IN USE (Caddy needs it) → use PROXIED mode:"; port_owner "$p" | sed 's/^/       /'
	else ok "tcp/$p free (standalone Caddy OK)"; fi
done
for p in 3478 5349; do
	if port_used "$p"; then warn "$p IN USE (coturn needs it) — likely another TURN; resolve before deploy:"; port_owner "$p" | sed 's/^/       /'
	else ok "$p free (coturn OK)"; fi
done
if listeners | awk '{print $5}' | grep -Eq "[:.](4916[0-9]|491[7-9][0-9]|4920[0-9])$"; then
	warn "something listens in 49160-49200/udp (coturn relay range) — pick a different range in coturn/turnserver.conf"
else ok "49160-49200/udp relay range appears free"; fi

hdr "Postgres (ours is INTERNAL — never publishes 5432)"
if port_used 5432; then note "host :5432 is in use (another Postgres) — FINE: our Postgres is compose-internal, we do NOT bind 5432."
else note "host :5432 free (irrelevant either way — ours is internal)."; fi
if have docker && docker ps --format '{{.Image}}' 2>/dev/null | grep -qi postgres; then note "a Postgres container is already running — also fine; ours is a separate isolated instance + volume."; fi

hdr "Reverse proxy on 80/443 (decides deploy mode)"
PROXY=""
for x in nginx caddy traefik apache2 httpd; do have "$x" && PROXY="$PROXY $x(host)"; done
if have docker && docker info >/dev/null 2>&1; then
	for x in nginx caddy traefik; do docker ps --format '{{.Image}}' 2>/dev/null | grep -qi "$x" && PROXY="$PROXY $x(docker)"; done
fi
if [ -n "$PROXY" ]; then warn "reverse proxy detected:$PROXY → use PROXIED mode (docker-compose.proxied.yml); let it front the relay."
else ok "no obvious reverse proxy — standalone Caddy is fine IF 80/443 are free (see above)."; fi

hdr "Firewall (setup-vps.sh will NOT enable/reset it)"
if have ufw; then
	if ufw status 2>/dev/null | grep -q "Status: active"; then note "ufw ACTIVE — add rules with: setup-vps.sh --apply-firewall (additive, safe). Current:"; ufw status 2>/dev/null | sed 's/^/     /' | head -20
	else note "ufw installed but INACTIVE — we will NOT enable it (could cut the running app/SSH). Open ports manually if you use it."; fi
else note "ufw not installed — check your provider's cloud firewall / iptables instead."; fi

hdr "Resources"
note "disk /: $(df -h / | awk 'NR==2{print $4" free of "$2}')"
note "memory: $(free -h 2>/dev/null | awk 'NR==2{print $7" available of "$2}' || echo n/a)"
note "cpu: $(nproc 2>/dev/null || echo '?') cores"

hdr "VERDICT"
if [ "$CONFLICTS" -eq 0 ]; then
	printf '  \033[32mNo conflicts detected — STANDALONE mode looks safe.\033[0m\n'
	note "Next: cp .env.example .env → fill secrets → ./add-domain.sh <domain> [email]"
else
	printf '  \033[33m%s potential conflict(s) — read above.\033[0m\n' "$CONFLICTS"
	note "If 80/443 taken or a reverse proxy exists → PROXIED mode:"
	note "  docker compose -f docker-compose.yml -f docker-compose.proxied.yml up -d --build"
	note "  then point your existing proxy at 127.0.0.1:\${RELAY_HOST_PORT:-8443} (see README)."
fi
echo
exit 0
