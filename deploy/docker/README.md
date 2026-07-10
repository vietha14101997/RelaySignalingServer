# RemoteScreen Relay — Docker deployment (Ubuntu VPS)

Runs the full signaling backend as containers: **Caddy** (auto-HTTPS reverse proxy) → **relay** (Go) → **Postgres**, plus **coturn** (STUN/TURN) on the host network. One script installs Docker + firewall; one script attaches a domain and issues TLS.

```
Internet ─┬─ 443/tcp+udp ─▶ Caddy ──(http)──▶ relay:8443 ──▶ Postgres
          │                   └ auto Let's Encrypt cert (shared → coturn)
          └─ 3478 udp/tcp, 5349/tls, 49160-49200/udp ─▶ coturn (host net)
```

## Safe deploy on a shared VPS (READ FIRST if the box already runs other apps)
This stack is designed to coexist with existing production apps:
- **Postgres is isolated** — our Postgres is compose-internal and does **not** publish `5432`. Your existing Postgres (host or container) is untouched.
- **Everything is project-scoped** — compose project `remotescreen-relay`, so containers/networks/volumes are prefixed (`remotescreen-relay_*`) and won't clash.
- **`setup-vps.sh` is non-destructive** — installs Docker only if absent, and **never enables/resets the firewall** (that could cut the running app or your SSH).

Run the read-only inspector first — it changes nothing and tells you which mode to use:
```bash
sudo ./preflight-check.sh
```
The only real conflict points are **80/443** (bundled Caddy) and **3478/5349 + 49160-49200/udp** (coturn). If a reverse proxy already owns 80/443 → use **Proxied mode** (below); `add-domain.sh` also refuses to run if 80/443 are taken.

## Prerequisites
- Ubuntu VPS (22.04/24.04), root/sudo.
- A domain with an **A record → the VPS public IP** (set this before `add-domain.sh`).
- Firewall ports (open at your cloud firewall, or via `setup-vps.sh --apply-firewall` only if ufw is already active): `80, 443 (tcp+udp)` *(standalone only)*, `3478 (tcp+udp), 5349 (tcp), 49160-49200 (udp)`.

## Deploy — Standalone mode (80/443 are free)
```bash
# on the VPS, from this directory (deploy/docker/)
sudo ./preflight-check.sh                # read-only; confirm no conflicts
sudo ./setup-vps.sh                       # Docker (if absent) + renewal cron; add --apply-firewall only if ufw active

cp .env.example .env                      # then edit .env:
#   JWT_SECRET=$(openssl rand -hex 32)
#   POSTGRES_PASSWORD=$(openssl rand -hex 24)
#   TURN_SECRET=$(openssl rand -hex 32)
#   RELAY_ADMIN_TOKEN=$(openssl rand -hex 16)   # optional

./add-domain.sh relay.example.com you@example.com
```
`add-domain.sh` writes the domain/IP into `.env`, runs `docker compose up -d --build`, waits for Caddy to issue the cert, then restarts coturn to enable `turns:5349`.

## Deploy — Proxied mode (VPS already has nginx/Caddy/Traefik on 80/443)
Keeps the relay off 80/443 so it never fights the running app.
```bash
sudo ./preflight-check.sh
sudo ./setup-vps.sh                        # skips firewall enable; installs Docker only if missing
cp .env.example .env                       # fill secrets; set RELAY_DOMAIN, TURN_DOMAIN, EXTERNAL_IP, RELAY_HOST_PORT
docker compose -f docker-compose.yml -f docker-compose.proxied.yml up -d --build
```
The relay is then reachable at `127.0.0.1:${RELAY_HOST_PORT:-8443}`. Point your existing proxy at it:

**nginx** (add a server block for the relay subdomain, then `nginx -t && systemctl reload nginx`; TLS via your usual certbot):
```nginx
server {
    listen 443 ssl;
    server_name relay.example.com;
    ssl_certificate     /etc/letsencrypt/live/relay.example.com/fullchain.pem;
    ssl_certificate_key /etc/letsencrypt/live/relay.example.com/privkey.pem;
    location / {
        proxy_pass http://127.0.0.1:8443;
        proxy_http_version 1.1;
        proxy_set_header Upgrade $http_upgrade;      # WebSocket (wss://…/ws/*)
        proxy_set_header Connection "upgrade";
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_read_timeout 3600s;                    # long-lived signaling WS
    }
}
```
**Caddy** (existing Caddyfile — WebSockets need no extra config):
```
relay.example.com {
    reverse_proxy 127.0.0.1:8443
}
```
coturn in proxied mode serves **plain TURN** (3478 udp/tcp) — enough for NAT traversal. For `turns:5349`, mount your proxy's existing Let's Encrypt cert dir into coturn and point the entrypoint at it via `COTURN_CERT_DIR` (the dir must contain `fullchain.pem` + `privkey.pem`, which certbot's `live/<domain>/` already does):
```yaml
# add under services: coturn: in docker-compose.proxied.yml
    environment:
      COTURN_CERT_DIR: /certs
    volumes:
      - /etc/letsencrypt/live/relay.example.com:/certs:ro
# then: docker compose ... restart coturn   → log shows "TLS enabled"
```
Still open the coturn ports at the firewall: `3478 (tcp+udp), 5349 (tcp), 49160-49200 (udp)`.

## Verify
```bash
curl -s https://relay.example.com/health                    # -> ok
curl -s https://relay.example.com/ice-servers-public | jq   # STUN + turn:<domain>:3478
docker compose ps
docker compose logs -f coturn                               # look for "TLS enabled"
```

## What talks to what
| Service   | Ports (host)                     | TLS                          | Exposed? |
|-----------|----------------------------------|------------------------------|----------|
| caddy     | 80, 443 (tcp+udp)                | auto Let's Encrypt           | yes      |
| relay     | 8443 (internal only)             | none (Caddy terminates)      | no       |
| postgres  | 5432 (internal only)             | —                            | no       |
| coturn    | 3478 tcp+udp, 5349 tls, 49160-49200 udp | shares Caddy's cert   | yes      |

The relay advertises `turn:<domain>:3478?transport=udp|tcp` (see `internal/turn/handler.go`), so **3478 + a matching `TURN_SECRET` is the load-bearing path**; TLS-TURN on 5349 is a bonus (not yet advertised by the relay).

## Point the apps here
- Host + Android client relay URL → `https://relay.example.com` (WebSocket `wss://relay.example.com`).
- Android default is hardcoded in `RemotePlay/.../core/network/relay/RelayModule.kt` — update it (or ship via QR pairing config).

## Operations
```bash
docker compose logs -f relay          # or caddy / coturn / postgres
docker compose up -d --build          # after pulling new relay code
docker compose down                   # stop (volumes/certs persist)
docker compose restart coturn         # after a cert renewal (weekly cron does this)
```
- **Backups:** the `pg_data` volume (users/devices/rooms) and `caddy_data` (certs).
- **Renewal:** Caddy auto-renews; the `setup-vps.sh` cron restarts coturn weekly so it re-reads the renewed cert. No downtime for the relay.

## Security notes
- Secrets live only in `.env` (git-ignored) and container env — never committed. `TURN_SECRET` is shared by relay + coturn by design.
- coturn hardening in `coturn/turnserver.conf`: per-session cap (`max-bps`, never unlimited), and `denied-peer-ip` blocks RFC1918 + loopback + link-local **incl. `169.254.0.0/16` (cloud-metadata SSRF)** + CGNAT.
- Known trade-offs (documented, not blockers): coturn runs as root under host networking (typical for a TURN box; can add `proc-user`/`proc-group` to drop privileges). `turns:443` (for UDP-only-blocked networks) is not served — needs the relay to emit `turns:` URLs (tracked in the P2P plan).
- Postgres and the relay are never published to the internet; only Caddy and coturn are reachable.
