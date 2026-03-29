#!/bin/bash
# Setup script for coturn on Ubuntu 22.04+
# Run as root: sudo bash setup-coturn.sh

set -e

echo "=== Installing coturn ==="
apt update && apt install -y coturn

echo "=== Enabling coturn service ==="
sed -i 's/#TURNSERVER_ENABLED=1/TURNSERVER_ENABLED=1/' /etc/default/coturn

echo "=== Generating shared secret ==="
TURN_SECRET=$(openssl rand -hex 32)
echo "TURN_SECRET=$TURN_SECRET"
echo ""
echo ">>> IMPORTANT: Set this as TURN_SECRET env var in your Go relay server <<<"
echo ">>> And update static-auth-secret in /etc/turnserver.conf             <<<"
echo ""

echo "=== Copying config ==="
cp turnserver.conf /etc/turnserver.conf
sed -i "s/CHANGE_ME_GENERATE_WITH_openssl_rand_hex_32/$TURN_SECRET/" /etc/turnserver.conf

echo "=== Opening firewall ports ==="
ufw allow 3478/udp   # STUN/TURN UDP
ufw allow 3478/tcp   # TURN TCP
ufw allow 5349/tcp   # TURNS (TLS)
ufw allow 49152:65535/udp  # Relay port range

echo "=== Starting coturn ==="
systemctl enable coturn
systemctl restart coturn
systemctl status coturn

echo ""
echo "=== Setup complete ==="
echo "TURN_SECRET=$TURN_SECRET"
echo ""
echo "Next steps:"
echo "  1. Update external-ip in /etc/turnserver.conf with your VPS public IP"
echo "  2. Update realm to your domain"
echo "  3. Uncomment and set TLS cert/key paths if using TURNS"
echo "  4. Set TURN_SECRET and TURN_DOMAIN env vars for Go relay server"
echo "  5. Restart coturn: systemctl restart coturn"
echo "  6. Test: turnutils_uclient -t -u test -w test relay.remoteplay.app"
