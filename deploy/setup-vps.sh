#!/bin/bash
# One-click VPS setup for RelaySignalingServer
# Run: sudo bash setup-vps.sh
set -e

echo "========================================="
echo "  RelaySignalingServer VPS Setup"
echo "========================================="

# 1. Install PostgreSQL
echo "[1/6] Installing PostgreSQL..."
apt update -y && apt install -y postgresql postgresql-contrib

# Start PostgreSQL
systemctl enable postgresql
systemctl start postgresql

# Create database and user
sudo -u postgres psql -c "CREATE USER relay WITH PASSWORD 'relay_db_pass_2026';" 2>/dev/null || true
sudo -u postgres psql -c "CREATE DATABASE relay OWNER relay;" 2>/dev/null || true
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE relay TO relay;" 2>/dev/null || true
echo "[1/6] PostgreSQL ready"

# 2. Setup relay server directory
echo "[2/6] Setting up relay server..."
mkdir -p /opt/relay
cp relay-server /opt/relay/relay-server 2>/dev/null || echo "  (copy relay-server binary to /opt/relay/ manually)"
chmod +x /opt/relay/relay-server

# 3. Generate secrets
JWT_SECRET=$(openssl rand -hex 32)
TURN_SECRET=$(openssl rand -hex 32)

# Get external IP
EXTERNAL_IP=$(curl -s http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip -H "Metadata-Flavor: Google" 2>/dev/null || curl -s ifconfig.me)

echo "[3/6] External IP: $EXTERNAL_IP"

# 4. Create .env file
cat > /opt/relay/.env << EOF
RELAY_PORT=8443
DATABASE_URL=postgres://relay:relay_db_pass_2026@localhost:5432/relay?sslmode=disable
JWT_SECRET=${JWT_SECRET}
TURN_SECRET=${TURN_SECRET}
TURN_DOMAIN=${EXTERNAL_IP}
EOF

echo "[4/6] Config written to /opt/relay/.env"

# 5. Create systemd service
cat > /etc/systemd/system/relay-server.service << 'EOF'
[Unit]
Description=RemotePlay Relay Signaling Server
After=network.target postgresql.service

[Service]
Type=simple
User=root
WorkingDirectory=/opt/relay
EnvironmentFile=/opt/relay/.env
ExecStart=/opt/relay/relay-server
Restart=on-failure
RestartSec=5
LimitNOFILE=65535

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable relay-server

echo "[5/6] Systemd service created"

# 6. Install coturn
echo "[6/6] Installing coturn..."
apt install -y coturn

# Enable coturn
sed -i 's/#TURNSERVER_ENABLED=1/TURNSERVER_ENABLED=1/' /etc/default/coturn

cat > /etc/turnserver.conf << TURNEOF
listening-port=3478
alt-listening-port=3479
external-ip=${EXTERNAL_IP}
min-port=49152
max-port=65535
use-auth-secret
static-auth-secret=${TURN_SECRET}
realm=relay.remoteplay.app
no-multicast-peers
denied-peer-ip=10.0.0.0-10.255.255.255
denied-peer-ip=172.16.0.0-172.31.255.255
denied-peer-ip=192.168.0.0-192.168.255.255
denied-peer-ip=127.0.0.0-127.255.255.255
bps-capacity=5000000
max-bps=5000000
log-file=/var/log/turnserver.log
TURNEOF

systemctl enable coturn
systemctl restart coturn

echo ""
echo "========================================="
echo "  SETUP COMPLETE"
echo "========================================="
echo ""
echo "  External IP:  $EXTERNAL_IP"
echo "  Relay Port:   8443"
echo "  JWT Secret:   $JWT_SECRET"
echo "  TURN Secret:  $TURN_SECRET"
echo ""
echo "  Next steps:"
echo "  1. Upload relay-server binary:"
echo "     gcloud compute scp relay-server relay-signaling-server:/opt/relay/"
echo ""
echo "  2. Start the server:"
echo "     sudo systemctl start relay-server"
echo ""
echo "  3. Check status:"
echo "     sudo systemctl status relay-server"
echo "     curl http://localhost:8443/health"
echo ""
echo "  4. Test from outside:"
echo "     curl http://$EXTERNAL_IP:8443/health"
echo "========================================="
