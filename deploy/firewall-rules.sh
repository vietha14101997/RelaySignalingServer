#!/bin/bash
# Run on VPS: sudo bash firewall-rules.sh
# Opens required ports via iptables (works immediately, no GCP console needed)

set -e

echo "Opening relay server port 8443..."
sudo iptables -I INPUT -p tcp --dport 8443 -j ACCEPT

echo "Opening STUN/TURN ports..."
sudo iptables -I INPUT -p udp --dport 3478 -j ACCEPT
sudo iptables -I INPUT -p tcp --dport 3478 -j ACCEPT
sudo iptables -I INPUT -p tcp --dport 5349 -j ACCEPT
sudo iptables -I INPUT -p udp --dport 49152:65535 -j ACCEPT

echo "Saving iptables rules..."
sudo apt install -y iptables-persistent 2>/dev/null || true
sudo netfilter-persistent save 2>/dev/null || sudo iptables-save > /etc/iptables.rules

echo ""
echo "Done! Ports opened:"
echo "  TCP 8443        - Relay signaling"
echo "  UDP/TCP 3478    - STUN/TURN"
echo "  TCP 5349        - TURNS (TLS)"
echo "  UDP 49152-65535 - TURN relay range"
echo ""
echo "Test: curl http://$(curl -s ifconfig.me):8443/health"
