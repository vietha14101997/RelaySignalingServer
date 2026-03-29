#!/bin/bash
echo "=== 1. coturn status ==="
systemctl is-active coturn

echo ""
echo "=== 2. coturn listening ==="
ss -ulnp | grep turnserver | head -5
ss -tlnp | grep turnserver | head -5

echo ""
echo "=== 3. coturn config ==="
grep -E "^(listening-ip|relay-ip|external-ip|static-auth-secret|use-auth-secret|listening-port|min-port|max-port)" /etc/turnserver.conf

echo ""
echo "=== 4. relay server TURN_SECRET ==="
grep TURN_SECRET /opt/relay/.env

echo ""
echo "=== 5. secrets match? ==="
COTURN_SECRET=$(grep static-auth-secret /etc/turnserver.conf | cut -d= -f2)
RELAY_SECRET=$(grep TURN_SECRET /opt/relay/.env | cut -d= -f2)
if [ "$COTURN_SECRET" = "$RELAY_SECRET" ]; then
    echo "✅ MATCH"
else
    echo "❌ MISMATCH!"
    echo "  coturn: $COTURN_SECRET"
    echo "  relay:  $RELAY_SECRET"
fi

echo ""
echo "=== 6. GCP firewall (iptables) ==="
iptables -L INPUT -n | grep -E "3478|49152"

echo ""
echo "=== 7. test TURN allocation locally ==="
which turnutils_uclient >/dev/null 2>&1 && {
    timeout 5 turnutils_uclient -t -e 127.0.0.1 -p 3478 -u test -w test 2>&1 | head -5
} || echo "turnutils_uclient not installed"

echo ""
echo "=== 8. external IP check ==="
curl -s http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/access-configs/0/external-ip -H "Metadata-Flavor: Google"
echo ""
