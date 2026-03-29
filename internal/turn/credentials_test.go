package turn

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestGenerateCredentials(t *testing.T) {
	secret := "test-shared-secret"
	userID := "user-123"
	ttl := 24 * time.Hour

	username, password := GenerateCredentials(userID, secret, ttl)

	// Username should be "timestamp:userID"
	parts := strings.SplitN(username, ":", 2)
	if len(parts) != 2 {
		t.Fatalf("Expected username format 'timestamp:userID', got %s", username)
	}

	ts, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		t.Fatalf("Failed to parse timestamp: %v", err)
	}

	// Timestamp should be ~24h from now
	expected := time.Now().Add(ttl).Unix()
	if ts < expected-2 || ts > expected+2 {
		t.Errorf("Timestamp %d not within 2s of expected %d", ts, expected)
	}

	if parts[1] != userID {
		t.Errorf("Expected userID=%s, got %s", userID, parts[1])
	}

	// Password should be valid HMAC-SHA1
	mac := hmac.New(sha1.New, []byte(secret))
	mac.Write([]byte(username))
	expectedPassword := base64.StdEncoding.EncodeToString(mac.Sum(nil))

	if password != expectedPassword {
		t.Errorf("Password mismatch: expected %s, got %s", expectedPassword, password)
	}
}

func TestCredentialsUniqueness(t *testing.T) {
	secret := "test-secret"

	u1, p1 := GenerateCredentials("user-1", secret, time.Hour)
	u2, p2 := GenerateCredentials("user-2", secret, time.Hour)

	if u1 == u2 {
		t.Error("Usernames for different users should differ")
	}
	if p1 == p2 {
		t.Error("Passwords for different users should differ")
	}
}

func TestCredentialsDifferentSecrets(t *testing.T) {
	_, p1 := GenerateCredentials("user-1", "secret-a", time.Hour)
	_, p2 := GenerateCredentials("user-1", "secret-b", time.Hour)

	if p1 == p2 {
		t.Error("Passwords with different secrets should differ")
	}
}
