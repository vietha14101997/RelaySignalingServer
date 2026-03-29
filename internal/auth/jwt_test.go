package auth

import (
	"testing"
	"time"
)

func TestGenerateAndValidateAccessToken(t *testing.T) {
	svc := NewJWTService("test-secret-key", 15*time.Minute)

	token, err := svc.GenerateAccessToken("user-123")
	if err != nil {
		t.Fatalf("GenerateAccessToken failed: %v", err)
	}
	if token == "" {
		t.Fatal("Token should not be empty")
	}

	claims, err := svc.ValidateToken(token)
	if err != nil {
		t.Fatalf("ValidateToken failed: %v", err)
	}

	if claims.UserID != "user-123" {
		t.Errorf("Expected user_id=user-123, got %s", claims.UserID)
	}
	if claims.Subject != "user-123" {
		t.Errorf("Expected sub=user-123, got %s", claims.Subject)
	}
	if claims.Issuer != "relay-server" {
		t.Errorf("Expected issuer=relay-server, got %s", claims.Issuer)
	}
}

func TestExpiredToken(t *testing.T) {
	svc := NewJWTService("test-secret-key", -1*time.Second) // already expired

	token, err := svc.GenerateAccessToken("user-456")
	if err != nil {
		t.Fatalf("GenerateAccessToken failed: %v", err)
	}

	_, err = svc.ValidateToken(token)
	if err == nil {
		t.Error("Expected error for expired token, got nil")
	}
}

func TestInvalidSecret(t *testing.T) {
	svc1 := NewJWTService("secret-1", 15*time.Minute)
	svc2 := NewJWTService("secret-2", 15*time.Minute)

	token, _ := svc1.GenerateAccessToken("user-789")

	_, err := svc2.ValidateToken(token)
	if err == nil {
		t.Error("Expected error for wrong secret, got nil")
	}
}

func TestInvalidTokenString(t *testing.T) {
	svc := NewJWTService("test-secret", 15*time.Minute)

	_, err := svc.ValidateToken("not.a.valid.jwt.token")
	if err == nil {
		t.Error("Expected error for invalid token string, got nil")
	}
}

func TestGenerateRefreshToken(t *testing.T) {
	token1, err := GenerateRefreshToken()
	if err != nil {
		t.Fatalf("GenerateRefreshToken failed: %v", err)
	}
	if len(token1) != 64 { // 32 bytes = 64 hex chars
		t.Errorf("Expected 64-char hex string, got %d chars", len(token1))
	}

	token2, _ := GenerateRefreshToken()
	if token1 == token2 {
		t.Error("Two refresh tokens should not be equal")
	}
}

func TestHashToken(t *testing.T) {
	hash1 := HashToken("test-token")
	hash2 := HashToken("test-token")
	hash3 := HashToken("different-token")

	if hash1 != hash2 {
		t.Error("Same input should produce same hash")
	}
	if hash1 == hash3 {
		t.Error("Different input should produce different hash")
	}
	if len(hash1) != 64 { // SHA256 = 64 hex chars
		t.Errorf("Expected 64-char hash, got %d chars", len(hash1))
	}
}
