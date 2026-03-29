package turn

import (
	"crypto/hmac"
	"crypto/sha1"
	"encoding/base64"
	"fmt"
	"time"
)

// GenerateCredentials creates time-limited TURN credentials per RFC 5766.
// coturn validates these using its static-auth-secret (use-auth-secret mode).
// Username format: "expiry_timestamp:userID"
// Password: HMAC-SHA1(username, sharedSecret) base64-encoded
func GenerateCredentials(userID, sharedSecret string, ttl time.Duration) (username, password string) {
	expiry := time.Now().Add(ttl).Unix()
	username = fmt.Sprintf("%d:%s", expiry, userID)

	mac := hmac.New(sha1.New, []byte(sharedSecret))
	mac.Write([]byte(username))
	password = base64.StdEncoding.EncodeToString(mac.Sum(nil))

	return username, password
}
