package auth

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrUserNotFound  = errors.New("user not found")
	ErrTokenNotFound = errors.New("refresh token not found")
	ErrTokenRevoked  = errors.New("refresh token revoked")
	ErrTokenExpired  = errors.New("refresh token expired")
	ErrDuplicateUser = errors.New("email or username already exists")
)

type User struct {
	ID           string    `json:"id"`
	Email        string    `json:"email"`
	Username     string    `json:"username"`
	PasswordHash string    `json:"-"`
	CreatedAt    time.Time `json:"created_at"`
}

type RefreshToken struct {
	ID         string
	UserID     string
	TokenHash  string
	DeviceName string
	ExpiresAt  time.Time
	Revoked    bool
}

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func HashToken(token string) string {
	h := sha256.Sum256([]byte(token))
	return hex.EncodeToString(h[:])
}

func (r *Repository) CreateUser(ctx context.Context, email, username, passwordHash string) (*User, error) {
	user := &User{}
	err := r.pool.QueryRow(ctx,
		`INSERT INTO users (email, username, password_hash)
		 VALUES ($1, $2, $3)
		 RETURNING id, email, username, created_at`,
		email, username, passwordHash,
	).Scan(&user.ID, &user.Email, &user.Username, &user.CreatedAt)

	if err != nil {
		if isDuplicateError(err) {
			return nil, ErrDuplicateUser
		}
		return nil, err
	}
	return user, nil
}

func (r *Repository) FindUserByEmail(ctx context.Context, email string) (*User, error) {
	user := &User{}
	err := r.pool.QueryRow(ctx,
		`SELECT id, email, username, password_hash, created_at FROM users WHERE email = $1`,
		email,
	).Scan(&user.ID, &user.Email, &user.Username, &user.PasswordHash, &user.CreatedAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrUserNotFound
	}
	return user, err
}

func (r *Repository) StoreRefreshToken(ctx context.Context, userID, tokenHash, deviceName string, expiresAt time.Time) error {
	_, err := r.pool.Exec(ctx,
		`INSERT INTO refresh_tokens (user_id, token_hash, device_name, expires_at)
		 VALUES ($1, $2, $3, $4)`,
		userID, tokenHash, deviceName, expiresAt,
	)
	return err
}

func (r *Repository) FindRefreshToken(ctx context.Context, tokenHash string) (*RefreshToken, error) {
	rt := &RefreshToken{}
	err := r.pool.QueryRow(ctx,
		`SELECT id, user_id, token_hash, COALESCE(device_name, ''), expires_at, revoked
		 FROM refresh_tokens WHERE token_hash = $1`,
		tokenHash,
	).Scan(&rt.ID, &rt.UserID, &rt.TokenHash, &rt.DeviceName, &rt.ExpiresAt, &rt.Revoked)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrTokenNotFound
	}
	return rt, err
}

func (r *Repository) RevokeRefreshToken(ctx context.Context, tokenHash string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE refresh_tokens SET revoked = TRUE WHERE token_hash = $1`,
		tokenHash,
	)
	return err
}

func (r *Repository) RevokeAllUserTokens(ctx context.Context, userID string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE refresh_tokens SET revoked = TRUE WHERE user_id = $1`,
		userID,
	)
	return err
}

func isDuplicateError(err error) bool {
	return err != nil && (contains(err.Error(), "duplicate key") || contains(err.Error(), "23505"))
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && searchString(s, substr)
}

func searchString(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
