package auth

import (
	"net/http"
	"net/mail"
	"time"

	"github.com/labstack/echo/v4"
	"golang.org/x/crypto/bcrypt"
)

type Handler struct {
	repo       *Repository
	jwtService *JWTService
	refreshTTL time.Duration
}

func NewHandler(repo *Repository, jwtService *JWTService, refreshTTL time.Duration) *Handler {
	return &Handler{
		repo:       repo,
		jwtService: jwtService,
		refreshTTL: refreshTTL,
	}
}

type registerRequest struct {
	Email    string `json:"email"`
	Username string `json:"username"`
	Password string `json:"password"`
}

type loginRequest struct {
	Email      string `json:"email"`
	Password   string `json:"password"`
	DeviceName string `json:"device_name"`
}

type refreshRequest struct {
	RefreshToken string `json:"refresh_token"`
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
}

func (h *Handler) Register(c echo.Context) error {
	var req registerRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid request body"})
	}

	if _, err := mail.ParseAddress(req.Email); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid email format"})
	}
	if len(req.Password) < 8 {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "password must be at least 8 characters"})
	}
	if len(req.Username) < 3 || len(req.Username) > 50 {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "username must be 3-50 characters"})
	}

	hash, err := bcrypt.GenerateFromPassword([]byte(req.Password), 12)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "internal error"})
	}

	user, err := h.repo.CreateUser(c.Request().Context(), req.Email, req.Username, string(hash))
	if err == ErrDuplicateUser {
		return c.JSON(http.StatusConflict, map[string]string{"error": "email or username already exists"})
	}
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "internal error"})
	}

	return c.JSON(http.StatusCreated, map[string]string{
		"user_id": user.ID,
		"message": "registered",
	})
}

func (h *Handler) Login(c echo.Context) error {
	var req loginRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid request body"})
	}

	user, err := h.repo.FindUserByEmail(c.Request().Context(), req.Email)
	if err == ErrUserNotFound {
		return c.JSON(http.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
	}
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "internal error"})
	}

	if err := bcrypt.CompareHashAndPassword([]byte(user.PasswordHash), []byte(req.Password)); err != nil {
		return c.JSON(http.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
	}

	return h.issueTokens(c, user.ID, req.DeviceName)
}

func (h *Handler) Refresh(c echo.Context) error {
	var req refreshRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid request body"})
	}

	tokenHash := HashToken(req.RefreshToken)
	rt, err := h.repo.FindRefreshToken(c.Request().Context(), tokenHash)
	if err == ErrTokenNotFound {
		return c.JSON(http.StatusUnauthorized, map[string]string{"error": "invalid refresh token"})
	}
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "internal error"})
	}

	if rt.Revoked {
		return c.JSON(http.StatusUnauthorized, map[string]string{"error": "token revoked"})
	}
	if time.Now().After(rt.ExpiresAt) {
		return c.JSON(http.StatusUnauthorized, map[string]string{"error": "token expired"})
	}

	// Rotate: revoke old token
	h.repo.RevokeRefreshToken(c.Request().Context(), tokenHash)

	return h.issueTokens(c, rt.UserID, rt.DeviceName)
}

func (h *Handler) Logout(c echo.Context) error {
	var req refreshRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "invalid request body"})
	}

	tokenHash := HashToken(req.RefreshToken)
	h.repo.RevokeRefreshToken(c.Request().Context(), tokenHash)

	return c.JSON(http.StatusOK, map[string]string{"message": "logged out"})
}

func (h *Handler) issueTokens(c echo.Context, userID, deviceName string) error {
	accessToken, err := h.jwtService.GenerateAccessToken(userID)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "token generation failed"})
	}

	refreshToken, err := GenerateRefreshToken()
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "token generation failed"})
	}

	tokenHash := HashToken(refreshToken)
	expiresAt := time.Now().Add(h.refreshTTL)

	if err := h.repo.StoreRefreshToken(c.Request().Context(), userID, tokenHash, deviceName, expiresAt); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "internal error"})
	}

	return c.JSON(http.StatusOK, tokenResponse{
		AccessToken:  accessToken,
		RefreshToken: refreshToken,
		ExpiresIn:    int(h.jwtService.accessTTL.Seconds()),
	})
}
