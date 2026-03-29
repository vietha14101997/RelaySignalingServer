package auth

import (
	"net/http"
	"strings"

	"github.com/labstack/echo/v4"
)

const ContextUserID = "user_id"

// JWTMiddleware validates JWT from Authorization header or ?token= query param.
func JWTMiddleware(jwtService *JWTService) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) error {
			tokenString := extractToken(c)
			if tokenString == "" {
				return c.JSON(http.StatusUnauthorized, map[string]string{"error": "missing authentication token"})
			}

			claims, err := jwtService.ValidateToken(tokenString)
			if err != nil {
				return c.JSON(http.StatusUnauthorized, map[string]string{"error": "invalid or expired token"})
			}

			c.Set(ContextUserID, claims.UserID)
			return next(c)
		}
	}
}

func extractToken(c echo.Context) string {
	// 1. Check Authorization header
	auth := c.Request().Header.Get("Authorization")
	if strings.HasPrefix(auth, "Bearer ") {
		return strings.TrimPrefix(auth, "Bearer ")
	}

	// 2. Check query param (for WebSocket connections)
	if token := c.QueryParam("token"); token != "" {
		return token
	}

	return ""
}
