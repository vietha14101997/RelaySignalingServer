package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
	"github.com/reka/relay-server/internal/auth"
	"github.com/reka/relay-server/internal/config"
	"github.com/reka/relay-server/internal/database"
	"github.com/reka/relay-server/internal/device"
	"github.com/reka/relay-server/internal/health"
	"github.com/reka/relay-server/internal/guest"
	"github.com/reka/relay-server/internal/ratelimit"
	"github.com/reka/relay-server/internal/relay"
	"github.com/reka/relay-server/internal/room"
	"github.com/reka/relay-server/internal/session"
	"github.com/reka/relay-server/internal/turn"
)

func main() {
	cfg := config.Load()

	// Database (optional — skip in dev mode)
	var pool *pgxpool.Pool
	if cfg.DatabaseURL != "" {
		ctx := context.Background()
		var err error
		pool, err = database.Connect(ctx, cfg.DatabaseURL)
		if err != nil {
			log.Fatalf("Failed to connect to database: %v", err)
		}
		defer pool.Close()

		if err := database.RunMigrations(ctx, pool); err != nil {
			log.Fatalf("Failed to run migrations: %v", err)
		}
	} else {
		log.Println("[DEV MODE] No DATABASE_URL set — running without database (auth/device APIs disabled)")
	}

	// Services
	hub := relay.NewHub()
	deviceHub := relay.NewDeviceHub()
	jwtService := auth.NewJWTService(cfg.JWTSecret, cfg.AccessTokenTTL)
	wsHandler := relay.NewWSHandler(deviceHub, cfg.MaxMessageSize)
	legacyWSHandler := relay.NewHandler(hub, cfg.MaxMessageSize)
	turnHandler := turn.NewHandler(cfg)
	healthHandler := health.NewHandler(hub, deviceHub)

	e := echo.New()
	e.HideBanner = true

	// Global middleware
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	// Public routes
	e.GET("/health", healthHandler.HealthCheck)

	// DB-dependent routes (auth, devices, sessions)
	if pool != nil {
		authRepo := auth.NewRepository(pool)
		deviceRepo := device.NewRepository(pool)
		authHandler := auth.NewHandler(authRepo, jwtService, cfg.RefreshTokenTTL)
		deviceHandler := device.NewHandler(deviceRepo, deviceHub)
		sessionHandler := session.NewHandler(deviceHub)

		e.POST("/auth/register", authHandler.Register)
		e.POST("/auth/login", authHandler.Login)
		e.POST("/auth/refresh", authHandler.Refresh)
		e.POST("/auth/logout", authHandler.Logout)

		protected := e.Group("")
		protected.Use(auth.JWTMiddleware(jwtService))
		protected.POST("/devices/register", deviceHandler.Register)
		protected.GET("/devices", deviceHandler.List)
		protected.DELETE("/devices/:id", deviceHandler.Delete)
		protected.POST("/sessions/create", sessionHandler.Create)
		protected.GET("/ice-servers", turnHandler.GetIceServers)
		protected.GET("/ws/server", wsHandler.HandleServerWS)
		protected.GET("/ws/client", wsHandler.HandleClientWS)
	}

	// Guest access (no JWT needed for session/ws, but register needs JWT)
	guestHandler := guest.NewHandler(deviceHub, cfg.MaxMessageSize)
	guestLimiter := ratelimit.New(5, 5*time.Minute)

	if pool != nil {
		// Guest register needs JWT (server must be logged in)
		guestProtected := e.Group("")
		guestProtected.Use(auth.JWTMiddleware(jwtService))
		guestProtected.POST("/guest/register", guestHandler.Register)
	}
	e.POST("/sessions/guest", guestHandler.CreateSession, ratelimit.Middleware(guestLimiter))
	e.GET("/ws/guest", guestHandler.HandleGuestWS)
	e.GET("/ice-servers-public", turnHandler.GetIceServersPublic) // no JWT needed for guest mode

	// Room endpoints
	roomHandler := room.NewHandler(deviceHub, cfg.MaxMessageSize)
	roomLimiter := ratelimit.New(5, 5*time.Minute)
	if pool != nil {
		roomProtected := e.Group("")
		roomProtected.Use(auth.JWTMiddleware(jwtService))
		roomProtected.POST("/rooms/create", roomHandler.Create)
		roomProtected.POST("/rooms/set-password", roomHandler.SetPassword)
	}
	e.POST("/rooms/join", roomHandler.Join, ratelimit.Middleware(roomLimiter))
	e.GET("/rooms/:id/info", roomHandler.GetInfo)
	e.GET("/ws/room", roomHandler.HandleRoomWS)

	// Always available: Phase 1 simple relay (no auth needed in dev mode)
	e.GET("/ws", legacyWSHandler.HandleWebSocket)

	// Cleanup goroutines
	go func() {
		ticker := time.NewTicker(60 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			hub.CleanupStaleRooms(time.Duration(cfg.RoomIdleTimeout) * time.Second)
			deviceHub.CleanupStaleSessions(5 * time.Minute)
			guestLimiter.Cleanup()
		}
	}()

	// Start server
	addr := fmt.Sprintf(":%d", cfg.Port)
	go func() {
		if cfg.HasTLS() {
			log.Printf("Starting relay server on %s (TLS)", addr)
			if err := e.StartTLS(addr, cfg.TLSCertFile, cfg.TLSKeyFile); err != nil {
				log.Printf("Server stopped: %v", err)
			}
		} else {
			log.Printf("Starting relay server on %s (no TLS - dev mode)", addr)
			if err := e.Start(addr); err != nil {
				log.Printf("Server stopped: %v", err)
			}
		}
	}()

	// Graceful shutdown
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down relay server...")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := e.Shutdown(shutdownCtx); err != nil {
		log.Fatalf("Shutdown error: %v", err)
	}
	log.Println("Server stopped gracefully")
}
