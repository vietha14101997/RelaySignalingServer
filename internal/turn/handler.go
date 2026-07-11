package turn

import (
	"fmt"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/reka/relay-server/internal/auth"
	"github.com/reka/relay-server/internal/config"
)

type IceServer struct {
	URLs       []string `json:"urls"`
	Username   string   `json:"username,omitempty"`
	Credential string   `json:"credential,omitempty"`
}

type IceServersResponse struct {
	IceServers []IceServer `json:"ice_servers"`
	TTL        int         `json:"ttl"`
}

type Handler struct {
	cfg *config.Config
}

func NewHandler(cfg *config.Config) *Handler {
	return &Handler{cfg: cfg}
}

func (h *Handler) GetIceServersPublic(c echo.Context) error {
	return h.getIceServersForUser(c, "guest")
}

func (h *Handler) GetIceServers(c echo.Context) error {
	userID := c.Get(auth.ContextUserID).(string)
	return h.getIceServersForUser(c, userID)
}

func (h *Handler) getIceServersForUser(c echo.Context, userID string) error {
	servers := []IceServer{
		// STUN — always included, used for P2P (priority)
		{URLs: []string{
			"stun:stun.l.google.com:19302",
			"stun:stun1.l.google.com:19302",
		}},
	}

	// TURN — fallback only when P2P fails (Symmetric NAT, 4G, corporate firewall)
	// WebRTC ICE agent automatically prefers P2P (srflx) over TURN (relay)
	if h.cfg.TURNSecret != "" && h.cfg.TURNDomain != "" {
		username, credential := GenerateCredentials(userID, h.cfg.TURNSecret, h.cfg.TURNCredTTL)

		servers = append(servers, IceServer{
			URLs: []string{
				fmt.Sprintf("turn:%s:%d?transport=udp", h.cfg.TURNDomain, h.cfg.STUNPort),
				fmt.Sprintf("turn:%s:%d?transport=tcp", h.cfg.TURNDomain, h.cfg.STUNPort),
			},
			Username:   username,
			Credential: credential,
		})
	}

	return c.JSON(http.StatusOK, IceServersResponse{
		IceServers: servers,
		TTL:        int(h.cfg.TURNCredTTL / time.Second),
	})
}
