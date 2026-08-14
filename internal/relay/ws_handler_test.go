package relay

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v4"
	"github.com/reka/relay-server/internal/auth"
)

type stubDeviceOwnerChecker struct {
	owned bool
	err   error
}

func (s stubDeviceOwnerChecker) IsOwner(context.Context, string, string) (bool, error) {
	return s.owned, s.err
}

func TestHandleServerWSRejectsPersistentNonOwnerWithoutHubState(t *testing.T) {
	hub := NewDeviceHub()
	handler := NewWSHandler(hub, stubDeviceOwnerChecker{owned: false}, 1024)
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/ws/server?device_id=offline-device", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.Set(auth.ContextUserID, "attacker")

	if err := handler.HandleServerWS(c); err != nil {
		t.Fatalf("HandleServerWS returned error: %v", err)
	}
	if rec.Code != http.StatusForbidden {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusForbidden)
	}
	if hub.IsOnline("offline-device") {
		t.Fatal("non-owner connection reached AddServer")
	}
}
