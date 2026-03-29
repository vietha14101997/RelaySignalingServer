package device

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

var (
	ErrDeviceNotFound = errors.New("device not found")
	ErrNotOwner       = errors.New("not device owner")
)

type Device struct {
	ID           string          `json:"id"`
	UserID       string          `json:"user_id"`
	DeviceName   string          `json:"device_name"`
	DeviceType   string          `json:"device_type"`
	HWInfo       json.RawMessage `json:"hw_info,omitempty"`
	RegisteredAt time.Time       `json:"registered_at"`
	LastSeenAt   time.Time       `json:"last_seen_at"`
	Online       bool            `json:"online"`                  // populated from in-memory state
	RoomID       string          `json:"room_id,omitempty"`       // populated from DeviceHub
	DisplayID    string          `json:"display_id,omitempty"`    // formatted room_id (e.g. "ABC-DEF")
}

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) RegisterDevice(ctx context.Context, userID, name, deviceType string, hwInfo json.RawMessage) (*Device, error) {
	device := &Device{}
	var hw []byte

	if len(hwInfo) > 0 {
		hw = hwInfo
	}

	err := r.pool.QueryRow(ctx,
		`INSERT INTO devices (user_id, device_name, device_type, hw_info)
		 VALUES ($1, $2, $3, $4)
		 RETURNING id, user_id, device_name, device_type, hw_info, registered_at, last_seen_at`,
		userID, name, deviceType, hw,
	).Scan(&device.ID, &device.UserID, &device.DeviceName, &device.DeviceType,
		&device.HWInfo, &device.RegisteredAt, &device.LastSeenAt)

	return device, err
}

func (r *Repository) ListDevices(ctx context.Context, userID string) ([]Device, error) {
	rows, err := r.pool.Query(ctx,
		`SELECT id, user_id, device_name, device_type, hw_info, registered_at, last_seen_at
		 FROM devices WHERE user_id = $1 ORDER BY registered_at DESC`,
		userID,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var devices []Device
	for rows.Next() {
		var d Device
		if err := rows.Scan(&d.ID, &d.UserID, &d.DeviceName, &d.DeviceType,
			&d.HWInfo, &d.RegisteredAt, &d.LastSeenAt); err != nil {
			return nil, err
		}
		devices = append(devices, d)
	}
	return devices, nil
}

func (r *Repository) FindDevice(ctx context.Context, deviceID string) (*Device, error) {
	d := &Device{}
	err := r.pool.QueryRow(ctx,
		`SELECT id, user_id, device_name, device_type, hw_info, registered_at, last_seen_at
		 FROM devices WHERE id = $1`,
		deviceID,
	).Scan(&d.ID, &d.UserID, &d.DeviceName, &d.DeviceType,
		&d.HWInfo, &d.RegisteredAt, &d.LastSeenAt)

	if errors.Is(err, pgx.ErrNoRows) {
		return nil, ErrDeviceNotFound
	}
	return d, err
}

func (r *Repository) UpdateLastSeen(ctx context.Context, deviceID string) error {
	_, err := r.pool.Exec(ctx,
		`UPDATE devices SET last_seen_at = NOW() WHERE id = $1`,
		deviceID,
	)
	return err
}

func (r *Repository) DeleteDevice(ctx context.Context, deviceID, userID string) error {
	result, err := r.pool.Exec(ctx,
		`DELETE FROM devices WHERE id = $1 AND user_id = $2`,
		deviceID, userID,
	)
	if err != nil {
		return err
	}
	if result.RowsAffected() == 0 {
		return ErrNotOwner
	}
	return nil
}
