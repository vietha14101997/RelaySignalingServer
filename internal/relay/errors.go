package relay

import "errors"

var (
	ErrServerOffline   = errors.New("server is offline")
	ErrServerBusy      = errors.New("server already has an active session")
	ErrNotOwner        = errors.New("device does not belong to user")
	ErrGuestNotFound   = errors.New("guest device not found")
	ErrInvalidPassword = errors.New("invalid password")
	ErrShortIDConflict = errors.New("short ID already in use")

	// Room errors
	ErrRoomNotFound        = errors.New("room not found")
	ErrRoomFull            = errors.New("room is full")
	ErrRoomAlreadyExists   = errors.New("server already has a room")
	ErrInvalidRoomPassword = errors.New("invalid room password")
)
