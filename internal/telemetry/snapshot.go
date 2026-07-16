package telemetry

import "strings"

// report is the full Telemetry Snapshot Contract v1 payload (see
// plans/260715-2315-wan-p2p-turn-quality-hardening/telemetry-snapshot-contract-v1.md).
// Producers (Android, Host) send this fire-and-forget on every selected-pair
// snapshot / path transition / WS-safe-mode event. All fields are attacker
// controlled input from an unauthenticated endpoint — normalize() below is the
// single validation boundary: unknown enums fold to "unknown", absurd numerics
// are dropped or clamped, nothing here may ever panic on malformed input.
type report struct {
	// Identity block — required on every event.
	SchemaVersion int    `json:"schema_version"`
	Source        string `json:"source"`
	Event         string `json:"event"`
	SessionID     string `json:"session_id"`
	PCRole        string `json:"pc_role"`
	MonitorIndex  int    `json:"monitor_index"`
	Generation    int    `json:"generation"`
	Sequence      int    `json:"sequence"`

	// Selected candidate-pair block — present on snapshot/path_transition,
	// absent/ignored on ws_safe_mode_*.
	LocalCandidateType  string `json:"local_candidate_type,omitempty"`
	RemoteCandidateType string `json:"remote_candidate_type,omitempty"`
	AddressFamily       string `json:"address_family,omitempty"`
	Protocol            string `json:"protocol,omitempty"`
	RelayProtocol       string `json:"relay_protocol,omitempty"`
	PathClass           string `json:"path_class,omitempty"`

	// QoE block — entirely optional; nil pointer means "unavailable".
	RTTMs                *int     `json:"rtt_ms,omitempty"`
	JitterMs             *int     `json:"jitter_ms,omitempty"`
	LossPct              *float64 `json:"loss_pct,omitempty"`
	SendBitrateKbps      *int     `json:"send_bitrate_kbps,omitempty"`
	AvailableBitrateKbps *int     `json:"available_bitrate_kbps,omitempty"`
	Codec                string   `json:"codec,omitempty"`
	Width                *int     `json:"width,omitempty"`
	Height               *int     `json:"height,omitempty"`
	FPS                  *int     `json:"fps,omitempty"`
	QP                   *int     `json:"qp,omitempty"`
	FrameDrops           *int     `json:"frame_drops,omitempty"`
	TTFFMs               *int     `json:"ttff_ms,omitempty"`
	FreezeCount          *int     `json:"freeze_count,omitempty"`
	FreezeMsTotal        *int     `json:"freeze_ms_total,omitempty"`
}

// maxSessionIDLen bounds the opaque session token so a hostile client cannot
// stuff an arbitrarily large string into the admin-only ring buffer.
const maxSessionIDLen = 128

// maxCounterValue bounds monotonic counters (generation/sequence) — legitimate
// sessions never approach this; anything higher is almost certainly garbage.
const maxCounterValue = 1_000_000_000

// Whitelists bound enum cardinality so an abusive client cannot make the
// aggregate maps grow unbounded — any value off-list folds into "unknown".
var (
	validSources        = map[string]bool{"android": true, "host": true, "unknown": true}
	validEvents         = map[string]bool{"snapshot": true, "path_transition": true, "ws_safe_mode_enter": true, "ws_safe_mode_exit": true, "unknown": true}
	validPCRoles        = map[string]bool{"main": true, "video": true, "unknown": true}
	validCandidateTypes = map[string]bool{"host": true, "srflx": true, "prflx": true, "relay": true, "unknown": true}
	validFamilies       = map[string]bool{"ipv4": true, "ipv6": true, "unknown": true}
	validProtocols      = map[string]bool{"udp": true, "tcp": true, "unknown": true}
	validRelayProtocols = map[string]bool{"udp": true, "tcp": true, "tls": true, "none": true, "unknown": true}
	validPathClasses    = map[string]bool{"direct": true, "relay": true, "unknown": true}
	validCodecs         = map[string]bool{"h264": true, "h265": true, "av1": true, "unknown": true}
)

// normalize folds unknown enums, bounds counters/session-id, and clamps or
// drops absurd QoE numerics. Called once, right after JSON decode, before the
// report ever reaches the aggregator. Never errors — worst case every field
// collapses to "unknown"/zero, the report is still counted (fire-and-forget).
func (r *report) normalize() {
	r.Source = foldEnum(r.Source, validSources)
	r.Event = foldEnum(r.Event, validEvents)
	r.SessionID = normalizeSessionID(r.SessionID)
	r.PCRole = foldEnum(r.PCRole, validPCRoles)
	r.MonitorIndex = clampIntValue(r.MonitorIndex, 0, 15)
	r.Generation = clampIntValue(r.Generation, 0, maxCounterValue)
	r.Sequence = clampIntValue(r.Sequence, 0, maxCounterValue)

	r.LocalCandidateType = foldEnum(r.LocalCandidateType, validCandidateTypes)
	r.RemoteCandidateType = foldEnum(r.RemoteCandidateType, validCandidateTypes)
	r.AddressFamily = foldEnum(r.AddressFamily, validFamilies)
	r.Protocol = foldEnum(r.Protocol, validProtocols)
	r.RelayProtocol = foldEnum(r.RelayProtocol, validRelayProtocols)
	r.PathClass = foldEnum(r.PathClass, validPathClasses)
	r.Codec = foldEnum(r.Codec, validCodecs)

	r.RTTMs = clampOrDropInt(r.RTTMs, 0, 60_000)
	r.JitterMs = clampOrDropInt(r.JitterMs, 0, 60_000)
	r.LossPct = clampOrDropFloat(r.LossPct, 0, 100)
	r.SendBitrateKbps = clampOrDropInt(r.SendBitrateKbps, 0, 1_000_000)
	r.AvailableBitrateKbps = clampOrDropInt(r.AvailableBitrateKbps, 0, 1_000_000)
	r.Width = clampOrDropInt(r.Width, 0, 16_384)
	r.Height = clampOrDropInt(r.Height, 0, 16_384)
	r.FPS = clampOrDropInt(r.FPS, 0, 1_000)
	r.QP = clampOrDropInt(r.QP, 0, 63)
	r.FrameDrops = clampOrDropInt(r.FrameDrops, 0, 10_000_000)
	r.TTFFMs = clampOrDropInt(r.TTFFMs, 0, 600_000)
	r.FreezeCount = clampOrDropInt(r.FreezeCount, 0, 10_000_000)
	r.FreezeMsTotal = clampOrDropInt(r.FreezeMsTotal, 0, 3_600_000)
}

// foldEnum lower-cases/trims v and folds it to "unknown" if not in whitelist.
func foldEnum(v string, whitelist map[string]bool) string {
	v = strings.ToLower(strings.TrimSpace(v))
	if !whitelist[v] {
		return "unknown"
	}
	return v
}

// normalizeSessionID trims, defaults empty to "unknown", and truncates
// oversized tokens. session_id is opaque — no enum/format validation beyond bounding length.
func normalizeSessionID(s string) string {
	s = strings.TrimSpace(s)
	if s == "" {
		return "unknown"
	}
	if len(s) > maxSessionIDLen {
		s = s[:maxSessionIDLen]
	}
	return s
}

// clampIntValue bounds a required (non-pointer) int field to [lo, hi].
func clampIntValue(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

// clampOrDropInt bounds an optional numeric field. A value below lo is treated
// as an invalid sentinel (e.g. -1 for "unset") and dropped (nil); a value
// above hi is clamped to hi rather than rejected outright, since producers may
// legitimately exceed conservative bounds under transient extreme conditions.
func clampOrDropInt(p *int, lo, hi int) *int {
	if p == nil {
		return nil
	}
	v := *p
	if v < lo {
		return nil
	}
	if v > hi {
		v = hi
	}
	return &v
}

// clampOrDropFloat is the float64 counterpart of clampOrDropInt.
func clampOrDropFloat(p *float64, lo, hi float64) *float64 {
	if p == nil {
		return nil
	}
	v := *p
	if v < lo {
		return nil
	}
	if v > hi {
		v = hi
	}
	return &v
}
