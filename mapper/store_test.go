package mapper

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// newTestStore opens a MapperStore backed by a temporary on-disk sqlite
// database. An on-disk file (rather than ":memory:") is used deliberately:
// database/sql keeps a connection pool, and each pooled connection to an
// in-memory sqlite gets its own independent database, which makes schema and
// rows created on one connection invisible to the next.
func newTestStore(t *testing.T) (*MapperStore, *sql.DB) {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "test.db")
	db, err := sql.Open("sqlite3", "file:"+dbPath+"?_foreign_keys=on")
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { db.Close() })

	store, err := NewMapperStore(db)
	if err != nil {
		t.Fatalf("new store: %v", err)
	}
	return store, db
}

func TestMeasurementRoundTrip(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestStore(t)

	target := "14155551234@s.whatsapp.net"
	base := time.Unix(1700000000, 0)

	want := &RTTMeasurement{
		TargetJID:     target,
		Timestamp:     base,
		RTTMs:         250.5,
		ServerAckMs:   40.0,
		DeviceAckMs:   210.5,
		ProbeType:     "reaction",
		Success:       true,
		InferredState: "app_foreground",
	}
	if err := store.PutMeasurement(ctx, want); err != nil {
		t.Fatalf("put measurement: %v", err)
	}

	got, err := store.GetAllMeasurementsForTarget(ctx, target)
	if err != nil {
		t.Fatalf("get measurements: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 measurement, got %d", len(got))
	}

	m := got[0]
	if m.RTTMs != want.RTTMs || m.ServerAckMs != want.ServerAckMs || m.DeviceAckMs != want.DeviceAckMs {
		t.Errorf("RTT fields mismatch: got %+v want %+v", m, want)
	}
	if m.ProbeType != want.ProbeType || m.InferredState != want.InferredState || !m.Success {
		t.Errorf("metadata mismatch: got %+v want %+v", m, want)
	}
	// Timestamps are persisted at millisecond resolution.
	if !m.Timestamp.Equal(base) {
		t.Errorf("timestamp mismatch: got %v want %v", m.Timestamp, base)
	}
}

func TestMeasurementUpsertOnConflict(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestStore(t)

	target := "14155551234@s.whatsapp.net"
	ts := time.Unix(1700000000, 0)

	first := &RTTMeasurement{TargetJID: target, Timestamp: ts, RTTMs: 100, ProbeType: "reaction", Success: true}
	second := &RTTMeasurement{TargetJID: target, Timestamp: ts, RTTMs: 999, ProbeType: "reaction", Success: true}

	if err := store.PutMeasurement(ctx, first); err != nil {
		t.Fatalf("put first: %v", err)
	}
	// Same (target, timestamp, probe_type) key -> INSERT OR REPLACE overwrites.
	if err := store.PutMeasurement(ctx, second); err != nil {
		t.Fatalf("put second: %v", err)
	}

	got, err := store.GetAllMeasurementsForTarget(ctx, target)
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if len(got) != 1 {
		t.Fatalf("expected 1 row after upsert, got %d", len(got))
	}
	if got[0].RTTMs != 999 {
		t.Errorf("expected upsert to overwrite RTT to 999, got %v", got[0].RTTMs)
	}
}

func TestGetMeasurementsTimeRange(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestStore(t)

	target := "14155551234@s.whatsapp.net"
	base := time.Unix(1700000000, 0)

	for i := 0; i < 5; i++ {
		m := &RTTMeasurement{
			TargetJID: target,
			Timestamp: base.Add(time.Duration(i) * time.Minute),
			RTTMs:     float64(100 + i),
			ProbeType: "reaction",
			Success:   true,
		}
		if err := store.PutMeasurement(ctx, m); err != nil {
			t.Fatalf("put %d: %v", i, err)
		}
	}

	// Window covering indices 1..3 inclusive.
	got, err := store.GetMeasurements(ctx, target, base.Add(time.Minute), base.Add(3*time.Minute))
	if err != nil {
		t.Fatalf("get range: %v", err)
	}
	if len(got) != 3 {
		t.Fatalf("expected 3 measurements in range, got %d", len(got))
	}
	// Results are ordered by timestamp ascending.
	for i := 1; i < len(got); i++ {
		if got[i].Timestamp.Before(got[i-1].Timestamp) {
			t.Errorf("results not ordered ascending at index %d", i)
		}
	}
}

func TestStatsAndCSVExport(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestStore(t)

	target := "14155551234@s.whatsapp.net"
	base := time.Unix(1700000000, 0)

	// Three successes (100/200/300ms) and one failure.
	rows := []*RTTMeasurement{
		{TargetJID: target, Timestamp: base, RTTMs: 100, ProbeType: "reaction", Success: true, InferredState: "app_foreground"},
		{TargetJID: target, Timestamp: base.Add(time.Second), RTTMs: 200, ProbeType: "reaction", Success: true, InferredState: "screen_on"},
		{TargetJID: target, Timestamp: base.Add(2 * time.Second), RTTMs: 300, ProbeType: "reaction", Success: true, InferredState: "screen_off"},
		{TargetJID: target, Timestamp: base.Add(3 * time.Second), ProbeType: "reaction", Success: false, ErrorMessage: "timeout", InferredState: "unreachable"},
	}
	if err := store.PutMeasurements(ctx, rows); err != nil {
		t.Fatalf("put batch: %v", err)
	}

	stats, err := store.GetStats(ctx, target)
	if err != nil {
		t.Fatalf("stats: %v", err)
	}
	if stats["total_measurements"] != 4 {
		t.Errorf("total_measurements = %v, want 4", stats["total_measurements"])
	}
	if stats["successful_measurements"] != 3 {
		t.Errorf("successful_measurements = %v, want 3", stats["successful_measurements"])
	}
	if avg, ok := stats["avg_rtt_ms"].(float64); !ok || avg != 200 {
		t.Errorf("avg_rtt_ms = %v, want 200", stats["avg_rtt_ms"])
	}
	if min, ok := stats["min_rtt_ms"].(float64); !ok || min != 100 {
		t.Errorf("min_rtt_ms = %v, want 100", stats["min_rtt_ms"])
	}
	if max, ok := stats["max_rtt_ms"].(float64); !ok || max != 300 {
		t.Errorf("max_rtt_ms = %v, want 300", stats["max_rtt_ms"])
	}

	csv, err := store.ExportToCSV(ctx, target)
	if err != nil {
		t.Fatalf("export csv: %v", err)
	}
	lines := strings.Split(strings.TrimRight(csv, "\n"), "\n")
	// 1 header + 4 data rows.
	if len(lines) != 5 {
		t.Fatalf("expected 5 CSV lines (header + 4 rows), got %d:\n%s", len(lines), csv)
	}
	if !strings.HasPrefix(lines[0], "timestamp,") {
		t.Errorf("unexpected CSV header: %q", lines[0])
	}
}

func TestProbeSessionLifecycle(t *testing.T) {
	ctx := context.Background()
	store, db := newTestStore(t)

	session := &ProbeSession{
		TargetJID:       "14155551234@s.whatsapp.net",
		StartedAt:       time.Unix(1700000000, 0),
		ProbeIntervalMs: 2000,
		ProbeType:       "reaction",
	}
	id, err := store.StartProbeSession(ctx, session)
	if err != nil {
		t.Fatalf("start session: %v", err)
	}
	if id <= 0 {
		t.Fatalf("expected positive session id, got %d", id)
	}

	if err := store.EndProbeSession(ctx, id, 50, 42); err != nil {
		t.Fatalf("end session: %v", err)
	}

	var total, successful int
	var endedAt sql.NullInt64
	err = db.QueryRowContext(ctx,
		"SELECT total_probes, successful_probes, ended_at FROM probe_sessions WHERE id = ?", id,
	).Scan(&total, &successful, &endedAt)
	if err != nil {
		t.Fatalf("query session: %v", err)
	}
	if total != 50 || successful != 42 {
		t.Errorf("session counters = (%d,%d), want (50,42)", total, successful)
	}
	if !endedAt.Valid {
		t.Error("expected ended_at to be set after EndProbeSession")
	}
}

func TestDeviceInfoRoundTrip(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestStore(t)

	target := "14155551234@s.whatsapp.net"

	// Missing row returns (nil, nil).
	got, err := store.GetDeviceInfo(ctx, target)
	if err != nil {
		t.Fatalf("get missing device info: %v", err)
	}
	if got != nil {
		t.Fatalf("expected nil for missing device info, got %+v", got)
	}

	want := &DeviceInfo{TargetJID: target, DeviceCount: 2, OSType: "ios", DetectedAt: time.Unix(1700000000, 0)}
	if err := store.PutDeviceInfo(ctx, want); err != nil {
		t.Fatalf("put device info: %v", err)
	}

	got, err = store.GetDeviceInfo(ctx, target)
	if err != nil {
		t.Fatalf("get device info: %v", err)
	}
	if got == nil || got.DeviceCount != 2 || got.OSType != "ios" {
		t.Errorf("device info mismatch: got %+v want %+v", got, want)
	}
}
