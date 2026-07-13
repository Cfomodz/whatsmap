// Package mapper implements WhatsApp RTT-based device activity mapping
// Based on the "Careless Whisper" research paper (arXiv:2411.11194)
package mapper

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"
)

// RTTMeasurement represents a single RTT measurement for a target
type RTTMeasurement struct {
	ID           int64
	TargetJID    string
	Timestamp    time.Time
	RTTMs        float64 // Round-trip time in milliseconds
	ServerAckMs  float64 // Time to server acknowledgement
	DeviceAckMs  float64 // Time to device acknowledgement
	ProbeType    string  // "reaction", "receipt_read", "presence"
	Success      bool
	ErrorMessage string
	// Inferred state (populated by analysis)
	InferredState string // "screen_on", "screen_off", "app_foreground", "unknown"
}

// DeviceInfo represents detected device information
type DeviceInfo struct {
	TargetJID   string
	DeviceCount int
	OSType      string // "ios", "android", "unknown"
	DetectedAt  time.Time
}

// MapperStore handles persistence of RTT measurements
type MapperStore struct {
	db *sql.DB
}

// NewMapperStore creates a new store with the given database connection
func NewMapperStore(db *sql.DB) (*MapperStore, error) {
	store := &MapperStore{db: db}
	if err := store.initSchema(); err != nil {
		return nil, fmt.Errorf("failed to initialize schema: %w", err)
	}
	return store, nil
}

func (s *MapperStore) initSchema() error {
	schema := `
	CREATE TABLE IF NOT EXISTS rtt_measurements (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		target_jid TEXT NOT NULL,
		timestamp INTEGER NOT NULL,
		rtt_ms REAL NOT NULL,
		server_ack_ms REAL,
		device_ack_ms REAL,
		probe_type TEXT NOT NULL,
		success INTEGER NOT NULL,
		error_message TEXT,
		inferred_state TEXT,
		UNIQUE(target_jid, timestamp, probe_type)
	);
	
	CREATE INDEX IF NOT EXISTS idx_rtt_target_time ON rtt_measurements(target_jid, timestamp);
	CREATE INDEX IF NOT EXISTS idx_rtt_timestamp ON rtt_measurements(timestamp);
	
	CREATE TABLE IF NOT EXISTS device_info (
		target_jid TEXT PRIMARY KEY,
		device_count INTEGER,
		os_type TEXT,
		detected_at INTEGER
	);
	
	CREATE TABLE IF NOT EXISTS probe_sessions (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		target_jid TEXT NOT NULL,
		started_at INTEGER NOT NULL,
		ended_at INTEGER,
		probe_interval_ms INTEGER NOT NULL,
		probe_type TEXT NOT NULL,
		total_probes INTEGER DEFAULT 0,
		successful_probes INTEGER DEFAULT 0,
		notes TEXT
	);
	
	CREATE TABLE IF NOT EXISTS activity_patterns (
		id INTEGER PRIMARY KEY AUTOINCREMENT,
		target_jid TEXT NOT NULL,
		pattern_type TEXT NOT NULL,
		start_time INTEGER NOT NULL,
		end_time INTEGER NOT NULL,
		confidence REAL NOT NULL,
		description TEXT,
		metadata TEXT
	);
	`
	_, err := s.db.Exec(schema)
	return err
}

// PutMeasurement stores an RTT measurement
func (s *MapperStore) PutMeasurement(ctx context.Context, m *RTTMeasurement) error {
	query := `
		INSERT OR REPLACE INTO rtt_measurements 
		(target_jid, timestamp, rtt_ms, server_ack_ms, device_ack_ms, probe_type, success, error_message, inferred_state)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	_, err := s.db.ExecContext(ctx, query,
		m.TargetJID,
		m.Timestamp.UnixMilli(),
		m.RTTMs,
		m.ServerAckMs,
		m.DeviceAckMs,
		m.ProbeType,
		m.Success,
		m.ErrorMessage,
		m.InferredState,
	)
	return err
}

// PutMeasurements stores multiple RTT measurements in a transaction
func (s *MapperStore) PutMeasurements(ctx context.Context, measurements []*RTTMeasurement) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()

	query := `
		INSERT OR REPLACE INTO rtt_measurements 
		(target_jid, timestamp, rtt_ms, server_ack_ms, device_ack_ms, probe_type, success, error_message, inferred_state)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
	`
	stmt, err := tx.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	for _, m := range measurements {
		_, err := stmt.ExecContext(ctx,
			m.TargetJID,
			m.Timestamp.UnixMilli(),
			m.RTTMs,
			m.ServerAckMs,
			m.DeviceAckMs,
			m.ProbeType,
			m.Success,
			m.ErrorMessage,
			m.InferredState,
		)
		if err != nil {
			return err
		}
	}
	return tx.Commit()
}

// GetMeasurements retrieves RTT measurements for a target within a time range
func (s *MapperStore) GetMeasurements(ctx context.Context, targetJID string, start, end time.Time) ([]*RTTMeasurement, error) {
	query := `
		SELECT id, target_jid, timestamp, rtt_ms, server_ack_ms, device_ack_ms, 
		       probe_type, success, error_message, inferred_state
		FROM rtt_measurements
		WHERE target_jid = ? AND timestamp >= ? AND timestamp <= ?
		ORDER BY timestamp ASC
	`
	rows, err := s.db.QueryContext(ctx, query, targetJID, start.UnixMilli(), end.UnixMilli())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var measurements []*RTTMeasurement
	for rows.Next() {
		var m RTTMeasurement
		var ts int64
		var serverAck, deviceAck sql.NullFloat64
		var errMsg, inferredState sql.NullString

		err := rows.Scan(
			&m.ID, &m.TargetJID, &ts, &m.RTTMs,
			&serverAck, &deviceAck, &m.ProbeType, &m.Success,
			&errMsg, &inferredState,
		)
		if err != nil {
			return nil, err
		}
		m.Timestamp = time.UnixMilli(ts)
		if serverAck.Valid {
			m.ServerAckMs = serverAck.Float64
		}
		if deviceAck.Valid {
			m.DeviceAckMs = deviceAck.Float64
		}
		if errMsg.Valid {
			m.ErrorMessage = errMsg.String
		}
		if inferredState.Valid {
			m.InferredState = inferredState.String
		}
		measurements = append(measurements, &m)
	}
	return measurements, rows.Err()
}

// GetAllMeasurementsForTarget retrieves all measurements for a target
func (s *MapperStore) GetAllMeasurementsForTarget(ctx context.Context, targetJID string) ([]*RTTMeasurement, error) {
	query := `
		SELECT id, target_jid, timestamp, rtt_ms, server_ack_ms, device_ack_ms, 
		       probe_type, success, error_message, inferred_state
		FROM rtt_measurements
		WHERE target_jid = ?
		ORDER BY timestamp ASC
	`
	rows, err := s.db.QueryContext(ctx, query, targetJID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var measurements []*RTTMeasurement
	for rows.Next() {
		var m RTTMeasurement
		var ts int64
		var serverAck, deviceAck sql.NullFloat64
		var errMsg, inferredState sql.NullString

		err := rows.Scan(
			&m.ID, &m.TargetJID, &ts, &m.RTTMs,
			&serverAck, &deviceAck, &m.ProbeType, &m.Success,
			&errMsg, &inferredState,
		)
		if err != nil {
			return nil, err
		}
		m.Timestamp = time.UnixMilli(ts)
		if serverAck.Valid {
			m.ServerAckMs = serverAck.Float64
		}
		if deviceAck.Valid {
			m.DeviceAckMs = deviceAck.Float64
		}
		if errMsg.Valid {
			m.ErrorMessage = errMsg.String
		}
		if inferredState.Valid {
			m.InferredState = inferredState.String
		}
		measurements = append(measurements, &m)
	}
	return measurements, rows.Err()
}

// PutDeviceInfo stores or updates device information
func (s *MapperStore) PutDeviceInfo(ctx context.Context, info *DeviceInfo) error {
	query := `
		INSERT OR REPLACE INTO device_info (target_jid, device_count, os_type, detected_at)
		VALUES (?, ?, ?, ?)
	`
	_, err := s.db.ExecContext(ctx, query,
		info.TargetJID,
		info.DeviceCount,
		info.OSType,
		info.DetectedAt.Unix(),
	)
	return err
}

// GetDeviceInfo retrieves device information for a target
func (s *MapperStore) GetDeviceInfo(ctx context.Context, targetJID string) (*DeviceInfo, error) {
	query := `SELECT target_jid, device_count, os_type, detected_at FROM device_info WHERE target_jid = ?`
	var info DeviceInfo
	var detectedAt int64
	err := s.db.QueryRowContext(ctx, query, targetJID).Scan(
		&info.TargetJID, &info.DeviceCount, &info.OSType, &detectedAt,
	)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	} else if err != nil {
		return nil, err
	}
	info.DetectedAt = time.Unix(detectedAt, 0)
	return &info, nil
}

// ProbeSession represents a probing session
type ProbeSession struct {
	ID               int64
	TargetJID        string
	StartedAt        time.Time
	EndedAt          *time.Time
	ProbeIntervalMs  int
	ProbeType        string
	TotalProbes      int
	SuccessfulProbes int
	Notes            string
}

// StartProbeSession creates a new probe session
func (s *MapperStore) StartProbeSession(ctx context.Context, session *ProbeSession) (int64, error) {
	query := `
		INSERT INTO probe_sessions (target_jid, started_at, probe_interval_ms, probe_type, notes)
		VALUES (?, ?, ?, ?, ?)
	`
	result, err := s.db.ExecContext(ctx, query,
		session.TargetJID,
		session.StartedAt.Unix(),
		session.ProbeIntervalMs,
		session.ProbeType,
		session.Notes,
	)
	if err != nil {
		return 0, err
	}
	return result.LastInsertId()
}

// EndProbeSession marks a probe session as ended
func (s *MapperStore) EndProbeSession(ctx context.Context, sessionID int64, totalProbes, successfulProbes int) error {
	query := `
		UPDATE probe_sessions 
		SET ended_at = ?, total_probes = ?, successful_probes = ?
		WHERE id = ?
	`
	_, err := s.db.ExecContext(ctx, query, time.Now().Unix(), totalProbes, successfulProbes, sessionID)
	return err
}

// ActivityPattern represents a detected activity pattern
type ActivityPattern struct {
	ID          int64
	TargetJID   string
	PatternType string // "online_period", "offline_period", "active_usage", "sleep_time"
	StartTime   time.Time
	EndTime     time.Time
	Confidence  float64
	Description string
	Metadata    string // JSON for additional data
}

// PutActivityPattern stores an activity pattern
func (s *MapperStore) PutActivityPattern(ctx context.Context, pattern *ActivityPattern) error {
	query := `
		INSERT INTO activity_patterns (target_jid, pattern_type, start_time, end_time, confidence, description, metadata)
		VALUES (?, ?, ?, ?, ?, ?, ?)
	`
	_, err := s.db.ExecContext(ctx, query,
		pattern.TargetJID,
		pattern.PatternType,
		pattern.StartTime.Unix(),
		pattern.EndTime.Unix(),
		pattern.Confidence,
		pattern.Description,
		pattern.Metadata,
	)
	return err
}

// GetActivityPatterns retrieves activity patterns for a target
func (s *MapperStore) GetActivityPatterns(ctx context.Context, targetJID string) ([]*ActivityPattern, error) {
	query := `
		SELECT id, target_jid, pattern_type, start_time, end_time, confidence, description, metadata
		FROM activity_patterns
		WHERE target_jid = ?
		ORDER BY start_time ASC
	`
	rows, err := s.db.QueryContext(ctx, query, targetJID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var patterns []*ActivityPattern
	for rows.Next() {
		var p ActivityPattern
		var startTS, endTS int64
		var desc, meta sql.NullString
		err := rows.Scan(&p.ID, &p.TargetJID, &p.PatternType, &startTS, &endTS, &p.Confidence, &desc, &meta)
		if err != nil {
			return nil, err
		}
		p.StartTime = time.Unix(startTS, 0)
		p.EndTime = time.Unix(endTS, 0)
		if desc.Valid {
			p.Description = desc.String
		}
		if meta.Valid {
			p.Metadata = meta.String
		}
		patterns = append(patterns, &p)
	}
	return patterns, rows.Err()
}

// GetStats returns statistics about measurements for a target
func (s *MapperStore) GetStats(ctx context.Context, targetJID string) (map[string]interface{}, error) {
	stats := make(map[string]interface{})

	// Total measurements
	var totalCount int
	err := s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM rtt_measurements WHERE target_jid = ?`,
		targetJID,
	).Scan(&totalCount)
	if err != nil {
		return nil, err
	}
	stats["total_measurements"] = totalCount

	// Successful measurements
	var successCount int
	err = s.db.QueryRowContext(ctx,
		`SELECT COUNT(*) FROM rtt_measurements WHERE target_jid = ? AND success = 1`,
		targetJID,
	).Scan(&successCount)
	if err != nil {
		return nil, err
	}
	stats["successful_measurements"] = successCount

	// Average RTT
	var avgRTT sql.NullFloat64
	err = s.db.QueryRowContext(ctx,
		`SELECT AVG(rtt_ms) FROM rtt_measurements WHERE target_jid = ? AND success = 1`,
		targetJID,
	).Scan(&avgRTT)
	if err != nil {
		return nil, err
	}
	if avgRTT.Valid {
		stats["avg_rtt_ms"] = avgRTT.Float64
	}

	// Min/Max RTT
	var minRTT, maxRTT sql.NullFloat64
	err = s.db.QueryRowContext(ctx,
		`SELECT MIN(rtt_ms), MAX(rtt_ms) FROM rtt_measurements WHERE target_jid = ? AND success = 1`,
		targetJID,
	).Scan(&minRTT, &maxRTT)
	if err != nil {
		return nil, err
	}
	if minRTT.Valid {
		stats["min_rtt_ms"] = minRTT.Float64
	}
	if maxRTT.Valid {
		stats["max_rtt_ms"] = maxRTT.Float64
	}

	// Time range
	var minTS, maxTS sql.NullInt64
	err = s.db.QueryRowContext(ctx,
		`SELECT MIN(timestamp), MAX(timestamp) FROM rtt_measurements WHERE target_jid = ?`,
		targetJID,
	).Scan(&minTS, &maxTS)
	if err != nil {
		return nil, err
	}
	if minTS.Valid {
		stats["first_measurement"] = time.UnixMilli(minTS.Int64)
	}
	if maxTS.Valid {
		stats["last_measurement"] = time.UnixMilli(maxTS.Int64)
	}

	return stats, nil
}

// ExportToCSV exports measurements to CSV format
func (s *MapperStore) ExportToCSV(ctx context.Context, targetJID string) (string, error) {
	measurements, err := s.GetAllMeasurementsForTarget(ctx, targetJID)
	if err != nil {
		return "", err
	}

	csv := "timestamp,timestamp_unix_ms,rtt_ms,server_ack_ms,device_ack_ms,probe_type,success,inferred_state,error\n"
	for _, m := range measurements {
		csv += fmt.Sprintf("%s,%d,%.2f,%.2f,%.2f,%s,%t,%s,%s\n",
			m.Timestamp.Format(time.RFC3339),
			m.Timestamp.UnixMilli(),
			m.RTTMs,
			m.ServerAckMs,
			m.DeviceAckMs,
			m.ProbeType,
			m.Success,
			m.InferredState,
			m.ErrorMessage,
		)
	}
	return csv, nil
}
