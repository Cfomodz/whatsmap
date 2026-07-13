package mapper

import (
	"context"
	"testing"
	"time"
)

func TestAnalyzeNoData(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestStore(t)
	analyzer := NewAnalyzer(store)

	if _, err := analyzer.Analyze(ctx, "14155551234@s.whatsapp.net"); err == nil {
		t.Fatal("expected an error when analyzing a target with no measurements")
	}
}

func TestAnalyzeProducesResult(t *testing.T) {
	ctx := context.Background()
	store, _ := newTestStore(t)
	analyzer := NewAnalyzer(store)

	target := "14155551234@s.whatsapp.net"
	base := time.Date(2024, 1, 15, 8, 0, 0, 0, time.UTC)

	// Seed a day's worth of measurements alternating between fast (screen on)
	// and slow (screen off) RTTs so the analyzer has a bimodal distribution
	// and enough points to compute stats.
	var batch []*RTTMeasurement
	for i := 0; i < 60; i++ {
		rtt := 200.0
		state := "screen_on"
		if i%2 == 0 {
			rtt = 2000.0
			state = "screen_off"
		}
		batch = append(batch, &RTTMeasurement{
			TargetJID:     target,
			Timestamp:     base.Add(time.Duration(i) * time.Minute),
			RTTMs:         rtt,
			ProbeType:     "reaction",
			Success:       true,
			InferredState: state,
		})
	}
	if err := store.PutMeasurements(ctx, batch); err != nil {
		t.Fatalf("seed measurements: %v", err)
	}

	result, err := analyzer.Analyze(ctx, target)
	if err != nil {
		t.Fatalf("analyze: %v", err)
	}

	if result.TargetJID != target {
		t.Errorf("target jid = %q, want %q", result.TargetJID, target)
	}
	if result.RTTStats == nil || result.RTTStats.Count != len(batch) {
		t.Fatalf("expected RTT stats over %d measurements, got %+v", len(batch), result.RTTStats)
	}
	if result.RTTStats.Min != 200 || result.RTTStats.Max != 2000 {
		t.Errorf("min/max = %v/%v, want 200/2000", result.RTTStats.Min, result.RTTStats.Max)
	}
	if result.RTTStats.Mean <= 200 || result.RTTStats.Mean >= 2000 {
		t.Errorf("mean %v should sit between the two modes", result.RTTStats.Mean)
	}
	if !result.Period.Start.Equal(base) {
		t.Errorf("period start = %v, want %v", result.Period.Start, base)
	}

	// GenerateReport should render without panicking and mention the target.
	report := analyzer.GenerateReport(result)
	if report == "" {
		t.Error("expected a non-empty report")
	}
}
