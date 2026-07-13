package mapper

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"time"
)

// Analyzer processes RTT measurements to extract patterns and insights
type Analyzer struct {
	store *MapperStore
}

// NewAnalyzer creates a new analyzer instance
func NewAnalyzer(store *MapperStore) *Analyzer {
	return &Analyzer{store: store}
}

// AnalysisResult contains the complete analysis for a target
type AnalysisResult struct {
	TargetJID string `json:"target_jid"`
	Period    struct {
		Start time.Time `json:"start"`
		End   time.Time `json:"end"`
	} `json:"period"`

	// Device detection
	DeviceInfo *DeviceAnalysis `json:"device_info"`

	// RTT Statistics
	RTTStats *RTTStatistics `json:"rtt_stats"`

	// Activity patterns
	DailyPattern    *DailyPattern     `json:"daily_pattern"`
	ActivityPeriods []*ActivityPeriod `json:"activity_periods"`

	// Connection analysis
	ConnectionAnalysis *ConnectionAnalysis `json:"connection_analysis"`

	// Confidence scores
	OverallConfidence float64 `json:"overall_confidence"`
}

// DeviceAnalysis contains device type inference
type DeviceAnalysis struct {
	LikelyOS        string  `json:"likely_os"`     // "ios", "android", "unknown"
	OSConfidence    float64 `json:"os_confidence"` // 0-1
	DeviceCount     int     `json:"device_count"`  // Number of companion devices
	HasCompanion    bool    `json:"has_companion"` // Desktop/web client detected
	CompanionActive bool    `json:"companion_active"`
}

// RTTStatistics contains RTT distribution stats
type RTTStatistics struct {
	Count        int     `json:"count"`
	Mean         float64 `json:"mean"`
	Median       float64 `json:"median"`
	StdDev       float64 `json:"std_dev"`
	Min          float64 `json:"min"`
	Max          float64 `json:"max"`
	P5           float64 `json:"p5"`  // 5th percentile
	P95          float64 `json:"p95"` // 95th percentile
	Bimodal      bool    `json:"bimodal"`
	ScreenOnAvg  float64 `json:"screen_on_avg"`  // Avg RTT when screen likely on
	ScreenOffAvg float64 `json:"screen_off_avg"` // Avg RTT when screen likely off
}

// DailyPattern represents usage patterns throughout the day
type DailyPattern struct {
	HourlyActivity   [24]HourlyStats `json:"hourly_activity"`
	PeakHours        []int           `json:"peak_hours"`
	QuietHours       []int           `json:"quiet_hours"`
	TypicalWakeTime  string          `json:"typical_wake_time"`
	TypicalSleepTime string          `json:"typical_sleep_time"`
}

// HourlyStats contains stats for a specific hour
type HourlyStats struct {
	Hour               int     `json:"hour"`
	MeasurementCount   int     `json:"measurement_count"`
	AvgRTT             float64 `json:"avg_rtt"`
	ScreenOnPercentage float64 `json:"screen_on_percentage"`
	ActivePercentage   float64 `json:"active_percentage"` // App foreground
}

// ActivityPeriod represents a detected period of activity
type ActivityPeriod struct {
	Start       time.Time `json:"start"`
	End         time.Time `json:"end"`
	Duration    string    `json:"duration"`
	State       string    `json:"state"` // "active", "screen_on", "screen_off", "offline"
	Confidence  float64   `json:"confidence"`
	Transitions int       `json:"transitions"` // Number of state changes within
}

// ConnectionAnalysis contains network connection insights
type ConnectionAnalysis struct {
	LikelyWiFi       float64 `json:"likely_wifi_percentage"`
	LikelyCellular   float64 `json:"likely_cellular_percentage"`
	ConnectionStable bool    `json:"connection_stable"`
	AvgResponseTime  float64 `json:"avg_response_time"`
}

// Analyze performs a complete analysis on the target's RTT data
func (a *Analyzer) Analyze(ctx context.Context, targetJID string) (*AnalysisResult, error) {
	measurements, err := a.store.GetAllMeasurementsForTarget(ctx, targetJID)
	if err != nil {
		return nil, fmt.Errorf("failed to get measurements: %w", err)
	}

	if len(measurements) == 0 {
		return nil, fmt.Errorf("no measurements found for target")
	}

	result := &AnalysisResult{
		TargetJID: targetJID,
	}

	// Set time period
	result.Period.Start = measurements[0].Timestamp
	result.Period.End = measurements[len(measurements)-1].Timestamp

	// Calculate RTT statistics
	result.RTTStats = a.calculateRTTStats(measurements)

	// Analyze device type
	result.DeviceInfo = a.analyzeDevice(measurements)

	// Analyze daily patterns
	result.DailyPattern = a.analyzeDailyPattern(measurements)

	// Detect activity periods
	result.ActivityPeriods = a.detectActivityPeriods(measurements)

	// Analyze connection
	result.ConnectionAnalysis = a.analyzeConnection(measurements)

	// Calculate overall confidence
	result.OverallConfidence = a.calculateOverallConfidence(result, len(measurements))

	// Store detected patterns
	for _, period := range result.ActivityPeriods {
		pattern := &ActivityPattern{
			TargetJID:   targetJID,
			PatternType: period.State,
			StartTime:   period.Start,
			EndTime:     period.End,
			Confidence:  period.Confidence,
			Description: fmt.Sprintf("%s for %s", period.State, period.Duration),
		}
		metadata, _ := json.Marshal(period)
		pattern.Metadata = string(metadata)
		a.store.PutActivityPattern(ctx, pattern)
	}

	return result, nil
}

func (a *Analyzer) calculateRTTStats(measurements []*RTTMeasurement) *RTTStatistics {
	// Filter successful measurements
	var rtts []float64
	for _, m := range measurements {
		if m.Success && m.RTTMs > 0 {
			rtts = append(rtts, m.RTTMs)
		}
	}

	if len(rtts) == 0 {
		return &RTTStatistics{}
	}

	sort.Float64s(rtts)

	stats := &RTTStatistics{
		Count:  len(rtts),
		Min:    rtts[0],
		Max:    rtts[len(rtts)-1],
		P5:     percentile(rtts, 5),
		P95:    percentile(rtts, 95),
		Median: percentile(rtts, 50),
	}

	// Calculate mean and stddev
	var sum float64
	for _, rtt := range rtts {
		sum += rtt
	}
	stats.Mean = sum / float64(len(rtts))

	var variance float64
	for _, rtt := range rtts {
		variance += (rtt - stats.Mean) * (rtt - stats.Mean)
	}
	stats.StdDev = math.Sqrt(variance / float64(len(rtts)))

	// Check for bimodal distribution (indicates screen on/off states)
	// A bimodal distribution would have two clusters
	stats.Bimodal = a.detectBimodal(rtts)

	// Calculate screen on/off averages based on threshold
	const screenThreshold = 1000.0 // ms
	var screenOnSum, screenOffSum float64
	var screenOnCount, screenOffCount int
	for _, rtt := range rtts {
		if rtt < screenThreshold {
			screenOnSum += rtt
			screenOnCount++
		} else {
			screenOffSum += rtt
			screenOffCount++
		}
	}
	if screenOnCount > 0 {
		stats.ScreenOnAvg = screenOnSum / float64(screenOnCount)
	}
	if screenOffCount > 0 {
		stats.ScreenOffAvg = screenOffSum / float64(screenOffCount)
	}

	return stats
}

func (a *Analyzer) detectBimodal(rtts []float64) bool {
	if len(rtts) < 20 {
		return false
	}

	// Simple bimodal detection using histogram analysis
	// Create buckets and look for two peaks
	buckets := make([]int, 10)
	maxRTT := rtts[len(rtts)-1]
	bucketSize := maxRTT / 10.0

	for _, rtt := range rtts {
		bucket := int(rtt / bucketSize)
		if bucket >= 10 {
			bucket = 9
		}
		buckets[bucket]++
	}

	// Count peaks (local maxima)
	peaks := 0
	for i := 1; i < len(buckets)-1; i++ {
		if buckets[i] > buckets[i-1] && buckets[i] > buckets[i+1] {
			peaks++
		}
	}

	return peaks >= 2
}

func (a *Analyzer) analyzeDevice(measurements []*RTTMeasurement) *DeviceAnalysis {
	device := &DeviceAnalysis{
		LikelyOS:     "unknown",
		OSConfidence: 0.5,
		DeviceCount:  1,
	}

	var rtts []float64
	for _, m := range measurements {
		if m.Success {
			rtts = append(rtts, m.RTTMs)
		}
	}

	if len(rtts) < 10 {
		return device
	}

	sort.Float64s(rtts)

	// iOS vs Android detection based on RTT patterns from the paper
	// iOS typically has more consistent, faster RTTs when screen is on
	// Android has more variation due to doze mode and various OEM implementations

	// Calculate coefficient of variation
	mean := 0.0
	for _, r := range rtts {
		mean += r
	}
	mean /= float64(len(rtts))

	variance := 0.0
	for _, r := range rtts {
		variance += (r - mean) * (r - mean)
	}
	stddev := math.Sqrt(variance / float64(len(rtts)))
	cv := stddev / mean

	// iOS tends to have lower CV (more consistent)
	// Based on paper findings
	if cv < 0.3 {
		device.LikelyOS = "ios"
		device.OSConfidence = 0.7
	} else if cv > 0.5 {
		device.LikelyOS = "android"
		device.OSConfidence = 0.65
	}

	// Check for companion device patterns
	// Companion devices often show parallel processing patterns
	// with slightly different RTT characteristics
	device.HasCompanion = a.detectCompanionDevice(measurements)

	return device
}

func (a *Analyzer) detectCompanionDevice(measurements []*RTTMeasurement) bool {
	// Look for patterns that suggest multiple devices receiving messages
	// This could be detected by observing multiple receipts for same message
	// or by seeing RTT clusters at different levels

	if len(measurements) < 50 {
		return false
	}

	// Count measurements that came in very quick succession (< 100ms apart)
	// which might indicate multiple devices responding
	quickPairs := 0
	for i := 1; i < len(measurements); i++ {
		timeDiff := measurements[i].Timestamp.Sub(measurements[i-1].Timestamp)
		if timeDiff < 100*time.Millisecond && measurements[i].Success && measurements[i-1].Success {
			quickPairs++
		}
	}

	// If more than 5% of measurements have quick pairs, likely has companion
	return float64(quickPairs)/float64(len(measurements)) > 0.05
}

func (a *Analyzer) analyzeDailyPattern(measurements []*RTTMeasurement) *DailyPattern {
	pattern := &DailyPattern{}

	// Group measurements by hour
	hourlyMeasurements := make([][]*RTTMeasurement, 24)
	for i := range hourlyMeasurements {
		hourlyMeasurements[i] = make([]*RTTMeasurement, 0)
	}

	for _, m := range measurements {
		hour := m.Timestamp.Hour()
		hourlyMeasurements[hour] = append(hourlyMeasurements[hour], m)
	}

	// Calculate stats for each hour
	maxActivity := 0.0
	for hour := 0; hour < 24; hour++ {
		ms := hourlyMeasurements[hour]
		stats := &pattern.HourlyActivity[hour]
		stats.Hour = hour
		stats.MeasurementCount = len(ms)

		if len(ms) == 0 {
			continue
		}

		// Calculate averages
		var rttSum float64
		var screenOnCount, activeCount int
		for _, m := range ms {
			if m.Success {
				rttSum += m.RTTMs
				if m.RTTMs < 1000 {
					screenOnCount++
				}
				if m.RTTMs < 300 {
					activeCount++
				}
			}
		}

		stats.AvgRTT = rttSum / float64(len(ms))
		stats.ScreenOnPercentage = float64(screenOnCount) / float64(len(ms)) * 100
		stats.ActivePercentage = float64(activeCount) / float64(len(ms)) * 100

		if stats.ScreenOnPercentage > maxActivity {
			maxActivity = stats.ScreenOnPercentage
		}
	}

	// Identify peak and quiet hours
	activityThreshold := maxActivity * 0.7
	quietThreshold := maxActivity * 0.3

	for hour := 0; hour < 24; hour++ {
		if pattern.HourlyActivity[hour].ScreenOnPercentage >= activityThreshold {
			pattern.PeakHours = append(pattern.PeakHours, hour)
		}
		if pattern.HourlyActivity[hour].ScreenOnPercentage <= quietThreshold {
			pattern.QuietHours = append(pattern.QuietHours, hour)
		}
	}

	// Detect wake/sleep times
	pattern.TypicalWakeTime, pattern.TypicalSleepTime = a.detectSleepPattern(pattern)

	return pattern
}

func (a *Analyzer) detectSleepPattern(pattern *DailyPattern) (wakeTime, sleepTime string) {
	// Look for transitions from low to high activity (wake)
	// and high to low activity (sleep)

	wakeHour := -1
	sleepHour := -1

	// Find wake time: transition from quiet to active between 4am-11am
	for hour := 4; hour <= 11; hour++ {
		prevHour := hour - 1
		if pattern.HourlyActivity[prevHour].ScreenOnPercentage < 30 &&
			pattern.HourlyActivity[hour].ScreenOnPercentage > 50 {
			wakeHour = hour
			break
		}
	}

	// Find sleep time: transition from active to quiet between 8pm-2am
	for hour := 20; hour <= 26; hour++ {
		checkHour := hour % 24
		nextHour := (hour + 1) % 24
		if pattern.HourlyActivity[checkHour].ScreenOnPercentage > 50 &&
			pattern.HourlyActivity[nextHour].ScreenOnPercentage < 30 {
			sleepHour = checkHour
			break
		}
	}

	if wakeHour >= 0 {
		wakeTime = fmt.Sprintf("%02d:00", wakeHour)
	} else {
		wakeTime = "unknown"
	}

	if sleepHour >= 0 {
		sleepTime = fmt.Sprintf("%02d:00", sleepHour)
	} else {
		sleepTime = "unknown"
	}

	return
}

func (a *Analyzer) detectActivityPeriods(measurements []*RTTMeasurement) []*ActivityPeriod {
	if len(measurements) < 2 {
		return nil
	}

	var periods []*ActivityPeriod
	var currentPeriod *ActivityPeriod

	for i, m := range measurements {
		state := m.InferredState
		if state == "" {
			state = a.classifyState(m.RTTMs, m.Success)
		}

		if currentPeriod == nil {
			currentPeriod = &ActivityPeriod{
				Start:      m.Timestamp,
				State:      state,
				Confidence: 0.8,
			}
		} else if state != currentPeriod.State {
			// State changed, close current period
			currentPeriod.End = measurements[i-1].Timestamp
			currentPeriod.Duration = currentPeriod.End.Sub(currentPeriod.Start).String()
			periods = append(periods, currentPeriod)

			// Start new period
			currentPeriod = &ActivityPeriod{
				Start:      m.Timestamp,
				State:      state,
				Confidence: 0.8,
			}
		}
	}

	// Close final period
	if currentPeriod != nil {
		currentPeriod.End = measurements[len(measurements)-1].Timestamp
		currentPeriod.Duration = currentPeriod.End.Sub(currentPeriod.Start).String()
		periods = append(periods, currentPeriod)
	}

	// Merge short periods (< 1 minute) with surrounding periods
	periods = a.mergeShortPeriods(periods, time.Minute)

	return periods
}

func (a *Analyzer) classifyState(rttMs float64, success bool) string {
	if !success {
		return "unreachable"
	}
	switch {
	case rttMs < 300:
		return "active"
	case rttMs < 1000:
		return "screen_on"
	case rttMs < 3000:
		return "screen_off"
	default:
		return "deep_sleep"
	}
}

func (a *Analyzer) mergeShortPeriods(periods []*ActivityPeriod, minDuration time.Duration) []*ActivityPeriod {
	if len(periods) <= 1 {
		return periods
	}

	var merged []*ActivityPeriod
	for _, period := range periods {
		duration := period.End.Sub(period.Start)
		if duration < minDuration && len(merged) > 0 {
			// Merge with previous period
			merged[len(merged)-1].End = period.End
			merged[len(merged)-1].Duration = merged[len(merged)-1].End.Sub(merged[len(merged)-1].Start).String()
			merged[len(merged)-1].Transitions++
		} else {
			merged = append(merged, period)
		}
	}
	return merged
}

func (a *Analyzer) analyzeConnection(measurements []*RTTMeasurement) *ConnectionAnalysis {
	analysis := &ConnectionAnalysis{}

	var successfulRTTs []float64
	for _, m := range measurements {
		if m.Success {
			successfulRTTs = append(successfulRTTs, m.RTTMs)
		}
	}

	if len(successfulRTTs) == 0 {
		return analysis
	}

	// Calculate average response time
	var sum float64
	for _, rtt := range successfulRTTs {
		sum += rtt
	}
	analysis.AvgResponseTime = sum / float64(len(successfulRTTs))

	// WiFi vs Cellular estimation based on RTT consistency
	// WiFi typically has more consistent, lower RTTs
	// Cellular has more variation
	sort.Float64s(successfulRTTs)

	p25 := percentile(successfulRTTs, 25)
	p75 := percentile(successfulRTTs, 75)
	iqr := p75 - p25

	// Lower IQR suggests WiFi
	if iqr < 200 {
		analysis.LikelyWiFi = 70
		analysis.LikelyCellular = 30
	} else if iqr < 500 {
		analysis.LikelyWiFi = 50
		analysis.LikelyCellular = 50
	} else {
		analysis.LikelyWiFi = 30
		analysis.LikelyCellular = 70
	}

	// Connection stability - based on success rate and variance
	successRate := float64(len(successfulRTTs)) / float64(len(measurements))
	analysis.ConnectionStable = successRate > 0.95 && iqr < 300

	return analysis
}

func (a *Analyzer) calculateOverallConfidence(result *AnalysisResult, totalMeasurements int) float64 {
	// Base confidence from measurement count
	countConfidence := math.Min(float64(totalMeasurements)/1000.0, 1.0)

	// Factor in time period coverage
	duration := result.Period.End.Sub(result.Period.Start)
	durationConfidence := math.Min(duration.Hours()/24.0, 1.0) // More data over time = better

	// Factor in successful measurement rate
	successRate := 0.0
	if result.RTTStats != nil && totalMeasurements > 0 {
		successRate = float64(result.RTTStats.Count) / float64(totalMeasurements)
	}

	// Combine factors
	confidence := (countConfidence*0.3 + durationConfidence*0.3 + successRate*0.4)
	return math.Round(confidence*100) / 100
}

// Helper function to calculate percentile
func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	index := (p / 100.0) * float64(len(sorted)-1)
	lower := int(index)
	upper := lower + 1
	if upper >= len(sorted) {
		return sorted[len(sorted)-1]
	}
	weight := index - float64(lower)
	return sorted[lower]*(1-weight) + sorted[upper]*weight
}

// GenerateReport generates a human-readable report
func (a *Analyzer) GenerateReport(result *AnalysisResult) string {
	report := fmt.Sprintf(`
=======================================================
WhatsApp Activity Analysis Report
Target: %s
Period: %s to %s
Overall Confidence: %.0f%%
=======================================================

DEVICE ANALYSIS
---------------
Likely OS: %s (%.0f%% confidence)
Device Count: %d
Companion Device Detected: %v

RTT STATISTICS
--------------
Total Measurements: %d
Average RTT: %.2f ms
Median RTT: %.2f ms
Standard Deviation: %.2f ms
Range: %.2f - %.2f ms
5th Percentile: %.2f ms
95th Percentile: %.2f ms
Bimodal Distribution: %v

Screen State Averages:
  Screen On Avg:  %.2f ms
  Screen Off Avg: %.2f ms

DAILY PATTERN
-------------
Typical Wake Time: %s
Typical Sleep Time: %s
Peak Activity Hours: %v
Quiet Hours: %v

HOURLY ACTIVITY:
`,
		result.TargetJID,
		result.Period.Start.Format("2006-01-02 15:04"),
		result.Period.End.Format("2006-01-02 15:04"),
		result.OverallConfidence*100,
		result.DeviceInfo.LikelyOS, result.DeviceInfo.OSConfidence*100,
		result.DeviceInfo.DeviceCount,
		result.DeviceInfo.HasCompanion,
		result.RTTStats.Count,
		result.RTTStats.Mean,
		result.RTTStats.Median,
		result.RTTStats.StdDev,
		result.RTTStats.Min, result.RTTStats.Max,
		result.RTTStats.P5,
		result.RTTStats.P95,
		result.RTTStats.Bimodal,
		result.RTTStats.ScreenOnAvg,
		result.RTTStats.ScreenOffAvg,
		result.DailyPattern.TypicalWakeTime,
		result.DailyPattern.TypicalSleepTime,
		result.DailyPattern.PeakHours,
		result.DailyPattern.QuietHours,
	)

	// Add hourly breakdown
	for hour := 0; hour < 24; hour++ {
		stats := result.DailyPattern.HourlyActivity[hour]
		bar := ""
		barLen := int(stats.ScreenOnPercentage / 5)
		for i := 0; i < barLen; i++ {
			bar += "█"
		}
		report += fmt.Sprintf("  %02d:00 | %s %.0f%% (n=%d, avg=%.0fms)\n",
			hour, bar, stats.ScreenOnPercentage, stats.MeasurementCount, stats.AvgRTT)
	}

	// Connection analysis
	report += fmt.Sprintf(`
CONNECTION ANALYSIS
-------------------
Likely WiFi: %.0f%%
Likely Cellular: %.0f%%
Connection Stable: %v
Average Response: %.2f ms

DETECTED ACTIVITY PERIODS
-------------------------
`,
		result.ConnectionAnalysis.LikelyWiFi,
		result.ConnectionAnalysis.LikelyCellular,
		result.ConnectionAnalysis.ConnectionStable,
		result.ConnectionAnalysis.AvgResponseTime,
	)

	for _, period := range result.ActivityPeriods {
		report += fmt.Sprintf("  %s - %s: %s (%s, %.0f%% confidence)\n",
			period.Start.Format("15:04:05"),
			period.End.Format("15:04:05"),
			period.State,
			period.Duration,
			period.Confidence*100,
		)
	}

	return report
}
