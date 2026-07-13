// WhatsApp Mapper - RTT-based device activity monitoring tool
// Based on the "Careless Whisper" research paper (arXiv:2411.11194)
// For authorized OSINT research only - use responsibly on company-owned devices
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "github.com/mattn/go-sqlite3"

	whatsmeow "go.mau.fi/whatsmeow"
	"go.mau.fi/whatsmeow/mapper"
	"go.mau.fi/whatsmeow/store/sqlstore"
	waLog "go.mau.fi/whatsmeow/util/log"
)

var (
	// Connection flags
	dbPath   = flag.String("db", "mapper.db", "Path to WhatsApp session database")
	mapperDB = flag.String("mapper-db", "rtt_data.db", "Path to RTT measurements database")
	logLevel = flag.String("log", "INFO", "Log level (DEBUG, INFO, WARN, ERROR)")

	// Mode flags
	mode = flag.String("mode", "probe", "Operating mode: probe, analyze, export, qr")

	// Probe flags
	target    = flag.String("target", "", "Target phone number (with country code, no +)")
	interval  = flag.Duration("interval", 2*time.Second, "Probe interval")
	duration  = flag.Duration("duration", 0, "Probe duration (0 = unlimited)")
	maxProbes = flag.Int("max-probes", 0, "Maximum number of probes (0 = unlimited)")
	probeType = flag.String("probe-type", "reaction", "Probe type: reaction, receipt, presence")

	// Analysis flags
	outputJSON = flag.Bool("json", false, "Output analysis as JSON")
	exportCSV  = flag.String("export-csv", "", "Export measurements to CSV file")

	// Info flags
	showHelp = flag.Bool("help", false, "Show help message")
	version  = flag.Bool("version", false, "Show version")
)

const Version = "1.0.0"

func main() {
	flag.Parse()

	if *showHelp {
		printHelp()
		return
	}

	if *version {
		fmt.Printf("WhatsApp Mapper v%s\n", Version)
		fmt.Println("Based on 'Careless Whisper' research (arXiv:2411.11194)")
		return
	}

	// Setup logging
	log := waLog.Stdout("WAMapper", *logLevel, true)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle interrupt signals
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Infof("Received interrupt signal, shutting down...")
		cancel()
	}()

	switch *mode {
	case "qr":
		if err := runQRMode(ctx, log); err != nil {
			log.Errorf("QR mode failed: %v", err)
			os.Exit(1)
		}
	case "probe":
		if err := runProbeMode(ctx, log); err != nil {
			log.Errorf("Probe mode failed: %v", err)
			os.Exit(1)
		}
	case "analyze":
		if err := runAnalyzeMode(ctx, log); err != nil {
			log.Errorf("Analyze mode failed: %v", err)
			os.Exit(1)
		}
	case "export":
		if err := runExportMode(ctx, log); err != nil {
			log.Errorf("Export mode failed: %v", err)
			os.Exit(1)
		}
	default:
		log.Errorf("Unknown mode: %s", *mode)
		os.Exit(1)
	}
}

func printHelp() {
	fmt.Print(`
WhatsApp Mapper - RTT-based Device Activity Monitoring
=======================================================
Based on "Careless Whisper" research paper (arXiv:2411.11194)

⚠️  FOR AUTHORIZED OSINT RESEARCH ONLY ⚠️
Only use on company-owned devices with proper authorization!

USAGE:
  wamapper [flags]

MODES:
  qr       - Display QR code for WhatsApp Web pairing
  probe    - Start probing a target phone number
  analyze  - Analyze collected RTT data
  export   - Export measurements to CSV

CONNECTION FLAGS:
  -db string
        Path to WhatsApp session database (default "mapper.db")
  -mapper-db string
        Path to RTT measurements database (default "rtt_data.db")
  -log string
        Log level: DEBUG, INFO, WARN, ERROR (default "INFO")

PROBE FLAGS:
  -target string
        Target phone number (with country code, no +)
        Example: 14155551234
  -interval duration
        Time between probes (default 2s)
        Recommended: 2s for detailed, 20s-60s for long-term
  -duration duration
        How long to probe (default 0 = unlimited)
  -max-probes int
        Maximum number of probes (default 0 = unlimited)
  -probe-type string
        Probe method: reaction, receipt, presence (default "reaction")

ANALYSIS FLAGS:
  -json
        Output analysis results as JSON
  -export-csv string
        Export measurements to specified CSV file

EXAMPLES:
  # First time setup - scan QR code
  wamapper -mode qr

  # Probe target for 1 hour with 2-second intervals
  wamapper -mode probe -target 14155551234 -duration 1h

  # Long-term monitoring (24 hours, 30-second intervals)
  wamapper -mode probe -target 14155551234 -duration 24h -interval 30s

  # Analyze collected data
  wamapper -mode analyze -target 14155551234

  # Export data for visualization
  wamapper -mode export -target 14155551234 -export-csv measurements.csv

PROBE TYPES:
  reaction  - Uses reactions to fake messages (stealthy, recommended)
  receipt   - Uses read receipts (less reliable)
  presence  - Subscribes to presence updates

RTT INTERPRETATION (from the research paper):
  < 300ms   - App is in foreground (actively using WhatsApp)
  < 1000ms  - Screen is on (phone is being used)
  > 1000ms  - Screen is off (phone in pocket/on table)
  > 3000ms  - Doze mode or power saving active
  Timeout   - Device offline or no data connection

For more information, see: https://arxiv.org/abs/2411.11194
`)
}

func runQRMode(ctx context.Context, log waLog.Logger) error {
	log.Infof("Starting QR code pairing mode...")

	// Initialize WhatsApp store
	container, err := sqlstore.New(ctx, "sqlite3", "file:"+*dbPath+"?_foreign_keys=on", log.Sub("Store"))
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer container.Close()

	device, err := container.GetFirstDevice(ctx)
	if err != nil {
		return fmt.Errorf("failed to get device: %w", err)
	}

	client := whatsmeow.NewClient(device, log.Sub("Client"))

	if client.Store.ID != nil {
		log.Infof("Already logged in as %s", client.Store.ID.String())
		return nil
	}

	// Get QR channel
	qrChan, _ := client.GetQRChannel(ctx)

	if err := client.Connect(); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}

	for evt := range qrChan {
		switch evt.Event {
		case "code":
			fmt.Println("\n" + strings.Repeat("=", 60))
			fmt.Println("Scan this QR code with WhatsApp (Linked Devices)")
			fmt.Println(strings.Repeat("=", 60))
			printQR(evt.Code)
			fmt.Println(strings.Repeat("=", 60) + "\n")
		case "success":
			log.Infof("Successfully paired!")
			return nil
		case "timeout":
			log.Warnf("QR code timeout, generating new code...")
		}
	}

	return nil
}

func runProbeMode(ctx context.Context, log waLog.Logger) error {
	if *target == "" {
		return fmt.Errorf("target phone number is required (-target flag)")
	}

	log.Infof("Starting probe mode for target: %s", *target)

	// Initialize WhatsApp store
	container, err := sqlstore.New(ctx, "sqlite3", "file:"+*dbPath+"?_foreign_keys=on", log.Sub("Store"))
	if err != nil {
		return fmt.Errorf("failed to open database: %w", err)
	}
	defer container.Close()

	device, err := container.GetFirstDevice(ctx)
	if err != nil {
		return fmt.Errorf("failed to get device: %w", err)
	}

	client := whatsmeow.NewClient(device, log.Sub("Client"))

	if client.Store.ID == nil {
		return fmt.Errorf("not logged in - run with -mode qr first")
	}

	// Initialize mapper store
	mapperDatabase, err := sql.Open("sqlite3", "file:"+*mapperDB+"?_foreign_keys=on")
	if err != nil {
		return fmt.Errorf("failed to open mapper database: %w", err)
	}
	defer mapperDatabase.Close()

	store, err := mapper.NewMapperStore(mapperDatabase)
	if err != nil {
		return fmt.Errorf("failed to initialize mapper store: %w", err)
	}

	// Connect to WhatsApp
	if err := client.Connect(); err != nil {
		return fmt.Errorf("failed to connect: %w", err)
	}
	defer client.Disconnect()

	// Wait for connection
	if !client.WaitForConnection(30 * time.Second) {
		return fmt.Errorf("connection timeout")
	}

	log.Infof("Connected as %s", client.Store.ID.String())

	// Create prober
	prober := mapper.NewProber(client, store, log.Sub("Prober"))

	// Configure probe
	config := mapper.ProbeConfig{
		TargetPhone:   *target,
		ProbeInterval: *interval,
		ProbeType:     mapper.ProbeType(*probeType),
		MaxProbes:     *maxProbes,
		Duration:      *duration,
		StoreResults:  true,
	}

	log.Infof("Starting probes with config: interval=%v, type=%s", config.ProbeInterval, config.ProbeType)
	if config.Duration > 0 {
		log.Infof("Will run for %v", config.Duration)
	}
	if config.MaxProbes > 0 {
		log.Infof("Will send up to %d probes", config.MaxProbes)
	}

	// Start cleanup goroutine
	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				prober.CleanupOldProbes(30 * time.Second)
			}
		}
	}()

	// Run probing
	if err := prober.Start(ctx, config); err != nil && ctx.Err() == nil {
		return fmt.Errorf("probing failed: %w", err)
	}

	total, successful := prober.GetStats()
	log.Infof("Probing complete. Total: %d, Successful: %d (%.1f%%)",
		total, successful, float64(successful)/float64(total)*100)

	return nil
}

func runAnalyzeMode(ctx context.Context, log waLog.Logger) error {
	if *target == "" {
		return fmt.Errorf("target phone number is required (-target flag)")
	}

	log.Infof("Starting analysis for target: %s", *target)

	// Open mapper database
	mapperDatabase, err := sql.Open("sqlite3", "file:"+*mapperDB+"?_foreign_keys=on")
	if err != nil {
		return fmt.Errorf("failed to open mapper database: %w", err)
	}
	defer mapperDatabase.Close()

	store, err := mapper.NewMapperStore(mapperDatabase)
	if err != nil {
		return fmt.Errorf("failed to initialize mapper store: %w", err)
	}

	// Create analyzer
	analyzer := mapper.NewAnalyzer(store)

	// Format target JID
	targetJID := *target + "@s.whatsapp.net"

	// Run analysis
	result, err := analyzer.Analyze(ctx, targetJID)
	if err != nil {
		return fmt.Errorf("analysis failed: %w", err)
	}

	// Output results
	if *outputJSON {
		jsonData, err := json.MarshalIndent(result, "", "  ")
		if err != nil {
			return fmt.Errorf("failed to marshal JSON: %w", err)
		}
		fmt.Println(string(jsonData))
	} else {
		report := analyzer.GenerateReport(result)
		fmt.Println(report)
	}

	return nil
}

func runExportMode(ctx context.Context, log waLog.Logger) error {
	if *target == "" {
		return fmt.Errorf("target phone number is required (-target flag)")
	}

	outputPath := *exportCSV
	if outputPath == "" {
		outputPath = fmt.Sprintf("%s_measurements.csv", *target)
	}

	log.Infof("Exporting measurements for %s to %s", *target, outputPath)

	// Open mapper database
	mapperDatabase, err := sql.Open("sqlite3", "file:"+*mapperDB+"?_foreign_keys=on")
	if err != nil {
		return fmt.Errorf("failed to open mapper database: %w", err)
	}
	defer mapperDatabase.Close()

	store, err := mapper.NewMapperStore(mapperDatabase)
	if err != nil {
		return fmt.Errorf("failed to initialize mapper store: %w", err)
	}

	// Export to CSV
	targetJID := *target + "@s.whatsapp.net"
	csv, err := store.ExportToCSV(ctx, targetJID)
	if err != nil {
		return fmt.Errorf("failed to export: %w", err)
	}

	if err := os.WriteFile(outputPath, []byte(csv), 0644); err != nil {
		return fmt.Errorf("failed to write file: %w", err)
	}

	log.Infof("Exported measurements to %s", outputPath)

	// Also print stats
	stats, err := store.GetStats(ctx, targetJID)
	if err == nil {
		log.Infof("Statistics:")
		for k, v := range stats {
			log.Infof("  %s: %v", k, v)
		}
	}

	return nil
}

// printQR prints a QR code to terminal
func printQR(code string) {
	// Simple ASCII QR code representation
	// In practice, you'd use a proper QR library
	fmt.Println("\nQR Code (scan with WhatsApp):")
	fmt.Println(code)
	fmt.Println("\nOr visit: https://web.whatsapp.com and use this code")
}
