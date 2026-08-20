// Command crosssync is the CrossSync CLI: scan, sync, and serve folders
// between peers (e.g. Unraid servers over Tailscale).
package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"crosssync/internal/certs"
	"crosssync/internal/config"
	"crosssync/internal/control"
	"crosssync/internal/events"
	"crosssync/internal/index"
	"crosssync/internal/mcp"
	"crosssync/internal/node"
	"crosssync/internal/watcher"
)

const version = "0.1.0"

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(1)
	}
	sub := os.Args[1]
	args := os.Args[2:]

	var err error
	switch sub {
	case "serve":
		err = cmdServe(args)
	case "mcp":
		err = cmdMCP(args)
	case "scan":
		err = cmdScan(args)
	case "sync":
		err = cmdSync(args)
	case "status":
		err = cmdStatus(args)
	case "rebuild":
		err = cmdRebuild(args)
	case "events":
		err = cmdEvents(args)
	case "ack":
		err = cmdAck(args)
	case "fingerprint":
		err = cmdFingerprint(args)
	case "alert":
		err = cmdAlert(args)
	case "version":
		fmt.Printf("crosssync %s\n", version)
		return
	case "help", "-h", "--help":
		usage()
		return
	default:
		usage()
		os.Exit(1)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func usage() {
	fmt.Print(`crosssync - bidirectional peer-to-peer file sync

Usage:
  crosssync serve       --config <file> [--interval 10s]
  crosssync mcp         --config <file>        (MCP server over stdio)
  crosssync scan        --config <file> [folder-id]
  crosssync sync        --config <file> [peer-id]
  crosssync status      --config <file>
  crosssync rebuild     --config <file> [folder-id...]   (quarantine + rebuild index from filesystem)
  crosssync events      --config <file> [--folder <id>] [--category <cat>] [--open] [--limit N]
  crosssync ack         --config <file> <event-id> [--by <name>]
  crosssync alert       --config <file> --test      (send a test alert to alert_url)
  crosssync fingerprint --config <file>
  crosssync version

Categories: applied | conflict | versioned | skipped | unsynced | error | warning | peer | system
`)
}

func configFromArgs(args []string) (*config.Config, string) {
	fs := flag.NewFlagSet("crosssync", flag.ExitOnError)
	cfgPath := fs.String("config", "", "path to config.yaml")
	fs.Parse(args)
	if *cfgPath == "" {
		fmt.Fprintln(os.Stderr, "error: --config is required")
		os.Exit(1)
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	return cfg, *cfgPath
}

func buildNode(cfg *config.Config) *node.Node {
	n, err := node.New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	return n
}

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	cfgPath := fs.String("config", "", "path to config.yaml")
	interval := fs.Duration("interval", 10*time.Second, "scan/sync interval")
	fs.Parse(args)
	if *cfgPath == "" {
		return fmt.Errorf("--config is required")
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	n := buildNode(cfg)
	defer n.Close()

	n.RecordSystem(events.SevInfo, "daemon started")

	if err := n.ScanAll(); err != nil {
		return err
	}

	// Filesystem watcher: trigger scans promptly on changes. The periodic
	// tick remains the safety net for missed events. Watchers are an
	// optimization — if one cannot start, the daemon still works.
	for id, f := range n.Folders {
		w, err := watcher.New(f.Root, n.Logf)
		if err != nil {
			n.Logf("watcher: %s: %v", id, err)
			continue
		}
		defer w.Close()
		if err := w.Start(); err != nil {
			n.Logf("watcher: %s: %v", id, err)
			continue
		}
		n.Logf("watcher: watching %s (%d dirs)", f.Root, w.WatchCount())
		go func() {
			for range w.Changes() {
				if err := n.ScanAll(); err != nil {
					n.Logf("watcher scan: %v", err)
				}
			}
		}()
	}

	if cfg.Listen != "" {
		ln, err := n.Listen()
		if err != nil {
			return err
		}
		defer ln.Close()
		tlsNote := ""
		if n.TLS {
			tlsNote = " (tls)"
		}
		n.Logf("listening on %s%s", cfg.Listen, tlsNote)
		go n.Run(ln)
	}

	// Control plane: REST API + SSE (serves the same durable event store
	// the CLI queries).
	if cfg.ControlListen != "" {
		svc := control.New(n, version)
		svc.SetConfigPath(*cfgPath)
		svc.SetUI(cfg.UIDir, cfg.UIUpdateURL)
		srv := &http.Server{Addr: cfg.ControlListen, Handler: svc.Handler()}
		go func() {
			n.Logf("control API listening on %s", cfg.ControlListen)
			if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
				n.Logf("control API: %v", err)
			}
		}()
		defer srv.Close()
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	ticker := time.NewTicker(*interval)
	defer ticker.Stop()
	// Event-history auto-archive: prune routine logs older than the
	// configured age once at startup and then daily. Open attention /
	// conflict events are never pruned by this path.
	if deleted, err := n.RunAutoArchive(); err != nil {
		n.Logf("events: auto-archive: %v", err)
	} else if deleted > 0 {
		n.Logf("events: auto-archive ran at startup, removed %d event(s)", deleted)
	}
	autoTicker := time.NewTicker(24 * time.Hour)
	defer autoTicker.Stop()
	n.Logf("crosssync %s running (interval %s, tls=%v)", version, *interval, n.TLS)
	for {
		select {
		case <-stop:
			n.Logf("shutting down")
			n.RecordSystem(events.SevInfo, "daemon stopped")
			return nil
		case <-autoTicker.C:
			if _, err := n.RunAutoArchive(); err != nil {
				n.Logf("events: auto-archive: %v", err)
			}
		case <-ticker.C:
			if err := n.ScanAll(); err != nil {
				n.Logf("scan: %v", err)
			}
			// Peer syncs go through the connection manager so an offline peer
			// is retried with exponential backoff instead of every tick.
			n.ConnMgr.SyncAll()
		}
	}
}

func cmdMCP(args []string) error {
	fs := flag.NewFlagSet("mcp", flag.ExitOnError)
	cfgPath := fs.String("config", "", "path to config.yaml")
	fs.Parse(args)
	if *cfgPath == "" {
		return fmt.Errorf("--config is required")
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	n := buildNode(cfg)
	defer n.Close()
	svc := control.New(n, version)
	svc.SetConfigPath(*cfgPath)
	return mcp.New(svc, os.Stdin, os.Stdout).Run(context.Background())
}

func cmdScan(args []string) error {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	cfgPath := fs.String("config", "", "path to config.yaml")
	fs.Parse(args)
	if *cfgPath == "" {
		return fmt.Errorf("--config is required")
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	n := buildNode(cfg)
	defer n.Close()

	folderID := ""
	if rest := fs.Args(); len(rest) > 0 {
		folderID = rest[0]
	}
	if folderID == "" {
		for id := range n.Folders {
			applied, err := n.ScanFolder(id)
			if err != nil {
				return err
			}
			fmt.Printf("%s: %d change(s)\n", id, applied)
		}
		return nil
	}
	applied, err := n.ScanFolder(folderID)
	if err != nil {
		return err
	}
	fmt.Printf("%s: %d change(s)\n", folderID, applied)
	return nil
}

func cmdSync(args []string) error {
	fs := flag.NewFlagSet("sync", flag.ExitOnError)
	cfgPath := fs.String("config", "", "path to config.yaml")
	fs.Parse(args)
	if *cfgPath == "" {
		return fmt.Errorf("--config is required")
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	n := buildNode(cfg)
	defer n.Close()

	// Always rescan before syncing so local changes are indexed first.
	if err := n.ScanAll(); err != nil {
		return err
	}

	peerArg := ""
	if rest := fs.Args(); len(rest) > 0 {
		peerArg = rest[0]
	}
	if peerArg != "" {
		var pid uint64
		if _, err := fmt.Sscanf(peerArg, "%d", &pid); err != nil {
			return fmt.Errorf("invalid peer id %q", peerArg)
		}
		_, err := n.SyncWithPeer(pid)
		return err
	}
	return n.SyncAllPeers()
}

func cmdStatus(args []string) error {
	cfg, _ := configFromArgs(args)
	n := buildNode(cfg)
	defer n.Close()

	fmt.Printf("device: %s (id %d)\n", cfg.Device.Name, cfg.Device.ID)
	if fp := n.Fingerprint(); fp != "" {
		fmt.Printf("tls: enabled\n")
		fmt.Printf("fingerprint: %s\n", fp)
	} else {
		fmt.Printf("tls: disabled\n")
	}
	if open, err := n.Events.CountOpen(); err == nil {
		fmt.Printf("open events: %d (crosssync events --open to list)\n", open)
	}
	for _, p := range cfg.Peers {
		pinned := ""
		if p.Fingerprint != "" {
			pinned = " pinned"
		}
		fmt.Printf("peer: %s (id %d) %s%s\n", p.Name, p.ID, strings.Join(p.Addresses, ", "), pinned)
	}
	for id, f := range n.Folders {
		files, deleted := 0, 0
		_ = f.Ix.List(func(fi *index.FileInfo) error {
			files++
			return nil
		})
		_ = f.Ix.ListAll(func(fi *index.FileInfo) error {
			if fi.Deleted {
				deleted++
			}
			return nil
		})
		fmt.Printf("folder %s: %s (%d files, %d tombstones)\n", id, f.Root, files, deleted)
	}
	return nil
}

// cmdRebuild quarantines a folder's index and rebuilds it from the
// filesystem (the source of truth). With no folder ids it rebuilds every
// configured folder. Corruption is normally recovered automatically at
// startup; this is for forcing a regenerate (e.g. after a manual edit or a
// suspected consistency issue).
func cmdRebuild(args []string) error {
	fs := flag.NewFlagSet("rebuild", flag.ExitOnError)
	cfgPath := fs.String("config", "", "path to config.yaml")
	fs.Parse(args)
	if *cfgPath == "" {
		return fmt.Errorf("--config is required")
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	n := buildNode(cfg)
	defer n.Close()

	ids := fs.Args()
	if len(ids) == 0 {
		for id := range n.Folders {
			ids = append(ids, id)
		}
		sort.Strings(ids)
	}
	for _, id := range ids {
		applied, err := n.RebuildFolder(id)
		if err != nil {
			return fmt.Errorf("folder %s: %w", id, err)
		}
		fmt.Printf("rebuilt %s: %d change(s) from filesystem\n", id, applied)
	}
	return nil
}

// cmdAlert sends a test notification through the configured alert_url so an
// operator can verify the endpoint before relying on it.
func cmdAlert(args []string) error {
	fs := flag.NewFlagSet("alert", flag.ExitOnError)
	cfgPath := fs.String("config", "", "path to config.yaml")
	fs.Parse(args)
	if *cfgPath == "" {
		return fmt.Errorf("--config is required")
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	n := buildNode(cfg)
	defer n.Close()
	if err := n.SendTestAlert(); err != nil {
		return err
	}
	fmt.Printf("test alert sent to %s\n", cfg.AlertURL)
	return nil
}

// cmdFingerprint loads (or generates) this device's TLS identity and prints
// its certificate fingerprint and derived device ID — the values to paste
// into peer configs. Unlike other subcommands it does not need any folders
// to exist yet, so it can be run during initial setup.
func cmdFingerprint(args []string) error {
	fs := flag.NewFlagSet("fingerprint", flag.ExitOnError)
	cfgPath := fs.String("config", "", "path to config.yaml")
	fs.Parse(args)
	if *cfgPath == "" {
		return fmt.Errorf("--config is required")
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	cm, err := certs.LoadOrCreate(filepath.Join(cfg.MetaDir, "key.pem"),
		filepath.Join(cfg.MetaDir, "cert.pem"), cfg.Device.Name)
	if err != nil {
		return err
	}
	fmt.Printf("device:      %s\n", cfg.Device.Name)
	fmt.Printf("device id:   %d\n", cm.DeviceID())
	fmt.Printf("fingerprint: %s\n", cm.Fingerprint())
	fmt.Println("add this fingerprint to peers[] on every other node")
	return nil
}

// openEventStore opens the durable event store without requiring any
// folders to exist (used by events/ack, which only need the event DB).
func openEventStore(cfg *config.Config) (*events.Store, error) {
	if err := os.MkdirAll(cfg.MetaDir, 0o755); err != nil {
		return nil, err
	}
	return events.Open(filepath.Join(cfg.MetaDir, "events.db"))
}

func cmdEvents(args []string) error {
	fs := flag.NewFlagSet("events", flag.ExitOnError)
	cfgPath := fs.String("config", "", "path to config.yaml")
	folder := fs.String("folder", "", "filter by folder id")
	category := fs.String("category", "", "filter by category")
	limit := fs.Int("limit", 50, "maximum events to show")
	open := fs.Bool("open", false, "only events needing attention")
	peer := fs.Uint64("peer", 0, "filter by peer device id")
	fs.Parse(args)
	if *cfgPath == "" {
		return fmt.Errorf("--config is required")
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	s, err := openEventStore(cfg)
	if err != nil {
		return err
	}
	defer s.Close()

	peerName := ""
	for _, p := range cfg.Peers {
		if p.ID == *peer {
			peerName = p.Name
		}
	}
	evs, err := s.Query(events.Filter{
		Folder:   *folder,
		Category: events.Category(*category),
		Limit:    *limit,
		OpenOnly: *open,
		PeerID:   *peer,
		PeerName: peerName,
	})
	if err != nil {
		return err
	}
	if len(evs) == 0 {
		fmt.Println("no events")
		return nil
	}
	for _, e := range evs {
		who := ""
		if e.PeerID != 0 {
			who = " (from " + peerDisplayName(cfg, e.PeerID) + ")"
		}
		fmt.Printf("%d  %s  %-11s %-9s %-13s %s/%s: %s%s\n",
			e.ID, e.TS.Format("2006-01-02 15:04:05"),
			categoryLabel(e.Category), sevString(e.Severity), resString(e.Resolution),
			e.Folder, e.Path, e.Reason, who)
	}
	return nil
}

// categoryLabel maps internal categories to friendly display labels.
func categoryLabel(c events.Category) string {
	switch c {
	case events.CatApplied:
		return "synced"
	default:
		return string(c)
	}
}

// peerDisplayName resolves a device id to a configured peer/self name.
func peerDisplayName(cfg *config.Config, id uint64) string {
	if id == cfg.Device.ID {
		return cfg.Device.Name + " (this server)"
	}
	for _, p := range cfg.Peers {
		if p.ID == id {
			return p.Name
		}
	}
	return fmt.Sprintf("device %d", id)
}

func cmdAck(args []string) error {
	fs := flag.NewFlagSet("ack", flag.ExitOnError)
	cfgPath := fs.String("config", "", "path to config.yaml")
	by := fs.String("by", "", "who is acknowledging (default: device name)")
	fs.Parse(args)
	if *cfgPath == "" {
		return fmt.Errorf("--config is required")
	}
	rest := fs.Args()
	if len(rest) < 1 {
		return fmt.Errorf("ack requires an event id")
	}
	var id int64
	if _, err := fmt.Sscanf(rest[0], "%d", &id); err != nil {
		return fmt.Errorf("invalid event id %q", rest[0])
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	s, err := openEventStore(cfg)
	if err != nil {
		return err
	}
	defer s.Close()
	who := *by
	if who == "" {
		who = cfg.Device.Name
	}
	if err := s.Acknowledge(id, who); err != nil {
		return err
	}
	fmt.Printf("acknowledged event %d by %s (record kept; re-opens if the condition persists)\n", id, who)
	return nil
}

func sevString(s events.Severity) string {
	switch s {
	case events.SevError:
		return "error"
	case events.SevWarn:
		return "warn"
	default:
		return "info"
	}
}

func resString(r events.Resolution) string {
	switch r {
	case events.ResAcknowledged:
		return "acknowledged"
	case events.ResResolved:
		return "resolved"
	case events.ResAutoResolved:
		return "auto-resolved"
	default:
		return "open"
	}
}
