// Command crosssync is the CrossSync CLI: scan, sync, and serve folders
// between peers (e.g. Unraid servers over Tailscale).
package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"crosssync/internal/certs"
	"crosssync/internal/config"
	"crosssync/internal/control"
	"crosssync/internal/events"
	"crosssync/internal/index"
	"crosssync/internal/mcp"
	"crosssync/internal/node"
)

// version is the daemon version reported in /api/status and used for
// self-update checks. It is a var so release builds can stamp it at link
// time: -ldflags "-X main.version=<semver>".
var version = "0.1.0"

// isLoopbackListen reports whether an address:port binds only the loopback
// interface, where an unauthenticated control API is acceptable.
func isLoopbackListen(addr string) bool {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

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
	case "health":
		err = cmdHealth(args)
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
  crosssync health      [--config <file>] [--addr http://127.0.0.1:55556]   (probe the running daemon; exit 0 healthy, 1 degraded)
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
	_ = fs.Parse(args)
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

// safetyInterval picks the gap before the next FULL walk. Scans are driven by
// change events; this is the safety net for events the watcher missed (queue
// overflow, a share that was unmounted and came back, a subtree added while
// unwatched). The gap is 12x the measured walk so a slow share is never walked
// back-to-back, bounded by config (scan.full_interval, scan.full_max; defaults
// 3m and 15m).
func safetyInterval(walk time.Duration, cfg *config.Config) time.Duration {
	minGap := cfg.Scan.FullIntervalOrDefault()
	maxGap := cfg.Scan.FullMaxOrDefault()
	next := 12 * walk
	if next < minGap {
		next = minGap
	}
	if next > maxGap {
		next = maxGap
	}
	// Hard floor: never spend more than half the wall clock walking. Without
	// this a folder whose walk takes longer than scan.full_max (a million files
	// on a union share can take ~15-20 min) would be re-walked the moment the
	// previous walk ends — permanent 100% duty — because the ceiling is smaller
	// than the work. The floor wins; the operator sees the numbers in the log.
	if next < 2*walk {
		next = 2 * walk
	}
	return next
}

func buildNode(cfg *config.Config) *node.Node {
	n, err := node.New(cfg)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
	return n
}

// boundServices is what cmdServe has listening before the startup index walk:
// the peer sync listener, the control API, and the service object the daemon
// keeps for self-update.
type boundServices struct {
	LN  net.Listener
	SRV *http.Server
	Svc *control.Service
}

// bindServices opens and serves the sync listener and the control API. It runs
// BEFORE the first index pass on purpose: until it returns, nothing about this
// node is reachable, and the first pass over a large share used to keep it
// unreachable for ~40 minutes (both ports silent, container "running", no
// panic) — indistinguishable from a crash, and it hid the progress the operator
// needed to see.
//
// Both sockets are bound synchronously (net.Listen + Serve, not
// ListenAndServe), so a port that cannot be bound is an error the caller can
// act on instead of a log line nobody reads while the daemon happily walks a
// share with no API.
func bindServices(n *node.Node, cfg *config.Config, cfgPath, ver string) (*boundServices, error) {
	b := &boundServices{}
	if cfg.Listen != "" {
		ln, err := n.Listen()
		if err != nil {
			return nil, err
		}
		b.LN = ln
		tlsNote := ""
		if n.TLS {
			tlsNote = " (tls)"
		}
		n.Logf("listening on %s%s", cfg.Listen, tlsNote)
		go func() { _ = n.Run(ln) }()
	}
	if cfg.ControlListen == "" {
		return b, nil
	}
	svc := control.New(n, ver)
	svc.SetConfigPath(cfgPath)
	svc.ControlToken = cfg.ControlToken
	if cfg.ControlToken == "" && !isLoopbackListen(cfg.ControlListen) {
		// Not fatal (it would break every existing deployment), but it must
		// be visible: anything that can reach this port can delete files.
		n.Logf("control API on %s has NO control_token: anyone who can reach that port can pause folders, delete files and force updates (set control_token in config.yaml to require one)", cfg.ControlListen)
	}
	svc.SetUI(cfg.UIDir, cfg.UIUpdateURL)
	svc.SetUpdate(cfg.UpdateURL, cfg.UpdateDir, cfg.UpdateAuto)
	svc.CleanupUpdateArtifacts()
	cln, err := net.Listen("tcp", cfg.ControlListen)
	if err != nil {
		_ = b.Close()
		return nil, fmt.Errorf("control API: %w", err)
	}
	srv := &http.Server{
		Handler: svc.Handler(),
		// An admin port must not be holdable open by a stalled client.
		// No WriteTimeout: /api/events/stream is a long-lived SSE socket.
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       2 * time.Minute,
		MaxHeaderBytes:    64 << 10,
	}
	b.SRV, b.Svc = srv, svc
	go func() {
		n.Logf("control API listening on %s", cfg.ControlListen)
		if err := srv.Serve(cln); err != nil && err != http.ErrServerClosed {
			n.Logf("control API: %v", err)
		}
	}()
	return b, nil
}

// Close stops serving and releases the sockets. Safe on a nil receiver and on a
// partially built value.
func (b *boundServices) Close() error {
	if b == nil {
		return nil
	}
	if b.SRV != nil {
		_ = b.SRV.Close()
	}
	if b.LN != nil {
		_ = b.LN.Close()
	}
	return nil
}

func cmdServe(args []string) error {
	fs := flag.NewFlagSet("serve", flag.ExitOnError)
	cfgPath := fs.String("config", "", "path to config.yaml")
	interval := fs.Duration("interval", 10*time.Second, "peer sync + housekeeping tick (scans are change-driven)")
	_ = fs.Parse(args)
	if *cfgPath == "" {
		return fmt.Errorf("--config is required")
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	n := buildNode(cfg)
	defer func() { _ = n.Close() }()

	n.RecordSystem(events.SevInfo, "daemon started")

	// Shutdown signal, installed BEFORE anything long-running: the startup walk
	// below is minutes to hours on a large share, and a stop request during it
	// must be acted on instead of being ignored until it finishes.
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(stop)

	// BIND FIRST, WALK AFTER. The first index pass over a large folder is
	// minutes to hours of hashing (57.8 GB on the live pair), and it used to run
	// before ANY socket was open: both nodes then looked dead from outside for
	// ~40 minutes — nothing on the sync port, nothing on the control port,
	// container "running", exit 0, no panic — which is indistinguishable from a
	// crash and left the operator unable to see progress or even pause the
	// folder. The listeners and the API now come up first; the walk runs behind
	// them.
	bs, err := bindServices(n, cfg, *cfgPath, version)
	if err != nil {
		return err
	}
	defer func() { _ = bs.Close() }()
	svc := bs.Svc

	// Arm each folder's "first pass outstanding" mark, then run the first pass
	// in the background. The mark is PER FOLDER and cleared by that folder's own
	// pass, so a small folder is advertised and synced while a huge one is still
	// hashing — one folder never delays another's scan, state or sync. While a
	// folder is marked, it is not advertised to peers (a partial index must never
	// be published) and /api/status reports startup_scanning.
	n.SetStartupScanning(true)
	walkDone := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		if err := n.ScanAllFirstPass(); err != nil {
			n.Logf("startup scan: %v", err)
		}
		walkDone <- time.Since(start)
	}()

	// Change-driven sync: a change that just became syncable should not wait for
	// the next interval tick. Coalesced, because a burst of writes needs one
	// follow-up round, not one per file.
	kick := newSyncKicker(func() { n.ConnMgr.SyncAll() })

	// Filesystem watcher: trigger scans promptly on changes. The periodic
	// tick remains the safety net for missed events. Watchers are an
	// optimization — if one cannot start, the daemon still works.
	//
	// The registry lives on the Node (see internal/node/watchers.go) so a folder
	// added at runtime gets the same watcher the boot path does, and one removed
	// at runtime loses it. Started per folder BEFORE the first pass finishes, on
	// purpose: a path scan only needs THAT folder's lock, so it queues behind
	// that folder's own walk (and re-observes anything written while the first
	// hash was running) instead of waiting for the whole startup walk — and a
	// slow folder's watcher never delays another folder's.
	//
	// The kick is injected: a change that just became syncable should not wait
	// for the next interval tick, and a burst of writes needs one follow-up round
	// rather than one per file.
	n.SetWatcherKick(kick.Kick)
	n.StartFolderWatchers()

	// Cached pending counters for the control plane (files/bytes still to pull
	// and to push). Refreshed here instead of on every /api/status request,
	// which used to cost two full index scans per poll and could block for
	// minutes while a round held the engine lock. Started before the walk
	// finishes so the first status poll of a start already carries real
	// numbers for the folders that are indexed.
	n.StartPendingRefresher(5 * time.Second)

	ticker := time.NewTicker(*interval)
	// Full-walk safety net: independent of the interval ticker, because a full
	// walk of a large folder costs far more than a peer sync attempt. The
	// schedule adapts to how long the last walk took (never less than a minute,
	// never more than 10), so a folder that takes 8s to walk is not re-walked
	// every 10s by default anymore — the watcher drives the real work and this
	// only catches events that were missed.
	//
	// Armed only when the startup walk reports its ACTUAL duration: it is
	// derived from that number, so creating it before the walk finishes would
	// schedule the safety net from a duration nobody has measured yet. fullC
	// stays nil (a case that never fires) until then; every other tick below —
	// including peer syncs — runs during the startup walk, so folders that have
	// finished their first pass sync while a slow one is still hashing.
	var fullTicker *time.Ticker
	var fullC <-chan time.Time
	defer func() {
		if fullTicker != nil {
			fullTicker.Stop()
		}
	}()
	// Held-back re-check: re-observe the files the stability gate is holding so a
	// copy that finished syncs as soon as its quiet window elapses, without
	// waiting for the next full walk.
	heldTicker := time.NewTicker(2 * time.Second)
	defer heldTicker.Stop()
	defer ticker.Stop()
	// Single-flight guard for the held-back sweep, which runs off the loop
	// goroutine because it takes folder scan locks (see the heldTicker case).
	var heldSweep atomic.Bool
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
	// Startup cleanup: orphaned staging temps from a dead process (crash or
	// kill mid-transfer), then index maintenance (acknowledged tombstones).
	//
	// In the BACKGROUND, and waited for at shutdown: both take each folder's
	// scan lock, so running them inline would park this goroutine (and with it
	// the sync ticker) behind the startup walk — one folder holding up another's
	// sync, which is exactly what must not happen.
	choresDone := make(chan struct{})
	go func() {
		defer close(choresDone)
		if cnt, err := n.CleanupStaleTemps(); err != nil {
			n.Logf("staging: stale temp cleanup: %v", err)
		} else if cnt > 0 {
			n.Logf("staging: removed %d orphaned temp file(s) from previous run", cnt)
		}
		if _, err := n.RunMaintenance(); err != nil {
			n.Logf("maintenance: %v", err)
		}
	}()
	// Runs before the deferred n.Close(): maintenance must never write to an
	// index that has already been closed.
	defer func() { <-choresDone }()
	n.Logf("crosssync %s running (interval %s, tls=%v)", version, *interval, n.TLS)
	for {
		select {
		case <-stop:
			n.Logf("shutting down")
			if fullTicker == nil {
				// Shutdown landed inside the startup walk: abandon it rather
				// than waiting out a hash that can take an hour. A cancelled
				// pass applies nothing, so the index stays exactly as the last
				// completed pass left it — and we WAIT for the walk to return,
				// because Close (deferred) must never race a scan.
				n.Logf("abandoning the startup index walk (a cancelled pass applies nothing)")
				n.AbortScans()
				<-walkDone
			}
			n.RecordSystem(events.SevInfo, "daemon stopped")
			return nil
		case walk := <-walkDone:
			gap := safetyInterval(walk, cfg)
			n.Logf("startup scan finished in %s; next full safety scan in %s", walk.Round(time.Millisecond), gap.Round(time.Second))
			fullTicker = time.NewTicker(gap)
			fullC = fullTicker.C
		case <-autoTicker.C:
			if _, err := n.RunAutoArchive(); err != nil {
				n.Logf("events: auto-archive: %v", err)
			}
			if _, err := n.RunMaintenance(); err != nil {
				n.Logf("maintenance: %v", err)
			}
		case <-heldTicker.C:
			// A copy that just finished should sync immediately, not at the next
			// interval tick. Single-flight and off the loop goroutine: the sweep
			// takes each folder's scan lock, so it would otherwise park this loop
			// — and every peer sync behind it — on a folder that is still being
			// hashed or re-walked.
			if heldSweep.CompareAndSwap(false, true) {
				go func() {
					defer heldSweep.Store(false)
					if n.ScanHeldBack() > 0 {
						kick.Kick()
					}
				}()
			}
		case <-fullC:
			start := time.Now()
			if err := n.ScanAll(); err != nil {
				n.Logf("scan: %v", err)
			}
			next := safetyInterval(time.Since(start), cfg)
			n.Logf("full safety scan took %s; next in %s", time.Since(start).Round(time.Millisecond), next)
			fullTicker.Reset(next)
		case <-ticker.C:
			// Peer syncs go through the connection manager so an offline peer
			// is retried with exponential backoff instead of every tick.
			n.ConnMgr.SyncAll()
			// Daemon self-update: throttled internally, so this is cheap
			// (only fetches the manifest every few minutes).
			if svc != nil {
				svc.MaybeAutoUpdate()
			}
			// Push-side staleness watchdog: page when a folder that used to
			// sync stops completing rounds (throttled to one page per window).
			n.CheckStaleSync()
		}
	}
}

func cmdMCP(args []string) error {
	fs := flag.NewFlagSet("mcp", flag.ExitOnError)
	cfgPath := fs.String("config", "", "path to config.yaml")
	_ = fs.Parse(args)
	if *cfgPath == "" {
		return fmt.Errorf("--config is required")
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	n := buildNode(cfg)
	defer func() { _ = n.Close() }()
	svc := control.New(n, version)
	svc.SetConfigPath(*cfgPath)
	return mcp.New(svc, os.Stdin, os.Stdout).Run(context.Background())
}

func cmdScan(args []string) error {
	fs := flag.NewFlagSet("scan", flag.ExitOnError)
	cfgPath := fs.String("config", "", "path to config.yaml")
	verify := fs.Bool("verify", false, "verified scan: re-hash every file (catches changes that preserved size and mtime, e.g. rsync -a)")
	_ = fs.Parse(args)
	if *cfgPath == "" {
		return fmt.Errorf("--config is required")
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	n := buildNode(cfg)
	defer func() { _ = n.Close() }()

	scanOne := func(id string) (int, error) {
		if *verify {
			return n.ScanFolderDeep(id)
		}
		return n.ScanFolder(id)
	}

	folderID := ""
	if rest := fs.Args(); len(rest) > 0 {
		folderID = rest[0]
	}
	if folderID == "" {
		for id := range n.Folders {
			applied, err := scanOne(id)
			if err != nil {
				return err
			}
			fmt.Printf("%s: %d change(s)\n", id, applied)
		}
		return nil
	}
	applied, err := scanOne(folderID)
	if err != nil {
		return err
	}
	fmt.Printf("%s: %d change(s)\n", folderID, applied)
	return nil
}

func cmdSync(args []string) error {
	fs := flag.NewFlagSet("sync", flag.ExitOnError)
	cfgPath := fs.String("config", "", "path to config.yaml")
	_ = fs.Parse(args)
	if *cfgPath == "" {
		return fmt.Errorf("--config is required")
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	n := buildNode(cfg)
	defer func() { _ = n.Close() }()

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
		if err != nil {
			return err
		}
		// SyncWithPeer starts one session per folder and returns once the peer
		// has answered; a one-shot command must let those transfers finish before
		// the deferred Close tears the node down.
		n.WaitSyncIdle(0)
		return nil
	}
	if err := n.SyncAllPeers(); err != nil {
		return err
	}
	n.WaitSyncIdle(0)
	return nil
}

func cmdStatus(args []string) error {
	cfg, _ := configFromArgs(args)
	n := buildNode(cfg)
	defer func() { _ = n.Close() }()

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

// cmdHealth probes the RUNNING daemon's /healthz endpoint and exits non-zero
// when it reports degraded, so a container HEALTHCHECK (or a cron job) gets the
// same verdict the daemon gives about itself — including "no sync round has
// completed in over an hour", which is the failure that otherwise goes unnoticed
// because the process is still alive and every other endpoint answers 200.
//
// It deliberately does NOT open the node (no index DBs, no listening socket, no
// lock files): it loads the config only to find control_listen, then makes one
// HTTP request. Exit status is the contract: 0 healthy, 1 degraded or
// unreachable, 2 usage error.
func cmdHealth(args []string) error {
	fs := flag.NewFlagSet("health", flag.ExitOnError)
	cfgPath := fs.String("config", "/config/config.yaml", "path to config.yaml (used only to find control_listen)")
	base := fs.String("addr", "", "control API base URL, e.g. http://127.0.0.1:55556 (default: derived from control_listen)")
	timeout := fs.Duration("timeout", 5*time.Second, "request timeout")
	_ = fs.Parse(args)

	url := strings.TrimRight(*base, "/")
	if url == "" {
		cfg, err := config.Load(*cfgPath)
		if err != nil {
			return fmt.Errorf("load %s: %w", *cfgPath, err)
		}
		if strings.TrimSpace(cfg.ControlListen) == "" {
			return fmt.Errorf("control API is disabled in %s (control_listen is empty); there is no health endpoint to probe", *cfgPath)
		}
		// A wildcard bind is reachable on loopback.
		host, port, err := net.SplitHostPort(cfg.ControlListen)
		if err != nil {
			return fmt.Errorf("control_listen %q: %w", cfg.ControlListen, err)
		}
		if host == "" || host == "0.0.0.0" || host == "::" {
			host = "127.0.0.1"
		}
		url = "http://" + net.JoinHostPort(host, port)
	}

	client := &http.Client{Timeout: *timeout}
	resp, err := client.Get(url + "/healthz")
	if err != nil {
		return fmt.Errorf("daemon is not reachable at %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, _ := io.ReadAll(io.LimitReader(resp.Body, 4096))
	trimmed := strings.TrimSpace(string(body))
	switch resp.StatusCode {
	case http.StatusOK:
		fmt.Println(trimmed)
		return nil
	case http.StatusServiceUnavailable:
		return fmt.Errorf("degraded: %s", trimmed)
	default:
		return fmt.Errorf("healthz answered %s: %s", resp.Status, trimmed)
	}
}

// cmdRebuild quarantines a folder's index and rebuilds it from the
// filesystem (the source of truth). With no folder ids it rebuilds every
// configured folder. Corruption is normally recovered automatically at
// startup; this is for forcing a regenerate (e.g. after a manual edit or a
// suspected consistency issue).
func cmdRebuild(args []string) error {
	fs := flag.NewFlagSet("rebuild", flag.ExitOnError)
	cfgPath := fs.String("config", "", "path to config.yaml")
	_ = fs.Parse(args)
	if *cfgPath == "" {
		return fmt.Errorf("--config is required")
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	n := buildNode(cfg)
	defer func() { _ = n.Close() }()

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
	_ = fs.Parse(args)
	if *cfgPath == "" {
		return fmt.Errorf("--config is required")
	}
	cfg, err := config.Load(*cfgPath)
	if err != nil {
		return err
	}
	n := buildNode(cfg)
	defer func() { _ = n.Close() }()
	if err := n.SendTestAlert(); err != nil {
		return err
	}
	fmt.Printf("test alert sent to %s\n", cfg.AlertURL)
	return nil
}

// cmdFingerprint loads (or generates) this device's TLS identity and prints
// its certificate fingerprint and derived device ID — the values to paste
// into peer configs. It uses a lenient config load (device identity +
// meta_dir only) so it can be run during initial setup, before peers have
// been given valid TLS fingerprints.
func cmdFingerprint(args []string) error {
	fs := flag.NewFlagSet("fingerprint", flag.ExitOnError)
	cfgPath := fs.String("config", "", "path to config.yaml")
	_ = fs.Parse(args)
	if *cfgPath == "" {
		return fmt.Errorf("--config is required")
	}
	cfg, err := config.LoadLenient(*cfgPath)
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
	_ = fs.Parse(args)
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
	defer func() { _ = s.Close() }()

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
	_ = fs.Parse(args)
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
	defer func() { _ = s.Close() }()
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
