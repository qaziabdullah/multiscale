package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

// ---------------------------------------------------------------------------
// Paths & constants
// ---------------------------------------------------------------------------

// tailscaled.exe and tailscale.exe are expected to sit in the SAME folder as
// TS-Manager.exe. Resolved at startup via os.Executable().
var (
	exeDir         string
	TailscaledPath string
	TailscalePath  string
)

const (
	listenAddr      = "127.0.0.1:8080"
	logRingCapacity = 500 // lines of daemon output kept per node
	statusTimeout   = 8 * time.Second
)

// nameRe restricts node names to characters safe for pipe names and dir paths.
var nameRe = regexp.MustCompile(`^[A-Za-z0-9_-]{1,32}$`)

// ---------------------------------------------------------------------------
// Data model
// ---------------------------------------------------------------------------

type InstanceConfig struct {
	Name      string `json:"name"`
	SocksPort int    `json:"socksPort"`
	AuthKey   string `json:"authKey"`
	ExitNode  string `json:"exitNode"`
}

// runtimeNode holds everything about a node that is NOT persisted.
type runtimeNode struct {
	cmd     *exec.Cmd
	logBuf  *ringBuffer
	state   string // Stopped, Starting, Running, NeedsLogin, Error
	lastErr string
}

var (
	config    []InstanceConfig
	runtime   = make(map[string]*runtimeNode)
	mu        sync.Mutex // protects config and runtime
	cfgPath   = "config.json"
	shuttingD bool
)

// ---------------------------------------------------------------------------
// Ring buffer for per-node log capture
// ---------------------------------------------------------------------------

type ringBuffer struct {
	mu    sync.Mutex
	lines []string
	cap   int
}

func newRingBuffer(capacity int) *ringBuffer {
	return &ringBuffer{cap: capacity, lines: make([]string, 0, capacity)}
}

func (r *ringBuffer) add(line string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.lines = append(r.lines, line)
	if len(r.lines) > r.cap {
		r.lines = r.lines[len(r.lines)-r.cap:]
	}
}

func (r *ringBuffer) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.lines))
	copy(out, r.lines)
	return out
}

// captureWriter implements io.Writer, splitting on newlines into the ring.
type captureWriter struct {
	ring   *ringBuffer
	prefix string
	buf    strings.Builder
}

func (c *captureWriter) Write(p []byte) (int, error) {
	c.buf.Write(p)
	s := c.buf.String()
	for {
		i := strings.IndexByte(s, '\n')
		if i < 0 {
			break
		}
		line := strings.TrimRight(s[:i], "\r")
		c.ring.add(time.Now().Format("15:04:05") + " " + c.prefix + line)
		s = s[i+1:]
	}
	c.buf.Reset()
	c.buf.WriteString(s)
	return len(p), nil
}

// ---------------------------------------------------------------------------
// main
// ---------------------------------------------------------------------------

func main() {
	resolvePaths()
	loadConfig()

	// Graceful shutdown: stop all daemons we spawned.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sig
		fmt.Println("\nShutting down: stopping all daemons...")
		mu.Lock()
		shuttingD = true
		for name := range runtime {
			stopNodeLocked(name)
		}
		mu.Unlock()
		os.Exit(0)
	}()

	http.HandleFunc("/", handleDashboard)
	http.HandleFunc("/api/nodes", handleNodes)              // GET list, POST add
	http.HandleFunc("/api/node/", handleNodeItem)           // PUT/DELETE/start/stop/status/logs/exit-nodes/set-exit-node/check-ip
	http.HandleFunc("/api/preflight", handlePreflight)      // full preflight checklist

	fmt.Printf("TS-Manager active at http://%s\n", listenAddr)
	if !isElevated() {
		fmt.Println("WARNING: not running as Administrator. Daemons will fail to create pipes.")
		fmt.Println("         Relaunch TS-Manager.exe via right-click > Run as administrator.")
	}
	exec.Command("rundll32", "url.dll,FileProtocolHandler", "http://"+listenAddr).Start()
	http.ListenAndServe(listenAddr, nil)
}

// ---------------------------------------------------------------------------
// Setup helpers
// ---------------------------------------------------------------------------

func resolvePaths() {
	exe, err := os.Executable()
	if err != nil {
		exeDir, _ = os.Getwd()
	} else {
		exeDir = filepath.Dir(exe)
	}
	TailscaledPath = filepath.Join(exeDir, "tailscaled.exe")
	TailscalePath = filepath.Join(exeDir, "tailscale.exe")
	cfgPath = filepath.Join(exeDir, "config.json")
}

// isElevated reports whether the process has Administrator rights, by
// attempting to open a handle that requires elevation.
func isElevated() bool {
	f, err := os.Open(`\\.\PHYSICALDRIVE0`)
	if err == nil {
		f.Close()
		return true
	}
	return false
}

func pipeName(name string) string { return fmt.Sprintf(`\\.\pipe\ts_%s`, name) }
func stateDir(name string) string {
	return filepath.Join(os.Getenv("LOCALAPPDATA"), "TSMulti", name)
}

// ---------------------------------------------------------------------------
// Config persistence
// ---------------------------------------------------------------------------

func loadConfig() {
	file, err := os.ReadFile(cfgPath)
	if err != nil {
		fmt.Println("config.json not found; starting with an empty node list.")
		config = []InstanceConfig{}
		return
	}
	if err := json.Unmarshal(file, &config); err != nil {
		fmt.Printf("FAILED to parse config.json: %v\n", err)
		fmt.Println("Fix the JSON (top-level array, no trailing comma, no BOM) and restart.")
		config = []InstanceConfig{}
		return
	}
	fmt.Printf("Loaded %d node(s) from config.json.\n", len(config))
}

// saveConfig assumes the caller holds mu.
func saveConfig() {
	data, err := json.MarshalIndent(config, "", "  ")
	if err != nil {
		fmt.Printf("Error marshalling config: %v\n", err)
		return
	}
	if err := os.WriteFile(cfgPath, data, 0644); err != nil {
		fmt.Printf("Error writing config.json: %v\n", err)
	}
}

// ---------------------------------------------------------------------------
// Node lifecycle
// ---------------------------------------------------------------------------

// startNodeLocked starts the daemon for inst. Caller holds mu.
func startNodeLocked(inst *InstanceConfig) error {
	if _, err := os.Stat(TailscaledPath); err != nil {
		return fmt.Errorf("tailscaled.exe not found next to TS-Manager.exe")
	}
	if _, err := os.Stat(TailscalePath); err != nil {
		return fmt.Errorf("tailscale.exe not found next to TS-Manager.exe")
	}

	sd := stateDir(inst.Name)
	os.MkdirAll(sd, 0755)
	pn := pipeName(inst.Name)
	socks := fmt.Sprintf("127.0.0.1:%d", inst.SocksPort)

	rn := runtime[inst.Name]
	if rn == nil {
		rn = &runtimeNode{logBuf: newRingBuffer(logRingCapacity)}
		runtime[inst.Name] = rn
	}
	rn.state = "Starting"
	rn.lastErr = ""

	cmd := exec.Command(TailscaledPath,
		"--tun=userspace-networking",
		"--socks5-server="+socks,
		"--state="+filepath.Join(sd, "tailscaled.state"),
		"--socket="+pn,
	)
	// CREATE_NEW_PROCESS_GROUP gives tailscaled its own process group and,
	// critically, a fresh security context so it can own its named pipe
	// without hitting "This security ID may not be assigned as the owner".
	cmd.SysProcAttr = &syscall.SysProcAttr{
		HideWindow:    true,
		CreationFlags: 0x00000200, // CREATE_NEW_PROCESS_GROUP
	}
	cmd.Stdout = &captureWriter{ring: rn.logBuf, prefix: "[daemon] "}
	cmd.Stderr = &captureWriter{ring: rn.logBuf, prefix: "[daemon] "}

	if err := cmd.Start(); err != nil {
		rn.state = "Error"
		rn.lastErr = err.Error()
		return err
	}
	rn.cmd = cmd
	rn.logBuf.add(time.Now().Format("15:04:05") + fmt.Sprintf(" [mgr] tailscaled started (pid %d)", cmd.Process.Pid))

	// Background: wait for pipe, then run `tailscale up`.
	go authenticateNode(inst.Name, pn, inst.AuthKey, inst.ExitNode)
	return nil
}

func authenticateNode(name, pn, key, exit string) {
	mu.Lock()
	rn := runtime[name]
	mu.Unlock()
	if rn == nil {
		return
	}

	// Poll for the named pipe.
	ready := false
	for i := 0; i < 40; i++ {
		if _, err := os.Stat(pn); err == nil {
			ready = true
			break
		}
		time.Sleep(500 * time.Millisecond)
	}
	if !ready {
		mu.Lock()
		rn.state = "Error"
		rn.lastErr = "pipe never appeared (daemon failed to start)"
		mu.Unlock()
		rn.logBuf.add(time.Now().Format("15:04:05") + " [mgr] ERROR: pipe never appeared")
		return
	}
	rn.logBuf.add(time.Now().Format("15:04:05") + " [mgr] pipe ready, running tailscale up")

	args := []string{
		"--socket=" + pn, "up",
		"--auth-key=" + key,
		"--accept-routes=false",
		"--accept-dns=false",
		"--unattended",
	}
	if exit != "" {
		args = append(args, "--exit-node="+exit)
	}
	cmd := exec.Command(TailscalePath, args...)
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CreationFlags: 0x00000200}
	out, err := cmd.CombinedOutput()
	for _, ln := range strings.Split(strings.TrimRight(string(out), "\n"), "\n") {
		if ln != "" {
			rn.logBuf.add(time.Now().Format("15:04:05") + " [up] " + ln)
		}
	}
	mu.Lock()
	if err != nil {
		rn.state = "Error"
		rn.lastErr = "tailscale up failed: " + strings.TrimSpace(string(out))
	} else {
		rn.state = "Running"
	}
	mu.Unlock()
}

// stopNodeLocked stops the daemon for name. Caller holds mu.
func stopNodeLocked(name string) {
	rn := runtime[name]
	if rn == nil || rn.cmd == nil || rn.cmd.Process == nil {
		if rn != nil {
			rn.state = "Stopped"
		}
		return
	}
	pid := rn.cmd.Process.Pid
	rn.logBuf.add(time.Now().Format("15:04:05") + fmt.Sprintf(" [mgr] stopping (pid %d)", pid))
	kill := exec.Command("taskkill", "/F", "/T", "/PID", fmt.Sprintf("%d", pid))
	kill.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CreationFlags: 0x00000200}
	if out, err := kill.CombinedOutput(); err != nil {
		rn.cmd.Process.Kill() // fallback
		rn.logBuf.add(time.Now().Format("15:04:05") + " [mgr] taskkill: " + strings.TrimSpace(string(out)))
	}
	rn.cmd = nil
	rn.state = "Stopped"
}

// liveStatus runs `tailscale status` against a node's pipe and returns a
// short summary line. Best-effort; used to refine the displayed state.
func liveStatus(name string) string {
	cmd := exec.Command(TailscalePath, "--socket="+pipeName(name), "status")
	cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CreationFlags: 0x00000200}
	done := make(chan struct{})
	var out []byte
	go func() { out, _ = cmd.CombinedOutput(); close(done) }()
	select {
	case <-done:
	case <-time.After(statusTimeout):
		if cmd.Process != nil {
			cmd.Process.Kill()
		}
		return "status timed out"
	}
	return strings.TrimSpace(string(out))
}

// ---------------------------------------------------------------------------
// HTTP API
// ---------------------------------------------------------------------------

type nodeView struct {
	Name      string `json:"name"`
	SocksPort int    `json:"socksPort"`
	AuthKey   string `json:"authKeyMasked"`
	ExitNode  string `json:"exitNode"`
	State     string `json:"state"`
	LastErr   string `json:"lastErr"`
}

func maskKey(k string) string {
	if k == "" {
		return "(none)"
	}
	if len(k) <= 12 {
		return "tskey-..."
	}
	return k[:12] + "\u2022\u2022\u2022\u2022\u2022\u2022"
}

func writeJSON(w http.ResponseWriter, v interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(v)
}

func handleNodes(w http.ResponseWriter, r *http.Request) {
	switch r.Method {
	case http.MethodGet:
		mu.Lock()
		views := make([]nodeView, 0, len(config))
		for _, c := range config {
			st := "Stopped"
			le := ""
			if rn := runtime[c.Name]; rn != nil && rn.state != "" {
				st = rn.state
				le = rn.lastErr
			}
			views = append(views, nodeView{c.Name, c.SocksPort, maskKey(c.AuthKey), c.ExitNode, st, le})
		}
		mu.Unlock()
		writeJSON(w, views)

	case http.MethodPost:
		var in InstanceConfig
		if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
			http.Error(w, "bad JSON", http.StatusBadRequest)
			return
		}
		in.Name = strings.TrimSpace(in.Name)
		if !nameRe.MatchString(in.Name) {
			http.Error(w, "name must be 1-32 chars: letters, digits, _ or - only", http.StatusBadRequest)
			return
		}
		if in.SocksPort < 1024 || in.SocksPort > 65535 {
			http.Error(w, "socksPort must be between 1024 and 65535", http.StatusBadRequest)
			return
		}
		mu.Lock()
		for _, c := range config {
			if c.Name == in.Name {
				mu.Unlock()
				http.Error(w, "a node with that name already exists", http.StatusBadRequest)
				return
			}
			if c.SocksPort == in.SocksPort {
				mu.Unlock()
				http.Error(w, "that SOCKS port is already in use by "+c.Name, http.StatusBadRequest)
				return
			}
		}
		config = append(config, in)
		saveConfig()
		mu.Unlock()
		writeJSON(w, map[string]string{"ok": "added"})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleNodeItem routes /api/node/{name}[/action].
func handleNodeItem(w http.ResponseWriter, r *http.Request) {
	rest := strings.TrimPrefix(r.URL.Path, "/api/node/")
	parts := strings.SplitN(rest, "/", 2)
	name := parts[0]
	action := ""
	if len(parts) == 2 {
		action = parts[1]
	}
	if name == "" {
		http.Error(w, "missing node name", http.StatusBadRequest)
		return
	}

	switch action {
	case "": // PUT edit, DELETE remove
		switch r.Method {
		case http.MethodPut:
			var in InstanceConfig
			if err := json.NewDecoder(r.Body).Decode(&in); err != nil {
				http.Error(w, "bad JSON", http.StatusBadRequest)
				return
			}
			mu.Lock()
			idx := -1
			for i := range config {
				if config[i].Name == name {
					idx = i
				}
			}
			if idx < 0 {
				mu.Unlock()
				http.Error(w, "node not found", http.StatusNotFound)
				return
			}
			if in.SocksPort < 1024 || in.SocksPort > 65535 {
				mu.Unlock()
				http.Error(w, "socksPort must be between 1024 and 65535", http.StatusBadRequest)
				return
			}
			for i := range config {
				if i != idx && config[i].SocksPort == in.SocksPort {
					mu.Unlock()
					http.Error(w, "that SOCKS port is already in use", http.StatusBadRequest)
					return
				}
			}
			// Name is the key; keep it stable. Only port/key/exit are editable.
			config[idx].SocksPort = in.SocksPort
			if in.AuthKey != "" { // empty means "leave unchanged"
				config[idx].AuthKey = in.AuthKey
			}
			config[idx].ExitNode = in.ExitNode
			saveConfig()
			mu.Unlock()
			writeJSON(w, map[string]string{"ok": "updated"})

		case http.MethodDelete:
			mu.Lock()
			stopNodeLocked(name)
			delete(runtime, name)
			out := config[:0]
			found := false
			for _, c := range config {
				if c.Name == name {
					found = true
					continue
				}
				out = append(out, c)
			}
			config = out
			if found {
				saveConfig()
			}
			mu.Unlock()
			if !found {
				http.Error(w, "node not found", http.StatusNotFound)
				return
			}
			writeJSON(w, map[string]string{"ok": "deleted"})

		default:
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		}

	case "start":
		mu.Lock()
		var inst *InstanceConfig
		for i := range config {
			if config[i].Name == name {
				inst = &config[i]
			}
		}
		if inst == nil {
			mu.Unlock()
			http.Error(w, "node not found", http.StatusNotFound)
			return
		}
		if inst.AuthKey == "" {
			mu.Unlock()
			http.Error(w, "set an auth key before starting", http.StatusBadRequest)
			return
		}
		err := startNodeLocked(inst)
		mu.Unlock()
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, map[string]string{"ok": "starting"})

	case "stop":
		mu.Lock()
		stopNodeLocked(name)
		mu.Unlock()
		writeJSON(w, map[string]string{"ok": "stopped"})

	case "status":
		writeJSON(w, map[string]string{"status": liveStatus(name)})

	case "logs":
		mu.Lock()
		rn := runtime[name]
		mu.Unlock()
		lines := []string{}
		if rn != nil {
			lines = rn.logBuf.snapshot()
		}
		writeJSON(w, map[string]interface{}{"lines": lines})

	// Feature 3 — exit-node list for a specific node
	case "exit-nodes":
		if r.Method != http.MethodGet {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		mu.Lock()
		rn := runtime[name]
		mu.Unlock()
		if rn == nil || (rn.state != "Running" && rn.state != "Starting") {
			http.Error(w, "node is not running", http.StatusBadRequest)
			return
		}
		pn := pipeName(name)
		cmd := exec.Command(TailscalePath, "--socket="+pn, "exit-node", "list", "--json")
		cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CreationFlags: 0x00000200}
		done := make(chan struct{})
		var raw []byte
		var runErr error
		go func() { raw, runErr = cmd.CombinedOutput(); close(done) }()
		select {
		case <-done:
		case <-time.After(statusTimeout):
			if cmd.Process != nil {
				cmd.Process.Kill()
			}
			http.Error(w, "exit-node list timed out", http.StatusGatewayTimeout)
			return
		}
		if runErr != nil {
			// Older tailscale builds lack --json; fall back to plain text parsing.
			cmd2 := exec.Command(TailscalePath, "--socket="+pn, "exit-node", "list")
			cmd2.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CreationFlags: 0x00000200}
			raw, _ = cmd2.CombinedOutput()
			// Parse lines: each line is "  IP  hostname  country ..."
			type ExitNodeEntry struct {
				IP   string `json:"ip"`
				Name string `json:"name"`
			}
			var entries []ExitNodeEntry
			for _, line := range strings.Split(string(raw), "\n") {
				line = strings.TrimSpace(line)
				if line == "" || strings.HasPrefix(line, "#") || strings.HasPrefix(line, "IP") {
					continue
				}
				fields := strings.Fields(line)
				if len(fields) >= 2 {
					label := strings.Join(fields[1:], " ")
					entries = append(entries, ExitNodeEntry{IP: fields[0], Name: label})
				}
			}
			writeJSON(w, entries)
			return
		}
		// Forward the JSON array directly.
		w.Header().Set("Content-Type", "application/json")
		w.Write(raw)

	// Feature 3 — apply exit-node selection live (no restart)
	case "set-exit-node":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		var body struct {
			ExitNode string `json:"exitNode"`
		}
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			http.Error(w, "bad JSON", http.StatusBadRequest)
			return
		}
		mu.Lock()
		rn := runtime[name]
		mu.Unlock()
		if rn == nil || (rn.state != "Running" && rn.state != "Starting") {
			http.Error(w, "node is not running", http.StatusBadRequest)
			return
		}
		pn := pipeName(name)
		args := []string{"--socket=" + pn, "set"}
		if body.ExitNode == "" {
			args = append(args, "--exit-node=")
		} else {
			args = append(args, "--exit-node="+body.ExitNode)
		}
		cmd := exec.Command(TailscalePath, args...)
		cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CreationFlags: 0x00000200}
		out, err := cmd.CombinedOutput()
		if err != nil {
			http.Error(w, "tailscale set failed: "+strings.TrimSpace(string(out)), http.StatusInternalServerError)
			return
		}
		// Persist to config
		mu.Lock()
		for i := range config {
			if config[i].Name == name {
				config[i].ExitNode = body.ExitNode
				break
			}
		}
		rn.logBuf.add(time.Now().Format("15:04:05") + fmt.Sprintf(" [mgr] exit-node set to %q (live)", body.ExitNode))
		saveConfig()
		mu.Unlock()
		writeJSON(w, map[string]string{"ok": "exit-node updated"})

	// Feature 4 — per-node public IP check via curl --socks5
	case "check-ip":
		if r.Method != http.MethodPost {
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		mu.Lock()
		var socksPort int
		for _, c := range config {
			if c.Name == name {
				socksPort = c.SocksPort
				break
			}
		}
		rn := runtime[name]
		mu.Unlock()
		if socksPort == 0 {
			http.Error(w, "node not found", http.StatusNotFound)
			return
		}
		if rn == nil || rn.state != "Running" {
			http.Error(w, "node is not running", http.StatusBadRequest)
			return
		}
		socks5Addr := fmt.Sprintf("127.0.0.1:%d", socksPort)
		cmd := exec.Command("curl", "--socks5", socks5Addr,
			"--max-time", "15",
			"--silent",
			"https://api.ipify.org?format=json",
		)
		cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CreationFlags: 0x00000200}
		done2 := make(chan struct{})
		var raw2 []byte
		var ipErr error
		go func() { raw2, ipErr = cmd.CombinedOutput(); close(done2) }()
		select {
		case <-done2:
		case <-time.After(20 * time.Second):
			if cmd.Process != nil {
				cmd.Process.Kill()
			}
			http.Error(w, "IP check timed out", http.StatusGatewayTimeout)
			return
		}
		if ipErr != nil {
			http.Error(w, "curl failed: "+strings.TrimSpace(string(raw2)), http.StatusBadGateway)
			return
		}
		// raw2 is {"ip":"1.2.3.4"} — forward it with extra fields
		var ipResp map[string]string
		if err := json.Unmarshal(raw2, &ipResp); err != nil {
			http.Error(w, "unexpected response: "+string(raw2), http.StatusBadGateway)
			return
		}
		ipResp["node"] = name
		ipResp["socksPort"] = strconv.Itoa(socksPort)
		writeJSON(w, ipResp)

	default:
		http.Error(w, "unknown action", http.StatusNotFound)
	}
}

func handlePreflight(w http.ResponseWriter, r *http.Request) {
	checks := []map[string]interface{}{}
	add := func(label string, ok bool, detail string) {
		checks = append(checks, map[string]interface{}{"label": label, "ok": ok, "detail": detail})
	}

	// 1. Admin elevation
	add("Running as Administrator", isElevated(), "Required for named-pipe creation")

	// 2. tailscaled.exe present
	_, e1 := os.Stat(TailscaledPath)
	add("tailscaled.exe found", e1 == nil, TailscaledPath)

	// 3. tailscale.exe present
	_, e2 := os.Stat(TailscalePath)
	add("tailscale.exe found", e2 == nil, TailscalePath)

	// 4. curl available (needed for per-node IP checks)
	curlPath, e3 := exec.LookPath("curl")
	if e3 != nil {
		curlPath = "curl not found in PATH"
	}
	add("curl available (for IP checks)", e3 == nil, curlPath)

	// 5. LOCALAPPDATA writable (state dirs live here)
	localApp := os.Getenv("LOCALAPPDATA")
	testDir := filepath.Join(localApp, "TSMulti", ".preflight_probe")
	e4 := os.MkdirAll(testDir, 0755)
	if e4 == nil {
		os.Remove(testDir)
	}
	add("LOCALAPPDATA writable", e4 == nil, filepath.Join(localApp, "TSMulti"))

	// 6. No port conflicts: scan each configured node's SOCKS port
	mu.Lock()
	cfgSnap := make([]InstanceConfig, len(config))
	copy(cfgSnap, config)
	mu.Unlock()
	for _, c := range cfgSnap {
		lbl := fmt.Sprintf("SOCKS port %d free (%s)", c.SocksPort, c.Name)
		// If the node is Running its port is intentionally in use; skip.
		mu.Lock()
		rn := runtime[c.Name]
		running := rn != nil && (rn.state == "Running" || rn.state == "Starting")
		mu.Unlock()
		if running {
			add(lbl, true, "node is Running (port in use by itself)")
			continue
		}
		// Quick TCP dial attempt; port is free if connection refused immediately.
		cmd := exec.Command("powershell", "-NoProfile", "-Command",
			fmt.Sprintf("$t=New-Object Net.Sockets.TcpClient;try{$t.Connect('127.0.0.1',%d);$t.Close();'used'}catch{'free'}", c.SocksPort))
		cmd.SysProcAttr = &syscall.SysProcAttr{HideWindow: true, CreationFlags: 0x00000200}
		out, _ := cmd.Output()
		free := strings.TrimSpace(string(out)) == "free"
		add(lbl, free, fmt.Sprintf("127.0.0.1:%d", c.SocksPort))
	}

	writeJSON(w, checks)
}

// ---------------------------------------------------------------------------
// Dashboard (HTML + JS)
// ---------------------------------------------------------------------------

func handleDashboard(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, dashboardHTML)
}

const dashboardHTML = `<!doctype html>
<html lang="en"><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>TS-Manager</title>
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=DM+Mono:wght@400;500&family=Syne:wght@600;700;800&family=DM+Sans:wght@300;400;500&display=swap" rel="stylesheet">
<style>
*,*::before,*::after{box-sizing:border-box;margin:0;padding:0}

:root{
  --c0:#091413;
  --c1:#285A48;
  --c2:#408A71;
  --c3:#B0E4CC;
  --c3a:rgba(176,228,204,.12);
  --c3b:rgba(176,228,204,.06);
  --glass:rgba(40,90,72,.18);
  --glass2:rgba(9,20,19,.6);
  --border:rgba(176,228,204,.15);
  --border2:rgba(176,228,204,.08);
  --text:#e8f7f0;
  --muted:rgba(176,228,204,.5);
  --r:16px;
  --r2:12px;
  --shadow:0 8px 32px rgba(0,0,0,.45);
}

body{
  font-family:'DM Sans',sans-serif;
  background:var(--c0);
  color:var(--text);
  min-height:100vh;
  overflow-x:hidden;
}

/* ── background mesh ── */
body::before{
  content:'';
  position:fixed;inset:0;
  background:
    radial-gradient(ellipse 60% 50% at 15% 20%, rgba(64,138,113,.18) 0%, transparent 60%),
    radial-gradient(ellipse 40% 60% at 85% 80%, rgba(40,90,72,.22) 0%, transparent 55%),
    radial-gradient(ellipse 30% 30% at 50% 50%, rgba(176,228,204,.04) 0%, transparent 50%);
  pointer-events:none;z-index:0;
}

/* ── layout ── */
#app{position:relative;z-index:1;display:flex;flex-direction:column;min-height:100vh}

/* ── header ── */
header{
  display:flex;align-items:center;gap:16px;
  padding:18px 28px;
  background:rgba(9,20,19,.7);
  border-bottom:1px solid var(--border);
  backdrop-filter:blur(12px);
  position:sticky;top:0;z-index:100;
}
.logo{
  display:flex;align-items:center;gap:10px;flex:1;
}
.logo-icon{
  width:36px;height:36px;border-radius:10px;
  background:linear-gradient(135deg,var(--c2),var(--c1));
  display:flex;align-items:center;justify-content:center;
  font-size:18px;box-shadow:0 4px 12px rgba(64,138,113,.4);
  flex-shrink:0;
}
.logo h1{
  font-family:'Syne',sans-serif;font-size:17px;font-weight:700;
  color:var(--text);letter-spacing:.02em;
}
.logo h1 span{color:var(--c3);font-weight:800}
.header-actions{display:flex;gap:8px;align-items:center}

/* ── status pill ── */
#statusPill{
  font-family:'DM Mono',monospace;font-size:11px;
  padding:4px 12px;border-radius:20px;
  background:var(--c3a);border:1px solid var(--border);
  color:var(--muted);transition:all .3s;
}

/* ── buttons ── */
.btn{
  font-family:'DM Sans',sans-serif;font-size:13px;font-weight:500;
  padding:8px 16px;border:0;border-radius:10px;cursor:pointer;
  transition:all .18s;display:inline-flex;align-items:center;gap:6px;
}
.btn:active{transform:scale(.96)}
.btn-primary{background:var(--c2);color:var(--c0);}
.btn-primary:hover{background:var(--c3);box-shadow:0 4px 16px rgba(64,138,113,.35)}
.btn-ghost{background:var(--c3b);color:var(--c3);border:1px solid var(--border)}
.btn-ghost:hover{background:var(--c3a);border-color:rgba(176,228,204,.3)}
.btn-danger{background:rgba(220,50,50,.15);color:#f87171;border:1px solid rgba(220,50,50,.2)}
.btn-danger:hover{background:rgba(220,50,50,.25)}
.btn-sm{padding:5px 11px;font-size:12px;border-radius:8px}
.btn-icon{width:32px;height:32px;padding:0;justify-content:center;border-radius:8px}

/* ── main content ── */
main{flex:1;padding:28px;max-width:1200px;width:100%;margin:0 auto}

/* ── warn bar ── */
#warnBar{
  display:none;
  background:rgba(200,100,20,.12);
  border-bottom:1px solid rgba(200,100,20,.25);
  color:#fbbf72;
  padding:10px 28px;font-size:12.5px;
  font-family:'DM Mono',monospace;
  animation:slideDown .3s ease;
}
@keyframes slideDown{from{opacity:0;transform:translateY(-8px)}to{opacity:1;transform:none}}

/* ── section header row ── */
.section-header{
  display:flex;align-items:center;justify-content:space-between;
  margin-bottom:18px;
}
.section-title{
  font-family:'Syne',sans-serif;font-size:13px;font-weight:600;
  color:var(--muted);text-transform:uppercase;letter-spacing:.1em;
}

/* ── node cards ── */
#cardGrid{
  display:grid;
  grid-template-columns:repeat(auto-fill,minmax(340px,1fr));
  gap:16px;
}
.node-card{
  background:var(--glass2);
  border:1px solid var(--border);
  border-radius:var(--r);
  padding:20px;
  backdrop-filter:blur(10px);
  transition:border-color .2s,box-shadow .2s,transform .2s;
  animation:cardIn .35s ease both;
}
@keyframes cardIn{
  from{opacity:0;transform:translateY(12px)}
  to{opacity:1;transform:none}
}
.node-card:hover{
  border-color:rgba(176,228,204,.28);
  box-shadow:0 12px 40px rgba(0,0,0,.35);
  transform:translateY(-1px);
}

/* card header */
.card-head{display:flex;align-items:flex-start;gap:12px;margin-bottom:16px}
.card-avatar{
  width:40px;height:40px;border-radius:10px;flex-shrink:0;
  background:linear-gradient(135deg,var(--c1),var(--c2));
  display:flex;align-items:center;justify-content:center;
  font-family:'Syne',sans-serif;font-size:16px;font-weight:800;
  color:var(--c3);text-transform:uppercase;
  box-shadow:0 4px 12px rgba(0,0,0,.3);
}
.card-meta{flex:1;min-width:0}
.card-name{
  font-family:'Syne',sans-serif;font-size:15px;font-weight:700;
  color:var(--text);white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
}
.card-port{
  font-family:'DM Mono',monospace;font-size:11px;color:var(--muted);margin-top:2px;
}
.card-badge{
  flex-shrink:0;
  font-size:11px;font-weight:600;font-family:'DM Mono',monospace;
  padding:3px 10px;border-radius:20px;
  transition:all .3s;
}
.badge-Running{background:rgba(64,200,120,.15);color:#6ee7a4;border:1px solid rgba(64,200,120,.25)}
.badge-Stopped{background:rgba(176,228,204,.06);color:var(--muted);border:1px solid var(--border2)}
.badge-Starting{background:rgba(250,180,30,.1);color:#fcd060;border:1px solid rgba(250,180,30,.2)}
.badge-Error{background:rgba(220,50,50,.12);color:#f87171;border:1px solid rgba(220,50,50,.2)}
.badge-NeedsLogin{background:rgba(100,120,250,.12);color:#a5b4fc;border:1px solid rgba(100,120,250,.2)}

/* card info rows */
.card-info{
  display:grid;grid-template-columns:1fr 1fr;gap:8px;
  margin-bottom:16px;
}
.info-item{
  background:var(--c3b);border:1px solid var(--border2);
  border-radius:var(--r2);padding:8px 12px;
}
.info-label{font-size:10px;color:var(--muted);text-transform:uppercase;letter-spacing:.08em;margin-bottom:2px}
.info-val{
  font-family:'DM Mono',monospace;font-size:12px;color:var(--c3);
  white-space:nowrap;overflow:hidden;text-overflow:ellipsis;
}

/* ip inline result */
.ip-pill{
  display:inline-flex;align-items:center;gap:5px;
  font-family:'DM Mono',monospace;font-size:11px;
  padding:3px 9px;border-radius:20px;
  background:rgba(64,200,120,.12);color:#6ee7a4;
  border:1px solid rgba(64,200,120,.2);
  margin-top:4px;animation:fadeIn .3s ease;
}
.ip-pill.fail{background:rgba(220,50,50,.1);color:#f87171;border-color:rgba(220,50,50,.2)}
@keyframes fadeIn{from{opacity:0}to{opacity:1}}

/* card error */
.card-err{
  font-size:11px;color:#f87171;
  background:rgba(220,50,50,.08);border:1px solid rgba(220,50,50,.15);
  border-radius:8px;padding:6px 10px;margin-bottom:12px;
  font-family:'DM Mono',monospace;
}

/* card actions */
.card-actions{display:flex;flex-wrap:wrap;gap:6px}

/* empty state */
.empty-state{
  text-align:center;padding:64px 24px;
  grid-column:1/-1;
}
.empty-icon{font-size:48px;margin-bottom:16px;opacity:.4}
.empty-title{font-family:'Syne',sans-serif;font-size:18px;color:var(--muted);margin-bottom:8px}
.empty-sub{font-size:13px;color:rgba(176,228,204,.3)}

/* ── preflight panel ── */
#pfPanel{
  background:var(--glass2);border:1px solid var(--border);
  border-radius:var(--r);overflow:hidden;margin-bottom:24px;
  backdrop-filter:blur(10px);
  animation:cardIn .3s ease;
}
.pf-header{
  display:flex;align-items:center;gap:12px;
  padding:14px 20px;cursor:pointer;user-select:none;
  transition:background .2s;
}
.pf-header:hover{background:var(--c3b)}
.pf-title{
  flex:1;font-family:'Syne',sans-serif;font-size:13px;font-weight:700;
  letter-spacing:.05em;text-transform:uppercase;color:var(--muted);
}
.pf-chevron{color:var(--muted);transition:transform .2s;font-size:14px}
.pf-chevron.open{transform:rotate(180deg)}
.pf-body{display:none;padding:0 20px 16px}
.pf-body.open{display:block}
.pf-grid{display:grid;grid-template-columns:repeat(auto-fill,minmax(280px,1fr));gap:8px;margin-top:12px}
.pf-row{
  display:flex;align-items:flex-start;gap:10px;
  background:var(--c3b);border:1px solid var(--border2);
  border-radius:10px;padding:10px 12px;
}
.pf-icon{font-size:14px;min-width:18px;margin-top:1px}
.pf-label{font-size:12.5px;font-weight:500;color:var(--text)}
.pf-detail{font-size:10.5px;color:var(--muted);margin-top:2px;font-family:'DM Mono',monospace;word-break:break-all}

/* ── dialogs ── */
dialog{
  border:0;border-radius:20px;padding:0;
  background:linear-gradient(160deg,#142420,#0d1c1a);
  color:var(--text);
  box-shadow:0 24px 80px rgba(0,0,0,.7),0 0 0 1px var(--border);
  width:440px;max-width:94vw;
  animation:dlgIn .22s cubic-bezier(.34,1.56,.64,1);
}
@keyframes dlgIn{
  from{opacity:0;transform:scale(.93) translateY(8px)}
  to{opacity:1;transform:none}
}
dialog::backdrop{background:rgba(5,12,11,.75);backdrop-filter:blur(4px)}
.dlg-head{
  padding:22px 24px 0;
  font-family:'Syne',sans-serif;font-size:17px;font-weight:700;color:var(--c3);
}
.dlg-body{padding:16px 24px}
.dlg-foot{
  padding:16px 24px;border-top:1px solid var(--border2);
  display:flex;justify-content:flex-end;gap:8px;
}

/* form fields */
.field{margin-bottom:14px}
.field label{
  display:block;font-size:11px;font-weight:500;
  color:var(--muted);text-transform:uppercase;letter-spacing:.08em;
  margin-bottom:6px;
}
.field input,.field select{
  width:100%;background:rgba(9,20,19,.8);
  border:1px solid var(--border);border-radius:10px;
  padding:9px 13px;color:var(--text);
  font-family:'DM Mono',monospace;font-size:13px;
  transition:border-color .2s;outline:none;
}
.field input:focus,.field select:focus{border-color:var(--c2);box-shadow:0 0 0 3px rgba(64,138,113,.15)}
.field input::placeholder{color:rgba(176,228,204,.25)}
.field input:disabled{opacity:.5;cursor:not-allowed}
.field select option{background:#142420}
.field select[size]{height:160px}
.field-hint{font-size:10.5px;color:rgba(176,228,204,.35);margin-top:4px;font-family:'DM Mono',monospace}
.field-err{
  font-size:12px;color:#f87171;margin-top:12px;
  background:rgba(220,50,50,.08);border:1px solid rgba(220,50,50,.15);
  border-radius:8px;padding:8px 12px;display:none;
  font-family:'DM Mono',monospace;
}
.field-err.visible{display:block}

/* log dialog */
#logDlg{width:600px}
pre{
  background:rgba(5,12,11,.9);
  border:1px solid var(--border2);
  color:#a7f3d0;
  padding:14px;border-radius:12px;
  max-height:360px;overflow:auto;
  font-family:'DM Mono',monospace;font-size:11.5px;line-height:1.6;
  white-space:pre-wrap;word-break:break-all;
}
pre::-webkit-scrollbar{width:5px}
pre::-webkit-scrollbar-track{background:transparent}
pre::-webkit-scrollbar-thumb{background:var(--c1);border-radius:3px}

/* spinner */
@keyframes spin{to{transform:rotate(360deg)}}
.spin{display:inline-block;animation:spin .7s linear infinite}

/* pulse dot for Running */
.pulse-dot{
  width:7px;height:7px;border-radius:50%;
  background:#6ee7a4;display:inline-block;margin-right:4px;
  box-shadow:0 0 0 0 rgba(110,231,164,.4);
  animation:pulse 2s infinite;
}
@keyframes pulse{
  0%{box-shadow:0 0 0 0 rgba(110,231,164,.4)}
  70%{box-shadow:0 0 0 6px rgba(110,231,164,0)}
  100%{box-shadow:0 0 0 0 rgba(110,231,164,0)}
}

/* scrollbar global */
::-webkit-scrollbar{width:6px;height:6px}
::-webkit-scrollbar-track{background:transparent}
::-webkit-scrollbar-thumb{background:var(--c1);border-radius:3px}
</style>
</head>
<body>
<div id="app">

<header>
  <div class="logo">
    <div class="logo-icon">⬡</div>
    <h1>TS<span>Manager</span></h1>
  </div>
  <div class="header-actions">
    <span id="statusPill">● 0 nodes</span>
    <button class="btn btn-ghost btn-sm" onclick="togglePf()">
      <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.8"><circle cx="8" cy="8" r="6"/><path d="M8 5v3l2 2"/></svg>
      Pre-flight
    </button>
    <button class="btn btn-primary" onclick="openAdd()">
      <svg width="13" height="13" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="2.2"><path d="M8 3v10M3 8h10"/></svg>
      Add Node
    </button>
  </div>
</header>

<div id="warnBar"></div>

<main>

  <!-- preflight -->
  <div id="pfPanel" style="display:none">
    <div class="pf-header" onclick="togglePf()">
      <svg width="14" height="14" viewBox="0 0 16 16" fill="none" stroke="currentColor" stroke-width="1.8"><path d="M8 2l1.5 3 3.5.5-2.5 2.5.6 3.5L8 10l-3.1 1.5.6-3.5L3 5.5l3.5-.5z"/></svg>
      <span class="pf-title">Pre-flight Checks</span>
      <span id="pfBadge" class="card-badge badge-Stopped"></span>
      <span class="pf-chevron" id="pfChevron">▾</span>
    </div>
    <div class="pf-body" id="pfBody">
      <div class="pf-grid" id="pfRows"><div style="color:var(--muted);font-size:13px">Checking…</div></div>
      <div style="margin-top:12px">
        <button class="btn btn-ghost btn-sm" onclick="runPreflight()">↺ Refresh</button>
      </div>
    </div>
  </div>

  <!-- node grid -->
  <div class="section-header">
    <span class="section-title">Nodes</span>
  </div>
  <div id="cardGrid"><div class="empty-state"><div class="empty-icon">🌐</div><div class="empty-title">Loading…</div></div></div>

</main>
</div>

<!-- ── Add / Edit dialog ── -->
<dialog id="dlg">
  <div class="dlg-head" id="dlgTitle">Add Node</div>
  <div class="dlg-body">
    <input type="hidden" id="fMode">
    <div class="field">
      <label>Node Name</label>
      <input id="fName" placeholder="e.g. node-01">
      <div class="field-hint">Letters, digits, _ or - · max 32 chars · fixed after creation</div>
    </div>
    <div class="field">
      <label>SOCKS5 Port</label>
      <input id="fPort" type="number" value="1081" placeholder="1081–65535">
    </div>
    <div class="field">
      <label>Auth Key</label>
      <input id="fKey" placeholder="tskey-auth-…">
      <div class="field-hint">Leave blank when editing to keep current key</div>
    </div>
    <div class="field">
      <label>Exit Node IP <span style="text-transform:none;font-size:10px;opacity:.6">(optional)</span></label>
      <input id="fExit" placeholder="100.x.y.z  — or use picker once running">
    </div>
    <div class="field-err" id="fErr"></div>
  </div>
  <div class="dlg-foot">
    <button class="btn btn-ghost" onclick="dlg.close()">Cancel</button>
    <button class="btn btn-primary" onclick="saveNode()">Save Node</button>
  </div>
</dialog>

<!-- ── Logs dialog ── -->
<dialog id="logDlg">
  <div class="dlg-head" id="logTitle">Logs</div>
  <div class="dlg-body">
    <pre id="logBox">loading…</pre>
  </div>
  <div class="dlg-foot">
    <button class="btn btn-ghost" onclick="stopLogs()">Close</button>
  </div>
</dialog>

<!-- ── Exit-node picker dialog ── -->
<dialog id="exitDlg">
  <div class="dlg-head" id="exitTitle">Exit Nodes</div>
  <div class="dlg-body">
    <div class="field">
      <label>Available exit nodes</label>
      <select id="exitList" size="7"></select>
    </div>
    <div class="field-err" id="exitErr"></div>
  </div>
  <div class="dlg-foot">
    <button class="btn btn-ghost" onclick="exitDlg.close()">Cancel</button>
    <button class="btn btn-danger btn-sm" onclick="clearExitNode()">✕ Clear</button>
    <button class="btn btn-primary" onclick="applyExitNode()">Apply</button>
  </div>
</dialog>

<script>
const dlg=document.getElementById('dlg');
const logDlg=document.getElementById('logDlg');
const exitDlg=document.getElementById('exitDlg');
let logTimer=null,logName='',exitNodeName='';

function esc(s){return String(s).replace(/[&<>"']/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));}
function enc(o){return "'"+encodeURIComponent(JSON.stringify(o))+"'";}
function showErr(id,msg){const el=document.getElementById(id);el.textContent=msg;el.classList.toggle('visible',!!msg);}

// ── Main card refresh ─────────────────────────────────────────────────────
async function refresh(){
  try{
    const r=await fetch('/api/nodes');
    const nodes=await r.json();
    const grid=document.getElementById('cardGrid');
    document.getElementById('statusPill').textContent='● '+nodes.length+' node'+(nodes.length===1?'':'s');

    if(!nodes.length){
      grid.innerHTML='<div class="empty-state">'
        +'<div class="empty-icon">🌐</div>'
        +'<div class="empty-title">No nodes yet</div>'
        +'<div class="empty-sub">Click "+ Add Node" to create your first Tailscale instance</div>'
        +'</div>';
      return;
    }

    // Keep IP pill contents across refreshes
    const ipCache={};
    document.querySelectorAll('[id^="ip_"]').forEach(el=>{
      const nm=el.id.slice(3);ipCache[nm]=el.innerHTML;
    });

    grid.innerHTML=nodes.map((n,i)=>{
      const running=n.state==='Running';
      const starting=n.state==='Starting';
      const active=running||starting;

      const dot=running?'<span class="pulse-dot"></span>':'';
      const avatar=esc(n.name.charAt(0));

      const toggleBtn=active
        ?'<button class="btn btn-danger btn-sm" onclick="act(\''+n.name+'\',\'stop\')">⏹ Stop</button>'
        :'<button class="btn btn-primary btn-sm" onclick="act(\''+n.name+'\',\'start\')">▶ Start</button>';

      const exitBtn=running
        ?'<button class="btn btn-ghost btn-sm" onclick="openExitPicker(\''+n.name+'\')">⇌ Exit Node</button>':'';

      const ipBtn=running
        ?'<button class="btn btn-ghost btn-sm" onclick="checkIP(\''+n.name+'\','+n.socksPort+')">⌖ Check IP</button>':'';

      const errHtml=n.lastErr
        ?'<div class="card-err">'+esc(n.lastErr)+'</div>':'';

      const cached=ipCache[n.name]||'';

      return '<div class="node-card" style="animation-delay:'+(i*40)+'ms">'
        +'<div class="card-head">'
          +'<div class="card-avatar">'+avatar+'</div>'
          +'<div class="card-meta">'
            +'<div class="card-name">'+esc(n.name)+'</div>'
            +'<div class="card-port">SOCKS :'+n.socksPort+'</div>'
          +'</div>'
          +'<span class="card-badge badge-'+n.state+'">'+dot+n.state+'</span>'
        +'</div>'
        +'<div class="card-info">'
          +'<div class="info-item">'
            +'<div class="info-label">Exit Node</div>'
            +'<div class="info-val">'+esc(n.exitNode||'— none —')+'</div>'
          +'</div>'
          +'<div class="info-item">'
            +'<div class="info-label">Auth Key</div>'
            +'<div class="info-val">'+esc(n.authKeyMasked)+'</div>'
          +'</div>'
        +'</div>'
        +errHtml
        +'<div class="card-actions">'
          +toggleBtn+exitBtn+ipBtn
          +'<button class="btn btn-ghost btn-sm" onclick="showLogs(\''+n.name+'\')">📋 Logs</button>'
          +'<button class="btn btn-ghost btn-sm" onclick="openEdit('+enc(n)+')">✎ Edit</button>'
          +'<button class="btn btn-danger btn-sm" onclick="delNode(\''+n.name+'\')">✕</button>'
        +'</div>'
        +'<div id="ip_'+esc(n.name)+'" style="margin-top:8px">'+cached+'</div>'
        +'</div>';
    }).join('');
  }catch(e){console.error(e);}
}

// ── CRUD ─────────────────────────────────────────────────────────────────
function openAdd(){
  document.getElementById('dlgTitle').textContent='Add Node';
  document.getElementById('fMode').value='add';
  document.getElementById('fName').value='';document.getElementById('fName').disabled=false;
  document.getElementById('fPort').value='1081';
  document.getElementById('fKey').value='';document.getElementById('fExit').value='';
  showErr('fErr','');dlg.showModal();
}
function openEdit(e){
  const n=JSON.parse(decodeURIComponent(e));
  document.getElementById('dlgTitle').textContent='Edit '+n.name;
  document.getElementById('fMode').value='edit';
  document.getElementById('fName').value=n.name;document.getElementById('fName').disabled=true;
  document.getElementById('fPort').value=n.socksPort;
  document.getElementById('fKey').value='';
  document.getElementById('fExit').value=n.exitNode||'';
  showErr('fErr','');dlg.showModal();
}
async function saveNode(){
  const mode=document.getElementById('fMode').value;
  const body={name:document.getElementById('fName').value,
    socksPort:parseInt(document.getElementById('fPort').value,10),
    authKey:document.getElementById('fKey').value,
    exitNode:document.getElementById('fExit').value};
  const url=mode==='add'?'/api/nodes':'/api/node/'+encodeURIComponent(body.name);
  const method=mode==='add'?'POST':'PUT';
  const r=await fetch(url,{method,headers:{'Content-Type':'application/json'},body:JSON.stringify(body)});
  if(r.ok){dlg.close();refresh();}
  else showErr('fErr',await r.text());
}
async function delNode(name){
  if(!confirm('Delete "'+name+'"? It will be stopped first.'))return;
  await fetch('/api/node/'+encodeURIComponent(name),{method:'DELETE'});refresh();
}
async function act(name,a){
  const r=await fetch('/api/node/'+encodeURIComponent(name)+'/'+a,{method:'POST'});
  if(!r.ok)alert(await r.text());
  refresh();
}

// ── Logs ─────────────────────────────────────────────────────────────────
async function showLogs(name){
  logName=name;
  document.getElementById('logTitle').textContent='Logs — '+name;
  document.getElementById('logBox').textContent='loading…';
  logDlg.showModal();
  const pull=async()=>{
    try{
      const r=await fetch('/api/node/'+encodeURIComponent(logName)+'/logs');
      const d=await r.json();
      const box=document.getElementById('logBox');
      box.textContent=(d.lines&&d.lines.length)?d.lines.join('\n'):'(no output yet)';
      box.scrollTop=box.scrollHeight;
    }catch(e){}
  };
  await pull();logTimer=setInterval(pull,1500);
}
function stopLogs(){if(logTimer)clearInterval(logTimer);logTimer=null;logDlg.close();}

// ── Exit-node picker ──────────────────────────────────────────────────────
async function openExitPicker(name){
  exitNodeName=name;
  document.getElementById('exitTitle').textContent='Exit Nodes — '+name;
  showErr('exitErr','');
  const list=document.getElementById('exitList');
  list.innerHTML='<option disabled>⏳ Loading…</option>';
  exitDlg.showModal();
  try{
    const r=await fetch('/api/node/'+encodeURIComponent(name)+'/exit-nodes');
    if(!r.ok){showErr('exitErr',await r.text());return;}
    const nodes=await r.json();
    if(!nodes||!nodes.length){
      list.innerHTML='<option disabled>(no exit nodes advertised)</option>';return;
    }
    list.innerHTML=nodes.map(en=>{
      const ip=en.TailscaleIPs?en.TailscaleIPs[0]:en.ip||'';
      const label=en.HostName||en.Name||en.name||ip;
      return '<option value="'+esc(ip)+'">'+esc(label)+'  ('+esc(ip)+')</option>';
    }).join('');
  }catch(e){showErr('exitErr','Error: '+e.message);}
}
async function applyExitNode(){
  const sel=document.getElementById('exitList');
  const ip=sel.value;
  if(!ip||sel.options[sel.selectedIndex]?.disabled)return;
  showErr('exitErr','Applying…');
  const r=await fetch('/api/node/'+encodeURIComponent(exitNodeName)+'/set-exit-node',{
    method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({exitNode:ip})});
  if(r.ok){exitDlg.close();refresh();}
  else showErr('exitErr',await r.text());
}
async function clearExitNode(){
  showErr('exitErr','Clearing…');
  const r=await fetch('/api/node/'+encodeURIComponent(exitNodeName)+'/set-exit-node',{
    method:'POST',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({exitNode:''})});
  if(r.ok){exitDlg.close();refresh();}
  else showErr('exitErr',await r.text());
}

// ── Check IP ─────────────────────────────────────────────────────────────
async function checkIP(name){
  const cell=document.getElementById('ip_'+name);
  if(!cell)return;
  cell.innerHTML='<span class="ip-pill"><span class="spin">↻</span> checking…</span>';
  try{
    const r=await fetch('/api/node/'+encodeURIComponent(name)+'/check-ip',{method:'POST'});
    if(!r.ok){
      cell.innerHTML='<span class="ip-pill fail">✗ '+(await r.text())+'</span>';return;
    }
    const d=await r.json();
    cell.innerHTML='<span class="ip-pill">⌖ '+esc(d.ip)+'</span>';
  }catch(e){cell.innerHTML='<span class="ip-pill fail">✗ '+esc(e.message)+'</span>';}
}

// ── Pre-flight ────────────────────────────────────────────────────────────
let pfVisible=false;
function togglePf(){
  pfVisible=!pfVisible;
  const panel=document.getElementById('pfPanel');
  const body=document.getElementById('pfBody');
  const chev=document.getElementById('pfChevron');
  panel.style.display=pfVisible?'':'none';
  body.classList.toggle('open',pfVisible);
  chev.classList.toggle('open',pfVisible);
  if(pfVisible)runPreflight();
}
async function runPreflight(){
  document.getElementById('pfRows').innerHTML='<div style="color:var(--muted);font-size:13px">Checking…</div>';
  try{
    const r=await fetch('/api/preflight');const checks=await r.json();
    const fail=checks.filter(c=>!c.ok);
    const badge=document.getElementById('pfBadge');
    if(fail.length){
      badge.textContent=fail.length+' issue'+(fail.length>1?'s':'');
      badge.className='card-badge badge-Error';
      const wb=document.getElementById('warnBar');
      wb.style.display='block';
      wb.textContent='⚠ Pre-flight: '+fail.map(c=>c.label).join('  ·  ');
    }else{
      badge.textContent='All clear ✓';badge.className='card-badge badge-Running';
      document.getElementById('warnBar').style.display='none';
    }
    document.getElementById('pfRows').innerHTML=checks.map(c=>{
      return '<div class="pf-row">'
        +'<div class="pf-icon">'+(c.ok?'✓':'✕')+'</div>'
        +'<div><div class="pf-label">'+esc(c.label)+'</div>'
        +'<div class="pf-detail">'+esc(c.detail)+'</div></div>'
        +'</div>';
    }).join('');
  }catch(e){document.getElementById('pfRows').textContent='Error: '+e.message;}
}

// ── Boot ─────────────────────────────────────────────────────────────────
(async()=>{
  try{
    const r=await fetch('/api/preflight');const checks=await r.json();
    const fail=checks.filter(c=>!c.ok);
    if(fail.length){
      const wb=document.getElementById('warnBar');
      wb.style.display='block';
      wb.textContent='⚠ Pre-flight issues detected: '+fail.map(c=>c.label).join('  ·  ')+'  — click Pre-flight for details';
    }
  }catch(e){}
})();

refresh();setInterval(refresh,10000);
</script>
</body></html>`