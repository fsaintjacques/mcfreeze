// Package ui provides an embedded web UI for the mcfreeze node agent.
// It exposes dataset state, catalog/kv-server version comparisons, key
// lookups, and kv-server metrics through a server-side rendered HTML
// interface backed by html/template and embed.FS.
package ui

import (
	"bytes"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/fsaintjacques/mcfreeze/go/api"
)

//go:embed templates/*.html
var templateFS embed.FS

// StateProvider is the interface the UI needs from the node agent.
type StateProvider interface {
	NodeState() api.NodeState
	// Assignments returns the current node assignments including version
	// records (which carry protobuf descriptors).
	Assignments() []api.NodeAssignment
}

// Config holds the UI server configuration.
type Config struct {
	// KVMemcacheAddr is the kv-server memcache protocol address (e.g. "localhost:11211").
	KVMemcacheAddr string
	// KVMetricsAddr is the kv-server HTTP metrics address (e.g. "localhost:9090").
	KVMetricsAddr string
	// CatalogDir is the path to the directory containing catalog.json.
	CatalogDir string
	// AgentNodeName is the Kubernetes node name.
	AgentNodeName string
	// AgentControlPlaneURL is the control-plane URL the agent connects to.
	AgentControlPlaneURL string
	// AgentStartTime is when the agent started.
	AgentStartTime time.Time
}

// Handler is the HTTP handler for the node agent web UI.
type Handler struct {
	state StateProvider
	cfg   Config
	tmpls *template.Template
}

// NewHandler creates a new UI handler.
func NewHandler(state StateProvider, cfg Config) *Handler {
	funcMap := template.FuncMap{
		"since": func(t time.Time) string {
			d := time.Since(t)
			switch {
			case d < time.Minute:
				return fmt.Sprintf("%ds", int(d.Seconds()))
			case d < time.Hour:
				return fmt.Sprintf("%dm%ds", int(d.Minutes()), int(d.Seconds())%60)
			default:
				return fmt.Sprintf("%dh%dm", int(d.Hours()), int(d.Minutes())%60)
			}
		},
		"phaseClass": func(p api.DatasetPhase) string {
			switch p {
			case api.PhaseActive:
				return "phase-active"
			case api.PhaseError:
				return "phase-error"
			case api.PhaseAttaching, api.PhaseMounting:
				return "phase-progress"
			default:
				return ""
			}
		},
		"isPrintable": func(b []byte) bool {
			return utf8.Valid(b) && !containsControl(b)
		},
		"hexDump": func(b []byte) string {
			return hex.Dump(b)
		},
	}

	tmpls := template.Must(template.New("").Funcs(funcMap).ParseFS(templateFS, "templates/*.html"))

	return &Handler{state: state, cfg: cfg, tmpls: tmpls}
}

// RegisterRoutes registers all UI routes on the given mux.
func (h *Handler) RegisterRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /", h.handleIndex)
	mux.HandleFunc("GET /catalog", h.handleCatalog)
	mux.HandleFunc("GET /query", h.handleQuery)
	mux.HandleFunc("GET /metrics", h.handleMetrics)
	mux.HandleFunc("GET /api/state", h.handleAPIState)
}

// page wraps page-specific data with the active nav tab.
type page struct {
	Nav  string
	Data any
}

// --- index: dataset state dashboard ---

type indexData struct {
	NodeName        string
	ControlPlaneURL string
	Uptime          string
	State           api.NodeState
	Now             time.Time
}

func (h *Handler) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	data := indexData{
		NodeName:        h.cfg.AgentNodeName,
		ControlPlaneURL: h.cfg.AgentControlPlaneURL,
		Uptime:          time.Since(h.cfg.AgentStartTime).Truncate(time.Second).String(),
		State:           h.state.NodeState(),
		Now:             time.Now(),
	}
	h.render(w, "index.html", "index", data)
}

// --- catalog: catalog.json vs kv-server /version ---

type catalogData struct {
	CatalogEntries []api.CatalogEntry
	CatalogError   string
	KVVersions     []api.KVDatasetVersion
	KVError        string
}

func (h *Handler) handleCatalog(w http.ResponseWriter, r *http.Request) {
	data := catalogData{}

	// Read catalog.json from disk.
	catalog, err := readCatalogFile(h.cfg.CatalogDir)
	if err != nil {
		data.CatalogError = err.Error()
	} else {
		data.CatalogEntries = catalog.Entries
	}

	// Fetch kv-server /version.
	kvVersions, err := fetchKVVersion(h.cfg.KVMetricsAddr)
	if err != nil {
		data.KVError = err.Error()
	} else {
		data.KVVersions = kvVersions
	}

	h.render(w, "catalog.html", "catalog", data)
}

// --- query: key lookup ---

type queryData struct {
	Key     string
	Queried bool
	Hit     bool
	RawHex  string
	RawUTF8 string
	JSON    string
	Error   string
}

func (h *Handler) handleQuery(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	data := queryData{Key: key}

	if key != "" {
		data.Queried = true
		value, hit, err := memcacheGet(h.cfg.KVMemcacheAddr, key)
		if err != nil {
			data.Error = err.Error()
		} else if !hit {
			data.Hit = false
		} else {
			data.Hit = true
			data.RawHex = hex.Dump(value)
			if utf8.Valid(value) && !containsControl(value) {
				data.RawUTF8 = string(value)
			}

			// Try protobuf decode if a descriptor is available.
			// First try matching by key prefix (catalog mode: "prefix:key").
			// Fall back to any assignment with a descriptor (snapshot mode:
			// bare keys, single dataset).
			if desc, msgName := h.findDescriptorForKey(key); desc != "" {
				decoded, err := decodeProtobuf(value, desc, msgName)
				if err == nil {
					data.JSON = decoded
				}
			}
		}
	}

	h.render(w, "query.html", "query", data)
}

// findDescriptorForKey returns the base64 descriptor and message name for the
// key. In catalog mode (key contains ":"), it matches by prefix. Otherwise it
// returns the first assignment that has a descriptor (snapshot mode).
func (h *Handler) findDescriptorForKey(key string) (descriptor, messageName string) {
	assignments := h.state.Assignments()

	// Catalog mode: match by key prefix.
	if prefix, _, ok := strings.Cut(key, ":"); ok {
		for _, a := range assignments {
			if a.KeyPrefix == prefix && a.Version.Descriptor != "" {
				return a.Version.Descriptor, a.Version.MessageName
			}
		}
	}

	// Fallback: return the first assignment with a descriptor.
	for _, a := range assignments {
		if a.Version.Descriptor != "" {
			return a.Version.Descriptor, a.Version.MessageName
		}
	}
	return "", ""
}

// --- metrics: node-agent + kv-server ---

type metricsData struct {
	NodeAgent nodeAgentMetrics
	KVServer  kvServerMetrics
}

type nodeAgentMetrics struct {
	NodeName    string
	Uptime      string
	NumDatasets int
	Datasets    []api.DatasetState
}

type kvServerMetrics struct {
	Metrics []kvMetric
	Error   string
}

type kvMetric struct {
	Name  string
	Value string
	Help  string
}

func (h *Handler) handleMetrics(w http.ResponseWriter, r *http.Request) {
	state := h.state.NodeState()

	data := metricsData{
		NodeAgent: nodeAgentMetrics{
			NodeName:    h.cfg.AgentNodeName,
			Uptime:      time.Since(h.cfg.AgentStartTime).Truncate(time.Second).String(),
			NumDatasets: len(state.Datasets),
			Datasets:    state.Datasets,
		},
	}

	// Fetch and parse prometheus metrics from kv-server.
	raw, err := fetchRawMetrics(h.cfg.KVMetricsAddr)
	if err != nil {
		data.KVServer.Error = err.Error()
	} else {
		data.KVServer.Metrics = parsePrometheusText(raw)
	}

	h.render(w, "metrics.html", "metrics", data)
}

// --- JSON API ---

func (h *Handler) handleAPIState(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(h.state.NodeState())
}

// --- helpers ---

func (h *Handler) render(w http.ResponseWriter, name string, nav string, data any) {
	var buf bytes.Buffer
	if err := h.tmpls.ExecuteTemplate(&buf, name, page{Nav: nav, Data: data}); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	w.Write(buf.Bytes())
}

func containsControl(b []byte) bool {
	for _, c := range b {
		if c < 0x20 && c != '\n' && c != '\r' && c != '\t' {
			return true
		}
	}
	return false
}

func readCatalogFile(catalogDir string) (*api.CatalogFile, error) {
	if catalogDir == "" {
		return nil, fmt.Errorf("catalog directory not configured")
	}
	path := filepath.Join(catalogDir, "catalog.json")
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var catalog api.CatalogFile
	if err := json.Unmarshal(data, &catalog); err != nil {
		return nil, fmt.Errorf("parse catalog.json: %w", err)
	}
	return &catalog, nil
}

func fetchKVVersion(metricsAddr string) ([]api.KVDatasetVersion, error) {
	url := fmt.Sprintf("http://%s/version", metricsAddr)
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return nil, fmt.Errorf("fetch /version: %w", err)
	}
	defer resp.Body.Close()

	var vr api.KVVersionResponse
	if err := json.NewDecoder(resp.Body).Decode(&vr); err != nil {
		return nil, fmt.Errorf("decode /version: %w", err)
	}
	return vr.Datasets, nil
}

func fetchRawMetrics(metricsAddr string) (string, error) {
	url := fmt.Sprintf("http://%s/metrics", metricsAddr)
	client := &http.Client{Timeout: 2 * time.Second}
	resp, err := client.Get(url)
	if err != nil {
		return "", fmt.Errorf("fetch /metrics: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("read /metrics body: %w", err)
	}
	return string(body), nil
}

// parsePrometheusText parses Prometheus/OpenMetrics exposition text into
// structured metrics. Handles the _total suffix doubling from the Rust
// prometheus-client crate and # EOF from OpenMetrics.
func parsePrometheusText(text string) []kvMetric {
	var metrics []kvMetric
	helpMap := make(map[string]string) // metric base name -> help text

	for _, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || line == "# EOF" {
			continue
		}

		// # HELP <name> <help text>
		if strings.HasPrefix(line, "# HELP ") {
			rest := line[len("# HELP "):]
			if i := strings.IndexByte(rest, ' '); i > 0 {
				helpMap[rest[:i]] = rest[i+1:]
			}
			continue
		}

		// # TYPE lines — skip
		if strings.HasPrefix(line, "#") {
			continue
		}

		// Metric line: <name>{<labels>} <value> or <name> <value>
		name, value := parseMetricLine(line)
		if name == "" {
			continue
		}

		// Look up help by stripping labels and known suffixes.
		baseName := name
		if i := strings.IndexByte(baseName, '{'); i > 0 {
			baseName = baseName[:i]
		}
		help := helpMap[baseName]
		if help == "" {
			// Try without _total suffix (prometheus-client doubles it).
			help = helpMap[strings.TrimSuffix(baseName, "_total")]
		}

		metrics = append(metrics, kvMetric{
			Name:  name,
			Value: value,
			Help:  help,
		})
	}
	return metrics
}

func parseMetricLine(line string) (name, value string) {
	// Handle labels: name{labels} value [timestamp]
	if i := strings.IndexByte(line, '{'); i > 0 {
		j := strings.IndexByte(line[i:], '}')
		if j < 0 {
			return "", ""
		}
		name = line[:i+j+1]
		rest := strings.TrimSpace(line[i+j+1:])
		// Value is the first field after }
		if k := strings.IndexByte(rest, ' '); k > 0 {
			value = rest[:k]
		} else {
			value = rest
		}
		return name, value
	}

	// No labels: name value [timestamp]
	fields := strings.Fields(line)
	if len(fields) < 2 {
		return "", ""
	}
	return fields[0], fields[1]
}
