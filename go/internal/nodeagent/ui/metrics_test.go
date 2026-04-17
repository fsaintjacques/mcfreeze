package ui

import (
	"testing"
)

func TestParsePrometheusText(t *testing.T) {
	input := `# HELP mcf_active_datasets Active dataset count.
# TYPE mcf_active_datasets gauge
mcf_active_datasets 3
# HELP mcf_connections_active Currently open connections by transport.
# TYPE mcf_connections_active gauge
mcf_connections_active{transport="tcp"} 5
mcf_connections_active{transport="uds"} 2
# HELP mcf_connections_total Total connections accepted by transport.
# TYPE mcf_connections_total counter
mcf_connections_total_total{transport="tcp"} 42
# EOF
`
	metrics := parsePrometheusText(input)

	if len(metrics) != 4 {
		t.Fatalf("expected 4 metrics, got %d: %+v", len(metrics), metrics)
	}

	// Check gauge.
	assertMetric(t, metrics[0], "mcf_active_datasets", "3")
	if metrics[0].Help != "Active dataset count." {
		t.Errorf("expected help text, got %q", metrics[0].Help)
	}

	// Check labeled gauge metrics.
	assertMetric(t, metrics[1], `mcf_connections_active{transport="tcp"}`, "5")
	assertMetric(t, metrics[2], `mcf_connections_active{transport="uds"}`, "2")

	// Check labeled counter with _total suffix doubling.
	assertMetric(t, metrics[3], `mcf_connections_total_total{transport="tcp"}`, "42")
	if metrics[3].Help != "Total connections accepted by transport." {
		t.Errorf("expected help text, got %q", metrics[3].Help)
	}
}

func TestParsePrometheusText_Empty(t *testing.T) {
	metrics := parsePrometheusText("")
	if len(metrics) != 0 {
		t.Fatalf("expected 0 metrics, got %d", len(metrics))
	}
}

func TestParsePrometheusText_OnlyComments(t *testing.T) {
	input := "# HELP foo bar\n# TYPE foo gauge\n# EOF\n"
	metrics := parsePrometheusText(input)
	if len(metrics) != 0 {
		t.Fatalf("expected 0 metrics, got %d", len(metrics))
	}
}

func TestParsePrometheusText_MalformedLines(t *testing.T) {
	input := "good_metric 123\nbad_line_no_value\n"
	metrics := parsePrometheusText(input)
	// Only the first line is valid; the second has no value.
	if len(metrics) != 1 {
		t.Fatalf("expected 1 metric, got %d: %+v", len(metrics), metrics)
	}
	assertMetric(t, metrics[0], "good_metric", "123")
}

func TestParseMetricLine_WithLabels(t *testing.T) {
	name, value := parseMetricLine(`http_requests{method="GET",code="200"} 1234`)
	if name != `http_requests{method="GET",code="200"}` {
		t.Errorf("unexpected name: %q", name)
	}
	if value != "1234" {
		t.Errorf("unexpected value: %q", value)
	}
}

func TestParseMetricLine_WithTimestamp(t *testing.T) {
	name, value := parseMetricLine("my_metric 99 1234567890")
	if name != "my_metric" {
		t.Errorf("unexpected name: %q", name)
	}
	if value != "99" {
		t.Errorf("unexpected value: %q", value)
	}
}

func TestParseMetricLine_NoValue(t *testing.T) {
	name, value := parseMetricLine("orphan")
	if name != "" || value != "" {
		t.Errorf("expected empty, got name=%q value=%q", name, value)
	}
}

func assertMetric(t *testing.T, m kvMetric, name, value string) {
	t.Helper()
	if m.Name != name {
		t.Errorf("expected name %q, got %q", name, m.Name)
	}
	if m.Value != value {
		t.Errorf("expected value %q, got %q", value, m.Value)
	}
}
