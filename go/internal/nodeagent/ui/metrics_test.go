package ui

import (
	"testing"
)

func TestParsePrometheusText(t *testing.T) {
	input := `# HELP fm_keys_requested_total Total individual key lookups.
# TYPE fm_keys_requested_total counter
fm_keys_requested_total_total 42
# HELP fm_active_datasets Active dataset count.
# TYPE fm_active_datasets gauge
fm_active_datasets 3
# HELP fm_connections_active Currently open connections by transport.
# TYPE fm_connections_active gauge
fm_connections_active{transport="tcp"} 5
fm_connections_active{transport="uds"} 2
# EOF
`
	metrics := parsePrometheusText(input)

	if len(metrics) != 4 {
		t.Fatalf("expected 4 metrics, got %d: %+v", len(metrics), metrics)
	}

	// Check counter with _total suffix doubling.
	assertMetric(t, metrics[0], "fm_keys_requested_total_total", "42")
	// Help should match via _total suffix stripping.
	if metrics[0].Help != "Total individual key lookups." {
		t.Errorf("expected help text, got %q", metrics[0].Help)
	}

	// Check gauge.
	assertMetric(t, metrics[1], "fm_active_datasets", "3")
	if metrics[1].Help != "Active dataset count." {
		t.Errorf("expected help text, got %q", metrics[1].Help)
	}

	// Check labeled metrics.
	assertMetric(t, metrics[2], `fm_connections_active{transport="tcp"}`, "5")
	assertMetric(t, metrics[3], `fm_connections_active{transport="uds"}`, "2")
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
