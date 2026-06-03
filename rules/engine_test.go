package rules

import (
	"context"
	"testing"

	"github.com/networkextension/polar-dataflow/core"
)

const testRules = `{
  "rules": [
    {"id":"t_high","data_type":"temperature","condition":"value > threshold","threshold":60,"action":"alert","severity":2,"need_task":true,"trigger_mode":"immediate","task":{"type":"alert"}},
    {"id":"legal_ok","data_type":"legal_case","condition":"quality_score >= 0.7","action":"ingest_rag","need_task":true,"trigger_mode":"on_transfer_done","task":{"type":"rag_write","params":{"target_kb":"kb"}}}
  ]
}`

func mustEngine(t *testing.T) *Engine {
	t.Helper()
	e, err := Parse([]byte(testRules))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	return e
}

func TestThresholdFires(t *testing.T) {
	e := mustEngine(t)
	env := core.NewEnvelope("x", "tn", "src", nil)
	env.DataType = "temperature"
	env.Meta["value"] = 65.0
	d, err := e.Evaluate(context.Background(), env)
	if err != nil {
		t.Fatal(err)
	}
	if !d.Triggered || d.RuleID != "t_high" || !d.NeedTask {
		t.Fatalf("expected t_high to fire with task, got %+v", d)
	}
}

func TestThresholdSilent(t *testing.T) {
	e := mustEngine(t)
	env := core.NewEnvelope("x", "tn", "src", nil)
	env.DataType = "temperature"
	env.Meta["value"] = 22.0
	d, _ := e.Evaluate(context.Background(), env)
	if d.Triggered {
		t.Fatalf("expected no trigger, got %+v", d)
	}
}

func TestDeferralAdvice(t *testing.T) {
	e := mustEngine(t)
	if !e.HasDeferredRules("legal_case") {
		t.Fatal("legal_case should be deferred")
	}
	if e.HasDeferredRules("temperature") {
		t.Fatal("temperature should not be deferred")
	}
}

func TestUnknownTypeAllows(t *testing.T) {
	e := mustEngine(t)
	env := core.NewEnvelope("x", "tn", "src", nil)
	env.DataType = "unknown"
	d, _ := e.Evaluate(context.Background(), env)
	if d.Triggered {
		t.Fatalf("unknown type should not trigger, got %+v", d)
	}
}
