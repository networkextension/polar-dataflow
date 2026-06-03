// Command example wires the framework with all three sample verticals
// (building temperature, theater EEG, legal RAG) into a single pipeline,
// then feeds one representative envelope of each type through it. It uses
// in-memory stub implementations of the infra ports so it runs anywhere.
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/networkextension/polar-dataflow/core"
	"github.com/networkextension/polar-dataflow/pipeline"
	"github.com/networkextension/polar-dataflow/plugins/eeg"
	"github.com/networkextension/polar-dataflow/plugins/legal"
	"github.com/networkextension/polar-dataflow/plugins/temperature"
	"github.com/networkextension/polar-dataflow/rules"
)

// ---- stub infra ports ----

type stdLogger struct{}

func (stdLogger) Info(msg string, kv ...interface{}) {
	log.Println(append([]interface{}{"INFO ", msg}, kv...)...)
}
func (stdLogger) Warn(msg string, kv ...interface{}) {
	log.Println(append([]interface{}{"WARN ", msg}, kv...)...)
}
func (stdLogger) Error(msg string, kv ...interface{}) {
	log.Println(append([]interface{}{"ERROR", msg}, kv...)...)
}

type memStore struct{}

func (memStore) Save(_ context.Context, env *core.DataEnvelope) error { return nil }
func (memStore) UpdateStatus(_ context.Context, traceID string, s core.ProcessStatus) error {
	log.Printf("status  trace=%s -> %s", traceID, s)
	return nil
}

type logAlert struct{}

func (logAlert) Emit(_ context.Context, env *core.DataEnvelope, d *core.Decision) error {
	log.Printf("ALERT   trace=%s rule=%s action=%s severity=%d", env.TraceID, d.RuleID, d.Action, d.Severity)
	return nil
}

// memBus is a trivial in-process EventBus.
type memBus struct {
	handlers map[string][]func([]byte)
}

func newMemBus() *memBus { return &memBus{handlers: map[string][]func([]byte){}} }
func (b *memBus) Publish(_ context.Context, topic string, payload []byte) error {
	for _, h := range b.handlers[topic] {
		h(payload)
	}
	return nil
}
func (b *memBus) Subscribe(topic string, handler func([]byte)) error {
	b.handlers[topic] = append(b.handlers[topic], handler)
	return nil
}

func main() {
	// 1. Load config-driven rules.
	engine, err := rules.Load("examples/rules.json")
	if err != nil {
		log.Fatalf("load rules: %v", err)
	}

	// 2. Register all plugins from the three verticals.
	reg := core.NewRegistry()

	// Building
	reg.RegisterClassifier(temperature.Classifier{})
	reg.RegisterProcessor(temperature.Processor{})
	reg.RegisterDispatcher(temperature.AlertDispatcher{
		Notify: func(point string, value float64, sev int) {
			log.Printf("  -> building notify: %s = %.1fC", point, value)
		},
	})

	// Theater
	reg.RegisterClassifier(eeg.Classifier{})
	reg.RegisterProcessor(eeg.NewProcessor())
	reg.RegisterDispatcher(eeg.AnalysisDispatcher{
		Enqueue: func(job map[string]interface{}) error {
			log.Printf("  -> theater analysis job: %+v", job)
			return nil
		},
	})

	// Legal
	reg.RegisterClassifier(legal.Classifier{})
	reg.RegisterProcessor(legal.Parser{
		Extract: func(uri string) (map[string]interface{}, string, error) {
			return map[string]interface{}{"title": "People v. Example", "case_no": "(2026) 沪01民终1234号"}, "full judgment text ...", nil
		},
		Chunk: func(text, strategy string) []string {
			return []string{"chunk-1 (" + strategy + ")", "chunk-2"}
		},
		Embed: func(chunks []string) ([][]float32, error) {
			out := make([][]float32, len(chunks))
			for i := range chunks {
				out[i] = []float32{0.1, 0.2, 0.3}
			}
			return out, nil
		},
	})
	reg.RegisterDispatcher(legal.RAGDispatcher{
		Write: func(kb string, env *core.DataEnvelope, chunks []string, vecs [][]float32, meta map[string]interface{}) error {
			log.Printf("  -> RAG write kb=%s chunks=%d meta=%+v", kb, len(chunks), meta)
			return nil
		},
	})

	// A single rule evaluator serves every data type.
	reg.RegisterEvaluator(rules.NewEvaluator(engine))

	// 3. Build the pipeline.
	bus := newMemBus()
	p := pipeline.New(reg, pipeline.DefaultConfig(), memStore{}, logAlert{}, bus, stdLogger{})
	ctx := context.Background()

	// ---- Feed one of each ----

	fmt.Println("\n=== building: temperature (immediate) ===")
	tempPayload, _ := json.Marshal(map[string]interface{}{
		"point": "B3-F7-E-return-air-temp", "value": 71.6, "unit": "F", // 22C, below 60C -> no alert
	})
	run(ctx, p, "trace-temp-1", "building-bms", tempPayload, nil)

	tempPayload2, _ := json.Marshal(map[string]interface{}{
		"point": "B3-F7-E-return-air-temp", "value": 65.0, "unit": "C", // > 60C -> alert
	})
	run(ctx, p, "trace-temp-2", "building-bms", tempPayload2, nil)

	fmt.Println("\n=== theater: EEG (immediate, sustained peak triggers) ===")
	base := time.Now().UnixMilli()
	// Three high-arousal samples spanning >3s -> the 2nd/3rd accumulate
	// duration and the rule fires.
	for i, dt := range []int64{0, 1500, 3500} {
		eegPayload, _ := json.Marshal(map[string]interface{}{
			"subject_id": "S-014",
			"channels":   []float64{0.9, 0.95, 0.88},
			"device_ts":  base + dt,
		})
		run(ctx, p, fmt.Sprintf("trace-eeg-%d", i+1), "theater-eeg", eegPayload, nil)
	}

	fmt.Println("\n=== legal: case ingest (on_transfer_done) ===")
	legalEnv := core.NewEnvelope("trace-legal-1", "tenant-lawfirm-A", "legal-intake", nil)
	legalEnv.Ref = &core.PayloadRef{URI: "minio://cases/2026/1234.pdf", Size: 5_000_000, MIMEType: "application/pdf", Complete: false}
	legalEnv.Meta["doc_kind"] = "case"
	legalEnv.Meta["chunk_strategy"] = "legal_clause"
	if _, err := p.Run(ctx, legalEnv); err != nil {
		log.Printf("legal run error: %v", err)
	}
	// Transfer completes later -> releases the deferred parse + RAG write.
	fmt.Println("  ... (transfer completes) ...")
	done, _ := json.Marshal(map[string]string{"trace_id": "trace-legal-1"})
	_ = bus.Publish(ctx, "transfer.complete", done)
}

func run(ctx context.Context, p *pipeline.Pipeline, trace, source string, payload []byte, ref *core.PayloadRef) {
	env := core.NewEnvelope(trace, "tenant-demo", source, payload)
	env.Ref = ref
	if _, err := p.Run(ctx, env); err != nil {
		log.Printf("run error: %v", err)
	}
}
