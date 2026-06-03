// Package eeg is a sample vertical plugin set for the theater project:
// an EEG sample stream that is filtered/normalized, scored for arousal,
// and (on a sustained arousal peak) dispatched as a content-reaction
// correlation analysis task. Demonstrates ms-level device timestamps
// and a stateful processor (sliding window) within a plugin.
package eeg

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"

	"github.com/networkextension/polar-dataflow/core"
)

// sample is the wire format from a wearable EEG device.
type sample struct {
	SubjectID string    `json:"subject_id"`
	Channels  []float64 `json:"channels"`  // band-power per channel
	DeviceTS  int64     `json:"device_ts"` // unix millis, hardware-synced
}

// Classifier tags EEG samples.
type Classifier struct{}

func (Classifier) Name() string { return "eeg-classifier" }

func (Classifier) Match(env *core.DataEnvelope) bool {
	var s sample
	if err := json.Unmarshal(env.Payload, &s); err != nil {
		return false
	}
	return s.SubjectID != "" && len(s.Channels) > 0
}

func (Classifier) Classify(_ context.Context, env *core.DataEnvelope) error {
	var s sample
	if err := json.Unmarshal(env.Payload, &s); err != nil {
		return fmt.Errorf("decode eeg: %w", err)
	}
	env.DataType = "eeg"
	env.DeviceTS = s.DeviceTS // preserve hardware-synced ms timestamp
	env.Meta["subject_id"] = s.SubjectID
	env.Meta["channels"] = s.Channels
	return nil
}

// Processor computes an arousal score and tracks how long it has stayed
// above a high threshold (per subject) — a simple sliding window kept in
// the plugin, not the framework.
type Processor struct {
	mu      sync.Mutex
	highRun map[string]int64 // subject -> device_ts when high run started
}

func NewProcessor() *Processor { return &Processor{highRun: make(map[string]int64)} }

func (*Processor) Name() string                 { return "eeg-processor" }
func (*Processor) Applies(dataType string) bool { return dataType == "eeg" }

func (p *Processor) Process(_ context.Context, env *core.DataEnvelope) error {
	chans, ok := env.Meta["channels"].([]float64)
	if !ok || len(chans) == 0 {
		return fmt.Errorf("missing channels")
	}
	// Toy arousal proxy: normalized mean band-power -> 0..1.
	var sum float64
	for _, c := range chans {
		sum += c
	}
	arousal := clamp01(sum / float64(len(chans)))
	env.Meta["arousal"] = arousal

	subject, _ := env.MetaString("subject_id")
	const highThresh = 0.8
	p.mu.Lock()
	if arousal >= highThresh {
		if start, running := p.highRun[subject]; running {
			env.Meta["arousal_duration_ms"] = env.DeviceTS - start
		} else {
			p.highRun[subject] = env.DeviceTS
			env.Meta["arousal_duration_ms"] = int64(0)
		}
	} else {
		delete(p.highRun, subject)
		env.Meta["arousal_duration_ms"] = int64(0)
	}
	p.mu.Unlock()
	return nil
}

// AnalysisDispatcher handles the "analyze" task: enqueue a job that
// correlates this arousal peak with the stage content timeline.
type AnalysisDispatcher struct {
	Enqueue func(job map[string]interface{}) error // injected job queue
}

func (AnalysisDispatcher) Name() string                 { return "eeg-analysis-dispatcher" }
func (AnalysisDispatcher) Handles(taskType string) bool { return taskType == "correlation_analysis" }

func (d AnalysisDispatcher) Dispatch(_ context.Context, env *core.DataEnvelope, task *core.TaskSpec) error {
	subject, _ := env.MetaString("subject_id")
	arousal, _ := env.MetaFloat("arousal")
	job := map[string]interface{}{
		"kind":       "correlation_analysis",
		"subject_id": subject,
		"device_ts":  env.DeviceTS, // align against stage timeline by this ts
		"arousal":    arousal,
		"priority":   task.Priority,
	}
	if d.Enqueue != nil {
		return d.Enqueue(job)
	}
	return nil
}

func clamp01(x float64) float64 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}
