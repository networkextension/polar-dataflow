// Package temperature is a sample vertical plugin set for the building
// project: a self-contained sensor reading classified, range-checked,
// and (on threshold breach) dispatched as an alert. Demonstrates the
// Immediate trigger mode with small inline payloads.
package temperature

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/networkextension/polar-dataflow/core"
)

// reading is the wire format from the building ingest adapter.
type reading struct {
	Point string  `json:"point"` // e.g. "B3-F7-E-return-air-temp"
	Value float64 `json:"value"`
	Unit  string  `json:"unit"` // "C" or "F"
}

// Classifier tags temperature readings.
type Classifier struct{}

func (Classifier) Name() string { return "temperature-classifier" }

func (Classifier) Match(env *core.DataEnvelope) bool {
	// Building source emits JSON with a "point" containing "temp".
	var r reading
	if err := json.Unmarshal(env.Payload, &r); err != nil {
		return false
	}
	return r.Point != "" && r.Unit != "" && containsTemp(r.Point)
}

func (Classifier) Classify(_ context.Context, env *core.DataEnvelope) error {
	var r reading
	if err := json.Unmarshal(env.Payload, &r); err != nil {
		return fmt.Errorf("decode reading: %w", err)
	}
	env.DataType = "temperature"
	env.Meta["point"] = r.Point
	env.Meta["raw_value"] = r.Value
	env.Meta["unit"] = r.Unit
	return nil
}

// Processor normalizes units to Celsius.
type Processor struct{}

func (Processor) Name() string                 { return "temperature-processor" }
func (Processor) Applies(dataType string) bool { return dataType == "temperature" }

func (Processor) Process(_ context.Context, env *core.DataEnvelope) error {
	val, _ := env.MetaFloat("raw_value")
	unit, _ := env.MetaString("unit")
	if unit == "F" {
		val = (val - 32) * 5 / 9
	}
	env.Meta["value"] = val // canonical Celsius value used by rules
	return nil
}

// AlertDispatcher handles the "alert" task type for this vertical.
type AlertDispatcher struct {
	Notify func(point string, value float64, severity int) // injected sink
}

func (AlertDispatcher) Name() string                 { return "temperature-alert-dispatcher" }
func (AlertDispatcher) Handles(taskType string) bool { return taskType == "alert" }

func (d AlertDispatcher) Dispatch(_ context.Context, env *core.DataEnvelope, task *core.TaskSpec) error {
	point, _ := env.MetaString("point")
	value, _ := env.MetaFloat("value")
	if d.Notify != nil {
		d.Notify(point, value, 0)
	}
	return nil
}

func containsTemp(s string) bool {
	for i := 0; i+4 <= len(s); i++ {
		if s[i:i+4] == "temp" {
			return true
		}
	}
	return false
}
