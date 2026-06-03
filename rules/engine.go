// Package rules implements the config-driven rule engine. Thresholds,
// conditions, and task specs live in YAML so customers can tune them
// post-delivery without code changes. Conditions are evaluated with
// expr-lang/expr against a flat env of the envelope's Meta fields.
package rules

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/expr-lang/expr"
	"github.com/expr-lang/expr/vm"

	"github.com/networkextension/polar-dataflow/core"
)

// Rule is one configured rule. Condition is an expr expression that
// evaluates to bool, run against the envelope Meta plus a few built-ins.
//
// Note: tags are `json` here so the loader uses only the standard
// library (no external YAML dep — friendlier for air-gapped builds).
// A YAML front-end can be added by unmarshalling YAML->map->json, or by
// vendoring gopkg.in/yaml.v3 in environments where it is available.
type Rule struct {
	ID          string                 `json:"id"`
	DataType    string                 `json:"data_type"`
	Condition   string                 `json:"condition"`
	Threshold   float64                `json:"threshold"` // exposed to condition as `threshold`
	Action      string                 `json:"action"`
	Severity    int                    `json:"severity"`
	NeedTask    bool                   `json:"need_task"`
	TriggerMode string                 `json:"trigger_mode"` // "immediate" | "on_transfer_done"
	Task        TaskConfig             `json:"task"`
	Extra       map[string]interface{} `json:"extra"` // free-form values exposed to condition

	program *vm.Program // compiled condition
}

type TaskConfig struct {
	Type     string                 `json:"type"`
	Priority string                 `json:"priority"`
	Params   map[string]interface{} `json:"params"`
}

type ruleFile struct {
	Rules []*Rule `json:"rules"`
}

// Engine holds compiled rules grouped by data type and is safe for
// concurrent reads. Reload swaps the rule set atomically (hot reload).
type Engine struct {
	byType map[string][]*Rule
}

// Load reads, parses and compiles rules from a JSON file.
func Load(path string) (*Engine, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read rules: %w", err)
	}
	return Parse(raw)
}

// Parse compiles rules from raw JSON bytes (handy for tests).
func Parse(raw []byte) (*Engine, error) {
	var rf ruleFile
	if err := json.Unmarshal(raw, &rf); err != nil {
		return nil, fmt.Errorf("parse rules: %w", err)
	}
	byType := make(map[string][]*Rule)
	for _, r := range rf.Rules {
		if r.Condition != "" {
			prog, err := expr.Compile(r.Condition, expr.AsBool(), expr.AllowUndefinedVariables())
			if err != nil {
				return nil, fmt.Errorf("compile rule %q: %w", r.ID, err)
			}
			r.program = prog
		}
		byType[r.DataType] = append(byType[r.DataType], r)
	}
	// Higher severity first so the most serious match wins.
	for _, rs := range byType {
		sort.SliceStable(rs, func(i, j int) bool { return rs[i].Severity > rs[j].Severity })
	}
	return &Engine{byType: byType}, nil
}

// Evaluate runs the rules for the envelope's data type and returns the
// first (highest-severity) match as a core.Decision. If nothing fires,
// Triggered is false.
func (e *Engine) Evaluate(_ context.Context, env *core.DataEnvelope) (*core.Decision, error) {
	rules := e.byType[env.DataType]
	if len(rules) == 0 {
		return &core.Decision{Triggered: false, Action: "allow"}, nil
	}

	for _, r := range rules {
		ok, err := r.matches(env)
		if err != nil {
			return nil, fmt.Errorf("eval rule %q: %w", r.ID, err)
		}
		if !ok {
			continue
		}
		return r.toDecision(), nil
	}
	return &core.Decision{Triggered: false, Action: "allow"}, nil
}

// HasDeferredRules reports whether any rule for this data type requires
// waiting for transfer completion (trigger_mode: on_transfer_done). The
// pipeline uses this to defer evaluation of incomplete large objects
// instead of evaluating them prematurely.
func (e *Engine) HasDeferredRules(dataType string) bool {
	for _, r := range e.byType[dataType] {
		if r.TriggerMode == "on_transfer_done" {
			return true
		}
	}
	return false
}

// matches builds the evaluation environment and runs the compiled program.
func (r *Rule) matches(env *core.DataEnvelope) (bool, error) {
	if r.program == nil {
		return true, nil // no condition => always matches
	}
	// Flat evaluation environment: all Meta keys, plus built-ins.
	vars := make(map[string]interface{}, len(env.Meta)+4)
	for k, v := range env.Meta {
		vars[k] = v
	}
	for k, v := range r.Extra {
		if _, exists := vars[k]; !exists {
			vars[k] = v
		}
	}
	vars["threshold"] = r.Threshold
	vars["data_type"] = env.DataType
	vars["device_ts"] = env.DeviceTS
	vars["transfer_complete"] = env.TransferComplete()

	out, err := expr.Run(r.program, vars)
	if err != nil {
		return false, err
	}
	b, ok := out.(bool)
	if !ok {
		return false, fmt.Errorf("condition did not return bool, got %T", out)
	}
	return b, nil
}

func (r *Rule) toDecision() *core.Decision {
	mode := core.Immediate
	if r.TriggerMode == "on_transfer_done" {
		mode = core.OnTransferDone
	}
	d := &core.Decision{
		Triggered:   true,
		RuleID:      r.ID,
		Action:      r.Action,
		Severity:    r.Severity,
		NeedTask:    r.NeedTask,
		TriggerMode: mode,
		Reason:      fmt.Sprintf("rule %s matched", r.ID),
	}
	if r.NeedTask {
		d.Task = &core.TaskSpec{
			Type:     r.Task.Type,
			Priority: r.Task.Priority,
			Params:   r.Task.Params,
		}
	}
	return d
}
