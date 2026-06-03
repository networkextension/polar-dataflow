package rules

import (
	"context"

	"github.com/networkextension/polar-dataflow/core"
)

// RuleEvaluator adapts the config-driven Engine to the core.Evaluator
// interface so the pipeline can use it like any other plugin. A single
// RuleEvaluator can serve all data types, or you can register several
// scoped to different types.
type RuleEvaluator struct {
	engine    *Engine
	dataTypes map[string]bool // nil => applies to all
}

// NewEvaluator wraps an engine. If dataTypes is empty the evaluator
// applies to every data type.
func NewEvaluator(engine *Engine, dataTypes ...string) *RuleEvaluator {
	var set map[string]bool
	if len(dataTypes) > 0 {
		set = make(map[string]bool, len(dataTypes))
		for _, dt := range dataTypes {
			set[dt] = true
		}
	}
	return &RuleEvaluator{engine: engine, dataTypes: set}
}

func (e *RuleEvaluator) Name() string { return "rule-evaluator" }

func (e *RuleEvaluator) Applies(dataType string) bool {
	if e.dataTypes == nil {
		return true
	}
	return e.dataTypes[dataType]
}

func (e *RuleEvaluator) Evaluate(ctx context.Context, env *core.DataEnvelope) (*core.Decision, error) {
	return e.engine.Evaluate(ctx, env)
}

// NeedsTransferComplete implements core.DeferralAdvisor: defer when this
// data type has any on_transfer_done rule.
func (e *RuleEvaluator) NeedsTransferComplete(dataType string) bool {
	return e.engine.HasDeferredRules(dataType)
}
