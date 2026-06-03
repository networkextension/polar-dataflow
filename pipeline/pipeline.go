package pipeline

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/networkextension/polar-dataflow/core"
)

// Config tunes pipeline behaviour. All infra is injected via ports so
// the framework stays decoupled from concrete DBs/queues (air-gap
// friendly, swappable).
type Config struct {
	// FailClose: if true, a stage error blocks the item (safe default for
	// sensitive domains). If false, errors are logged and processing
	// continues where possible.
	FailClose bool
	// MaxRetries for dispatch.
	MaxRetries int
	// RetryBackoff between dispatch attempts.
	RetryBackoff time.Duration
	// TransferTimeout caps how long we wait for transfer.complete before
	// failing an OnTransferDone task.
	TransferTimeout time.Duration
}

func DefaultConfig() Config {
	return Config{
		FailClose:       true,
		MaxRetries:      3,
		RetryBackoff:    2 * time.Second,
		TransferTimeout: 5 * time.Minute,
	}
}

// Pipeline orchestrates the five stages plus storage, alerting and
// transfer-aware dispatch. It owns tracing, status transitions, retry
// and backpressure concerns so plugins don't have to.
type Pipeline struct {
	reg   *core.Registry
	cfg   Config
	store core.Store
	alert core.AlertSink
	bus   core.EventBus
	log   core.Logger

	// pending holds OnTransferDone items awaiting a transfer.complete
	// event, keyed by TraceID.
	mu      sync.Mutex
	pending map[string]*pendingTask
}

type pendingTask struct {
	env      *core.DataEnvelope
	deadline time.Time
}

// New builds a pipeline. store/alert/bus/log may be nil for the trivial
// cases (the pipeline degrades gracefully), but production wires them.
func New(reg *core.Registry, cfg Config, store core.Store, alert core.AlertSink, bus core.EventBus, log core.Logger) *Pipeline {
	p := &Pipeline{
		reg:     reg,
		cfg:     cfg,
		store:   store,
		alert:   alert,
		bus:     bus,
		log:     log,
		pending: make(map[string]*pendingTask),
	}
	if bus != nil {
		// Subscribe to transfer completion to release waiting tasks.
		_ = bus.Subscribe("transfer.complete", p.onTransferComplete)
	}
	return p
}

// Run drives a single envelope through the full pipeline. Returns the
// final Decision (may be nil if not evaluated) and an error if the item
// was blocked under FailClose.
func (p *Pipeline) Run(ctx context.Context, env *core.DataEnvelope) (*core.Decision, error) {
	p.logInfo("pipeline start", "trace", env.TraceID, "source", env.Source)

	// Stage 1: Classify
	if c := p.reg.ClassifierFor(env); c != nil {
		if err := c.Classify(ctx, env); err != nil {
			return nil, p.fail(ctx, env, "classify", err)
		}
	} else if env.DataType == "" {
		// No classifier matched and type unknown: cannot proceed safely.
		return nil, p.fail(ctx, env, "classify", fmt.Errorf("no classifier matched"))
	}
	env.Status = core.StatusClassified
	p.updateStatus(ctx, env)

	// Stage 2: Process (cleaning / unit conv / parse+chunk+embed ...)
	for _, proc := range p.reg.ProcessorsFor(env.DataType) {
		if err := proc.Process(ctx, env); err != nil {
			if p.cfg.FailClose {
				return nil, p.fail(ctx, env, "process:"+proc.Name(), err)
			}
			p.logWarn("processor error (continuing)", "proc", proc.Name(), "err", err)
		}
	}
	env.Status = core.StatusProcessed
	p.updateStatus(ctx, env)

	// Transfer-deferral: if this is an incomplete large object and any
	// evaluator advises that its data type needs the fully transferred
	// object before it can be judged, park it now and resume the rest of
	// the pipeline (evaluate + dispatch) on transfer.complete. This breaks
	// the chicken-and-egg where the gating rule needs the parsed object.
	if env.IsLargeObject() && !env.TransferComplete() && p.deferredFor(env.DataType) {
		p.enqueuePending(env)
		env.Status = core.StatusWaiting
		p.updateStatus(ctx, env)
		p.logInfo("deferring until transfer.complete", "trace", env.TraceID, "type", env.DataType)
		p.save(ctx, env)
		return &core.Decision{Triggered: false, Action: "deferred"}, nil
	}

	return p.evaluateAndDispatch(ctx, env)
}

// deferredFor reports whether any registered evaluator advises deferral
// for the data type.
func (p *Pipeline) deferredFor(dataType string) bool {
	for _, ev := range p.reg.EvaluatorsFor(dataType) {
		if adv, ok := ev.(core.DeferralAdvisor); ok && adv.NeedsTransferComplete(dataType) {
			return true
		}
	}
	return false
}

// evaluateAndDispatch runs stages 3-5 (Evaluate, persist, alert,
// dispatch). Split out so it can run inline or be resumed after
// transfer completion.
func (p *Pipeline) evaluateAndDispatch(ctx context.Context, env *core.DataEnvelope) (*core.Decision, error) {
	// Stage 3: Evaluate (thresholds / quality gate / dedup ...)
	decision := &core.Decision{Triggered: false, Action: "allow"}
	for _, ev := range p.reg.EvaluatorsFor(env.DataType) {
		d, err := ev.Evaluate(ctx, env)
		if err != nil {
			if p.cfg.FailClose {
				return nil, p.fail(ctx, env, "evaluate:"+ev.Name(), err)
			}
			p.logWarn("evaluator error (continuing)", "eval", ev.Name(), "err", err)
			continue
		}
		if d != nil && d.Triggered {
			decision = d
			break // first (highest-priority evaluator) triggering wins
		}
	}
	env.Status = core.StatusEvaluated
	p.updateStatus(ctx, env)

	// Always persist (even non-triggering data goes to the time-series /
	// knowledge store for history).
	p.save(ctx, env)
	env.Status = core.StatusStored
	p.updateStatus(ctx, env)

	if !decision.Triggered {
		p.logInfo("no rule triggered", "trace", env.TraceID, "type", env.DataType)
		return decision, nil
	}

	// Rejected items (failed quality/dedup gate expressed as a rule whose
	// action is "reject") are recorded and stopped.
	if decision.Action == "reject" {
		env.Status = core.StatusRejected
		p.updateStatus(ctx, env)
		p.logInfo("rejected by rule", "trace", env.TraceID, "rule", decision.RuleID, "reason", decision.Reason)
		return decision, nil
	}

	// Emit alert for any triggering decision (convergence/dedup is the
	// AlertSink's responsibility).
	if p.alert != nil {
		if err := p.alert.Emit(ctx, env, decision); err != nil {
			p.logWarn("alert emit failed", "err", err)
		}
	}

	if !decision.NeedTask || decision.Task == nil {
		return decision, nil
	}

	// Dispatch. By the time we reach here, any transfer-dependent object
	// has already completed (deferral happens before evaluation), so we
	// dispatch directly.
	if err := p.dispatchWithRetry(ctx, env, decision.Task); err != nil {
		return decision, p.fail(ctx, env, "dispatch", err)
	}
	env.Status = core.StatusDispatched
	p.updateStatus(ctx, env)
	return decision, nil
}

// dispatchWithRetry runs the matching dispatcher with bounded retry.
func (p *Pipeline) dispatchWithRetry(ctx context.Context, env *core.DataEnvelope, task *core.TaskSpec) error {
	d := p.reg.DispatcherFor(task.Type)
	if d == nil {
		return fmt.Errorf("no dispatcher for task type %q", task.Type)
	}
	var lastErr error
	for attempt := 0; attempt <= p.cfg.MaxRetries; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(p.cfg.RetryBackoff):
			}
		}
		if err := d.Dispatch(ctx, env, task); err != nil {
			lastErr = err
			p.logWarn("dispatch attempt failed", "task", task.Type, "attempt", attempt, "err", err)
			continue
		}
		p.logInfo("dispatched", "trace", env.TraceID, "task", task.Type)
		return nil
	}
	return fmt.Errorf("dispatch exhausted retries: %w", lastErr)
}

// ---- transfer.complete handling ----

func (p *Pipeline) enqueuePending(env *core.DataEnvelope) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.pending[env.TraceID] = &pendingTask{
		env:      env,
		deadline: time.Now().Add(p.cfg.TransferTimeout),
	}
}

// onTransferComplete is the EventBus handler. Payload is expected to be
// JSON: {"trace_id":"..."}.
func (p *Pipeline) onTransferComplete(payload []byte) {
	var msg struct {
		TraceID string `json:"trace_id"`
	}
	if err := json.Unmarshal(payload, &msg); err != nil {
		p.logWarn("bad transfer.complete payload", "err", err)
		return
	}
	p.mu.Lock()
	pt, ok := p.pending[msg.TraceID]
	if ok {
		delete(p.pending, msg.TraceID)
	}
	p.mu.Unlock()
	if !ok {
		return
	}
	if pt.env.Ref != nil {
		pt.env.Ref.Complete = true
	}
	ctx := context.Background()
	// Re-run Process now that the object is fully transferred (deferred
	// heavy work: parse/chunk/embed), then resume evaluate + dispatch.
	for _, proc := range p.reg.ProcessorsFor(pt.env.DataType) {
		if err := proc.Process(ctx, pt.env); err != nil {
			_ = p.fail(ctx, pt.env, "reprocess-after-transfer:"+proc.Name(), err)
			return
		}
	}
	if _, err := p.evaluateAndDispatch(ctx, pt.env); err != nil {
		p.logError("resume after transfer failed", "trace", pt.env.TraceID, "err", err)
	}
}

// SweepExpired fails out pending tasks whose transfer never completed.
// Call periodically from a ticker.
func (p *Pipeline) SweepExpired(ctx context.Context) {
	now := time.Now()
	p.mu.Lock()
	var expired []*pendingTask
	for id, pt := range p.pending {
		if now.After(pt.deadline) {
			expired = append(expired, pt)
			delete(p.pending, id)
		}
	}
	p.mu.Unlock()
	for _, pt := range expired {
		_ = p.fail(ctx, pt.env, "transfer-timeout", fmt.Errorf("transfer did not complete within %s", p.cfg.TransferTimeout))
	}
}

// ---- helpers ----

func (p *Pipeline) fail(ctx context.Context, env *core.DataEnvelope, stage string, err error) error {
	env.Status = core.StatusFailed
	p.updateStatus(ctx, env)
	p.logError("stage failed", "trace", env.TraceID, "stage", stage, "err", err)
	return fmt.Errorf("%s: %w", stage, err)
}

func (p *Pipeline) save(ctx context.Context, env *core.DataEnvelope) {
	if p.store == nil {
		return
	}
	if err := p.store.Save(ctx, env); err != nil {
		p.logWarn("store save failed", "err", err)
	}
}

func (p *Pipeline) updateStatus(ctx context.Context, env *core.DataEnvelope) {
	if p.store == nil {
		return
	}
	if err := p.store.UpdateStatus(ctx, env.TraceID, env.Status); err != nil {
		p.logWarn("status update failed", "err", err)
	}
}

func (p *Pipeline) logInfo(msg string, kv ...interface{}) {
	if p.log != nil {
		p.log.Info(msg, kv...)
	}
}
func (p *Pipeline) logWarn(msg string, kv ...interface{}) {
	if p.log != nil {
		p.log.Warn(msg, kv...)
	}
}
func (p *Pipeline) logError(msg string, kv ...interface{}) {
	if p.log != nil {
		p.log.Error(msg, kv...)
	}
}
