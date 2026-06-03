package core

import "context"

// ---- Stage plugin interfaces ----
//
// Each stage is a plugin point. The framework discovers registered
// implementations and runs them in order. Plugins are stateless w.r.t.
// the pipeline (any state they need is their own concern) and must be
// safe for concurrent use.

// Classifier identifies what kind of data an envelope carries and
// extracts initial structured fields into env.Meta. Match lets the
// framework pick the right classifier for a given envelope.
type Classifier interface {
	// Name is a stable identifier for logging/metrics.
	Name() string
	// Match reports whether this classifier handles the envelope.
	Match(env *DataEnvelope) bool
	// Classify sets env.DataType and seeds env.Meta.
	Classify(ctx context.Context, env *DataEnvelope) error
}

// Processor performs "simple processing": cleaning, unit conversion,
// downsampling, parsing/chunking/vectorizing a document, etc. A
// Processor declares which DataTypes it applies to via Applies.
type Processor interface {
	Name() string
	// Applies reports whether this processor runs for the given dataType.
	Applies(dataType string) bool
	Process(ctx context.Context, env *DataEnvelope) error
}

// Evaluator decides whether the envelope triggers something, and if so
// what task (if any) to dispatch. This is where thresholds / quality
// gates / dedup checks live. The actual rules are config-driven (see
// the rules package); an Evaluator may delegate to the rule engine.
type Evaluator interface {
	Name() string
	Applies(dataType string) bool
	Evaluate(ctx context.Context, env *DataEnvelope) (*Decision, error)
}

// Dispatcher executes the task produced by an Evaluator: raise an alert,
// capture a clip, write to RAG, kick off analysis, etc.
type Dispatcher interface {
	Name() string
	// Handles reports whether this dispatcher executes the given task type.
	Handles(taskType string) bool
	Dispatch(ctx context.Context, env *DataEnvelope, task *TaskSpec) error
}

// DeferralAdvisor is an optional interface an Evaluator may implement to
// tell the pipeline that, for a given data type, evaluation should be
// deferred until a referenced large object finishes transferring. This
// resolves the chicken-and-egg where the gating rule itself needs the
// fully-transferred object (e.g. a parsed quality score) before it can
// decide. When an advisor returns true and the envelope's transfer is
// incomplete, the pipeline parks the item and re-runs Process+Evaluate
// on transfer.complete.
type DeferralAdvisor interface {
	NeedsTransferComplete(dataType string) bool
}

// ---- Decision / Task ----

// Decision is the output of the Evaluate stage.
type Decision struct {
	Triggered   bool        // did any rule fire?
	RuleID      string      // which rule fired
	Action      string      // allow / alert / capture / analyze / ingest_rag
	Severity    int         // 0 = info ... higher = worse
	NeedTask    bool        // should a task be dispatched?
	TriggerMode TriggerMode // Immediate vs OnTransferDone
	Task        *TaskSpec   // task to dispatch when NeedTask is true
	Reason      string      // human-readable explanation (for audit)
}

// TaskSpec describes a unit of downstream work. Params carries
// task-type-specific configuration (e.g. target knowledge base,
// embedding model, chunk strategy for rag_write).
type TaskSpec struct {
	Type     string                 // alert / capture / analyze / rag_write ...
	Priority string                 // low / normal / high
	Params   map[string]interface{} // task-specific config from the rule
}

// ---- Infrastructure ports (framework depends on these abstractions,
// not on concrete DBs/queues — keeps it air-gap / swap friendly) ----

// Store persists envelopes and their state transitions. A real impl
// might write metrics to TimescaleDB and state to Postgres.
type Store interface {
	Save(ctx context.Context, env *DataEnvelope) error
	UpdateStatus(ctx context.Context, traceID string, status ProcessStatus) error
}

// AlertSink emits alerts (with convergence/dedup applied upstream).
type AlertSink interface {
	Emit(ctx context.Context, env *DataEnvelope, d *Decision) error
}

// EventBus lets the framework publish/subscribe lifecycle events such
// as transfer.complete and service online/offline.
type EventBus interface {
	Publish(ctx context.Context, topic string, payload []byte) error
	// Subscribe invokes handler for each message on topic. Implementations
	// back this with NATS/Kafka/MQTT.
	Subscribe(topic string, handler func(payload []byte)) error
}

// Logger is a minimal structured logger port.
type Logger interface {
	Info(msg string, kv ...interface{})
	Warn(msg string, kv ...interface{})
	Error(msg string, kv ...interface{})
}
