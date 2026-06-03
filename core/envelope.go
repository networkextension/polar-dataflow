// Package core defines the framework's universal data model and the
// plugin interfaces that every vertical (building / theater / legal ...)
// implements. The framework owns orchestration, scheduling, retry,
// tracing and config; plugins own business know-how.
package core

import "time"

// ProcessStatus is the lifecycle state of a single piece of data as it
// flows through the pipeline. Persisted for traceability/audit.
type ProcessStatus string

const (
	StatusReceived   ProcessStatus = "RECEIVED"
	StatusClassified ProcessStatus = "CLASSIFIED"
	StatusProcessed  ProcessStatus = "PROCESSED"
	StatusEvaluated  ProcessStatus = "EVALUATED"
	StatusStored     ProcessStatus = "STORED"
	StatusDispatched ProcessStatus = "DISPATCHED"
	StatusWaiting    ProcessStatus = "WAITING_TRANSFER" // big file/stream, awaiting transfer.complete
	StatusFailed     ProcessStatus = "FAILED"
	StatusRejected   ProcessStatus = "REJECTED" // failed evaluation (e.g. low quality / duplicate)
)

// TriggerMode controls when a task is dispatched.
type TriggerMode int

const (
	// Immediate: data is self-contained (a sensor reading, an EEG sample);
	// dispatch as soon as evaluation triggers.
	Immediate TriggerMode = iota
	// OnTransferDone: payload references a large object still being
	// transferred (video clip, big case PDF). Wait for transfer.complete
	// before dispatching the task.
	OnTransferDone
)

// PayloadRef points at a large object in object storage instead of
// carrying the bytes inline. Lets the same envelope carry a few bytes
// (a temperature reading) or a multi-GB case file by reference.
type PayloadRef struct {
	URI      string // e.g. minio://bucket/key
	Size     int64
	MIMEType string
	Complete bool // set true once transfer.complete is observed
}

// DataEnvelope is the framework's single unit of work. Every source
// normalizes its input into this envelope; all stages operate on it.
type DataEnvelope struct {
	TraceID  string // full-chain correlation id
	TenantID string // multi-tenant isolation
	Source   string // building-bms / theater-eeg / legal-intake
	DataType string // temperature / eeg / legal_case / statute ...

	// DeviceTS is the source/device timestamp in unix milliseconds.
	// Critical for theater (ms-level cross-modal alignment).
	DeviceTS int64
	RecvAt   int64 // framework receive time, unix millis

	// Payload is the inline raw payload for small data. For large objects
	// it may be empty and Ref is used instead.
	Payload []byte
	Ref     *PayloadRef // non-nil for large/streamed objects

	// Meta holds structured fields extracted during Classify/Process,
	// plus computed signals used by Evaluate (e.g. quality_score).
	Meta map[string]interface{}

	Status ProcessStatus
}

// NewEnvelope builds an envelope with sane defaults.
func NewEnvelope(traceID, tenantID, source string, payload []byte) *DataEnvelope {
	now := time.Now().UnixMilli()
	return &DataEnvelope{
		TraceID:  traceID,
		TenantID: tenantID,
		Source:   source,
		Payload:  payload,
		RecvAt:   now,
		DeviceTS: now,
		Meta:     make(map[string]interface{}),
		Status:   StatusReceived,
	}
}

// IsLargeObject reports whether this envelope refers to an external
// large object that may need OnTransferDone handling.
func (e *DataEnvelope) IsLargeObject() bool {
	return e.Ref != nil
}

// TransferComplete reports whether a referenced large object has
// finished transferring. Inline payloads are always "complete".
func (e *DataEnvelope) TransferComplete() bool {
	if e.Ref == nil {
		return true
	}
	return e.Ref.Complete
}

// MetaFloat is a typed convenience accessor used by the rule engine.
func (e *DataEnvelope) MetaFloat(key string) (float64, bool) {
	v, ok := e.Meta[key]
	if !ok {
		return 0, false
	}
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	default:
		return 0, false
	}
}

// MetaString is a typed convenience accessor.
func (e *DataEnvelope) MetaString(key string) (string, bool) {
	v, ok := e.Meta[key]
	if !ok {
		return "", false
	}
	s, ok := v.(string)
	return s, ok
}

// MetaBool is a typed convenience accessor.
func (e *DataEnvelope) MetaBool(key string) (bool, bool) {
	v, ok := e.Meta[key]
	if !ok {
		return false, false
	}
	b, ok := v.(bool)
	return b, ok
}
