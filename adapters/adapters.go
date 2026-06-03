// Package adapters provides concrete, dependency-light implementations
// of the dataflow core ports (Logger, EventBus, Store). They use only
// the standard library, so importing them never pulls a Postgres driver
// or polar-sdk. The dock/sdk-coupled adapters (AlertSink → work order,
// rag.Embedder → llm-proxy) live in the vertical service, where the SDK
// is already a dependency — that keeps polar-dataflow's core deps to
// just expr-lang.
package adapters

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"

	"github.com/networkextension/polar-dataflow/core"
)

// ── Logger ──────────────────────────────────────────────────────────

// SlogLogger adapts log/slog to core.Logger.
type SlogLogger struct{ L *slog.Logger }

func NewSlogLogger() SlogLogger { return SlogLogger{L: slog.Default()} }

func (s SlogLogger) Info(msg string, kv ...interface{})  { s.L.Info(msg, kv...) }
func (s SlogLogger) Warn(msg string, kv ...interface{})  { s.L.Warn(msg, kv...) }
func (s SlogLogger) Error(msg string, kv ...interface{}) { s.L.Error(msg, kv...) }

// ── EventBus (in-process) ───────────────────────────────────────────

// InProcBus is a simple in-process pub/sub for single-binary deploys
// (most vertical installs are one box). Swap for a NATS/MQTT adapter
// when transfer.complete must cross process/host boundaries. Handlers
// run in their own goroutine so a slow subscriber can't block publish.
type InProcBus struct {
	mu   sync.RWMutex
	subs map[string][]func([]byte)
}

func NewInProcBus() *InProcBus { return &InProcBus{subs: map[string][]func([]byte){}} }

func (b *InProcBus) Publish(_ context.Context, topic string, payload []byte) error {
	b.mu.RLock()
	hs := append([]func([]byte){}, b.subs[topic]...)
	b.mu.RUnlock()
	for _, h := range hs {
		go h(payload)
	}
	return nil
}

func (b *InProcBus) Subscribe(topic string, handler func([]byte)) error {
	b.mu.Lock()
	b.subs[topic] = append(b.subs[topic], handler)
	b.mu.Unlock()
	return nil
}

// ── Store (Postgres via database/sql) ───────────────────────────────

// PGStore persists envelopes + their status transitions for traceability
// and audit. Driver-agnostic: the vertical passes its own *sql.DB.
type PGStore struct {
	DB    *sql.DB
	Table string // default "df_envelopes"
}

func NewPGStore(db *sql.DB) *PGStore { return &PGStore{DB: db, Table: "df_envelopes"} }

func (s *PGStore) table() string {
	if s.Table == "" {
		return "df_envelopes"
	}
	return s.Table
}

// StoreSchema is the DDL for the envelope/audit table.
func StoreSchema(table string) string {
	if table == "" {
		table = "df_envelopes"
	}
	return fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %[1]s (
    trace_id   TEXT PRIMARY KEY,
    tenant_id  TEXT NOT NULL,
    source     TEXT NOT NULL,
    data_type  TEXT NOT NULL DEFAULT '',
    status     TEXT NOT NULL,
    meta       JSONB NOT NULL DEFAULT '{}',
    recv_at    BIGINT NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS %[1]s_tenant ON %[1]s (tenant_id, updated_at DESC);
-- Append-only status trail for tamper-evident audit (政企/等保).
CREATE TABLE IF NOT EXISTS %[1]s_audit (
    id         BIGSERIAL PRIMARY KEY,
    trace_id   TEXT NOT NULL,
    status     TEXT NOT NULL,
    at         TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS %[1]s_audit_trace ON %[1]s_audit (trace_id, at);`, table)
}

func (s *PGStore) Save(ctx context.Context, env *core.DataEnvelope) error {
	meta, _ := json.Marshal(env.Meta)
	_, err := s.DB.ExecContext(ctx, fmt.Sprintf(`INSERT INTO %s (trace_id, tenant_id, source, data_type, status, meta, recv_at)
	     VALUES ($1,$2,$3,$4,$5,$6,$7)
	     ON CONFLICT (trace_id) DO UPDATE SET
	       data_type=EXCLUDED.data_type, status=EXCLUDED.status, meta=EXCLUDED.meta, updated_at=now()`, s.table()),
		env.TraceID, env.TenantID, env.Source, env.DataType, string(env.Status), meta, env.RecvAt)
	return err
}

func (s *PGStore) UpdateStatus(ctx context.Context, traceID string, status core.ProcessStatus) error {
	if _, err := s.DB.ExecContext(ctx,
		fmt.Sprintf(`UPDATE %s SET status=$2, updated_at=now() WHERE trace_id=$1`, s.table()),
		traceID, string(status)); err != nil {
		return err
	}
	_, err := s.DB.ExecContext(ctx,
		fmt.Sprintf(`INSERT INTO %s_audit (trace_id, status) VALUES ($1,$2)`, s.table()),
		traceID, string(status))
	return err
}
