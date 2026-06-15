package rag

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"
	"strings"
)

// PGStore is a pgvector-backed Store. It uses database/sql only, so the
// library pins no driver — the vertical passes its own *sql.DB (pgx or
// lib/pq). Vectors are sent as pgvector text literals ("[0.1,0.2,...]")
// cast to ::vector, which both drivers handle as a string parameter.
//
// Run Schema(dim) once (or ship it as a migration) to create the table +
// HNSW index in the vertical's database.
type PGStore struct {
	DB    *sql.DB
	Table string // default "rag_chunks"
}

func NewPGStore(db *sql.DB) *PGStore { return &PGStore{DB: db, Table: "rag_chunks"} }

func (s *PGStore) table() string {
	if s.Table == "" {
		return "rag_chunks"
	}
	return s.Table
}

// Schema returns the DDL for the chunk table at the given embedding
// dimension. CREATE EXTENSION vector must already be installed (a
// superuser one-time step in the vertical DB).
func Schema(table string, dim int) string {
	if table == "" {
		table = "rag_chunks"
	}
	return fmt.Sprintf(`CREATE EXTENSION IF NOT EXISTS vector;
CREATE TABLE IF NOT EXISTS %[1]s (
    id          BIGSERIAL PRIMARY KEY,
    kb          TEXT NOT NULL,
    tenant_id   TEXT NOT NULL,
    source_uri  TEXT NOT NULL,
    data_type   TEXT NOT NULL,
    chunk_idx   INT  NOT NULL,
    text        TEXT NOT NULL,
    embedding   vector(%[2]d) NOT NULL,
    tsv         tsvector GENERATED ALWAYS AS (to_tsvector('simple', text)) STORED,
    meta        JSONB NOT NULL DEFAULT '{}',
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS %[1]s_ann   ON %[1]s USING hnsw (embedding vector_cosine_ops);
CREATE INDEX IF NOT EXISTS %[1]s_tsv   ON %[1]s USING gin (tsv);
CREATE INDEX IF NOT EXISTS %[1]s_scope ON %[1]s (kb, tenant_id, data_type);
CREATE INDEX IF NOT EXISTS %[1]s_src   ON %[1]s (kb, tenant_id, source_uri);`, table, dim)
}

// vecLiteral encodes a float32 slice as a pgvector text literal.
func vecLiteral(v []float32) string {
	var b strings.Builder
	b.WriteByte('[')
	for i, f := range v {
		if i > 0 {
			b.WriteByte(',')
		}
		b.WriteString(strconv.FormatFloat(float64(f), 'f', -1, 32))
	}
	b.WriteByte(']')
	return b.String()
}

func (s *PGStore) Upsert(ctx context.Context, chunks []Chunk, vectors [][]float32) error {
	if len(chunks) == 0 {
		return nil
	}
	if len(chunks) != len(vectors) {
		return fmt.Errorf("chunks/vectors length mismatch: %d vs %d", len(chunks), len(vectors))
	}
	c0 := chunks[0]
	tx, err := s.DB.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	// Idempotent re-ingest: clear the source's prior chunks first.
	if _, err := tx.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM %s WHERE kb=$1 AND tenant_id=$2 AND source_uri=$3`, s.table()),
		c0.KB, c0.TenantID, c0.SourceURI); err != nil {
		return fmt.Errorf("clear source: %w", err)
	}
	q := fmt.Sprintf(`INSERT INTO %s (kb, tenant_id, source_uri, data_type, chunk_idx, text, embedding, meta)
	                  VALUES ($1,$2,$3,$4,$5,$6,$7::vector,$8)`, s.table())
	for i, ch := range chunks {
		meta, mErr := jsonMeta(ch.Meta)
		if mErr != nil {
			return mErr
		}
		if _, err := tx.ExecContext(ctx, q,
			ch.KB, ch.TenantID, ch.SourceURI, ch.DataType, ch.Index, ch.Text,
			vecLiteral(vectors[i]), meta); err != nil {
			return fmt.Errorf("insert chunk %d: %w", i, err)
		}
	}
	return tx.Commit()
}

func (s *PGStore) Search(ctx context.Context, kb string, queryVec []float32, k int, f Filter) ([]Result, error) {
	if k <= 0 {
		k = 8
	}
	// $1 is the query vector; scopeWhere appends kb + filter from $2 on.
	args := []interface{}{vecLiteral(queryVec)}
	args, where := scopeWhere(args, kb, f)
	args = append(args, k)
	q := fmt.Sprintf(`SELECT id, kb, tenant_id, source_uri, data_type, chunk_idx, text, meta,
	                         1 - (embedding <=> $1::vector) AS score
	                  FROM %s WHERE %s
	                  ORDER BY embedding <=> $1::vector
	                  LIMIT $%d`, s.table(), strings.Join(where, " AND "), len(args))
	rows, err := s.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanResults(rows)
}

// SearchKeyword is the sparse leg of hybrid retrieval: it scores chunks by
// how many of the exact keywords they contain (text ILIKE), under the same
// kb + filter isolation as Search. Chinese tokenizes poorly under PG's
// default FTS parser, so the keyword match is exact-substring on the
// caller-extracted high-signal tokens rather than tsvector BM25 (the tsv
// column is provisioned for a future zhparser/BM25 leg). Generalized from
// polar-lawyer's caseStore.searchKeyword.
func (s *PGStore) SearchKeyword(ctx context.Context, kb string, keywords []string, k int, f Filter) ([]Result, error) {
	if len(keywords) == 0 {
		return nil, nil
	}
	if k <= 0 {
		k = 8
	}
	args, where := scopeWhere(nil, kb, f)
	var orParts, scoreParts []string
	for _, kw := range keywords {
		args = append(args, "%"+kw+"%")
		p := fmt.Sprintf("text ILIKE $%d", len(args))
		orParts = append(orParts, p)
		scoreParts = append(scoreParts, "(CASE WHEN "+p+" THEN 1 ELSE 0 END)")
	}
	where = append(where, "("+strings.Join(orParts, " OR ")+")")
	args = append(args, k)
	q := fmt.Sprintf(`SELECT id, kb, tenant_id, source_uri, data_type, chunk_idx, text, meta,
	                         (%s)::float AS score
	                  FROM %s WHERE %s
	                  ORDER BY score DESC, id
	                  LIMIT $%d`, strings.Join(scoreParts, " + "), s.table(),
		strings.Join(where, " AND "), len(args))
	rows, err := s.DB.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	return scanResults(rows)
}

// scopeWhere appends the kb + Filter predicates to args (using positional
// placeholders from the current arg count + 1) and returns the grown args
// plus the WHERE conditions. Shared by Search and SearchKeyword so both
// legs enforce identical tenant/data_type/status/meta isolation in SQL.
func scopeWhere(args []interface{}, kb string, f Filter) ([]interface{}, []string) {
	args = append(args, kb)
	where := []string{fmt.Sprintf("kb = $%d", len(args))}
	add := func(cond string, v interface{}) {
		args = append(args, v)
		where = append(where, fmt.Sprintf(cond, len(args)))
	}
	if f.TenantID != "" {
		add("tenant_id = $%d", f.TenantID)
	}
	if f.DataType != "" {
		add("data_type = $%d", f.DataType)
	}
	if f.Status != "" {
		add("meta->>'status' = $%d", f.Status)
	}
	for key, val := range f.MetaEq {
		// key is a column-name-safe meta key; value is parameterized.
		args = append(args, val)
		where = append(where, fmt.Sprintf("meta->>'%s' = $%d", sanitizeKey(key), len(args)))
	}
	return args, where
}

// scanResults reads the common Result column set (Search + SearchKeyword
// select identical columns + a trailing score).
func scanResults(rows *sql.Rows) ([]Result, error) {
	var out []Result
	for rows.Next() {
		var r Result
		var metaRaw []byte
		if err := rows.Scan(&r.ID, &r.KB, &r.TenantID, &r.SourceURI, &r.DataType,
			&r.Index, &r.Text, &metaRaw, &r.Score); err != nil {
			return nil, err
		}
		r.Meta = parseMeta(metaRaw)
		out = append(out, r)
	}
	return out, rows.Err()
}

func (s *PGStore) DeleteSource(ctx context.Context, kb, tenantID, sourceURI string) error {
	_, err := s.DB.ExecContext(ctx,
		fmt.Sprintf(`DELETE FROM %s WHERE kb=$1 AND tenant_id=$2 AND source_uri=$3`, s.table()),
		kb, tenantID, sourceURI)
	return err
}

// sanitizeKey strips anything but [a-zA-Z0-9_] from a meta key so it
// can't break out of the jsonb path literal (values are parameterized;
// keys are validated here).
func sanitizeKey(k string) string {
	var b strings.Builder
	for _, r := range k {
		if r == '_' || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		}
	}
	return b.String()
}
