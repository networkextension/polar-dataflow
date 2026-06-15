// Package rag is the canonical retrieval-augmented-generation layer for
// the dataflow verticals: one chunk schema, one set of upsert/search
// ops, backed by pgvector inside each vertical's own Postgres database.
//
// The framework depends only on these interfaces; the concrete pgvector
// impl (PGStore) is here but takes a *sql.DB the caller owns, so the
// library never pins a Postgres driver — the vertical service chooses
// pgx or lib/pq. Per the platform's "数据不出域" rule, chunks and their
// embeddings stay in the vertical's tenant database.
package rag

import "context"

// Chunk is one embeddable unit of a document plus the provenance the
// legal/compliance domains require (version, effective date, status,
// source object). Meta carries domain-specific fields (case_no, article,
// floor, scene…) used for metadata-filtered search.
type Chunk struct {
	KB        string                 // knowledge base, e.g. "divorce-precedents"
	TenantID  string                 // multi-tenant isolation
	SourceURI string                 // the asset object this chunk came from
	DataType  string                 // legal_case / statute / sop / script ...
	Index     int                    // chunk ordinal within the source
	Text      string                 // chunk text
	Meta      map[string]interface{} // case_no, effective_date, status, 溯源 ...
}

// Result is one search hit: the stored chunk plus its similarity score
// (cosine: higher = closer).
type Result struct {
	Chunk
	ID    int64
	Score float64
}

// Filter narrows a search beyond vector similarity. Empty fields are
// ignored. Legal effectivity ("only in-force statutes as of date X") is
// expressed via Status + the Meta-backed filters the impl applies.
type Filter struct {
	TenantID string            // REQUIRED in practice — never search across tenants
	DataType string            // e.g. "statute"
	Status   string            // e.g. "in_force"
	MetaEq   map[string]string // exact-match meta filters (jsonb ->> key = value)
}

// Store is the RAG persistence + retrieval port. A vertical writes
// chunks (after embed) and searches by query vector + filter.
type Store interface {
	// Upsert writes chunks + their vectors for one source. vectors[i]
	// must align with chunks[i]. Re-ingesting the same source replaces
	// its prior chunks (idempotent by source_uri+kb+tenant).
	Upsert(ctx context.Context, chunks []Chunk, vectors [][]float32) error
	// Search returns the top-k nearest chunks for queryVec under filter.
	Search(ctx context.Context, kb string, queryVec []float32, k int, f Filter) ([]Result, error)
	// SearchKeyword ranks chunks by how many of the exact keywords they
	// contain (the sparse leg of hybrid retrieval). Same kb + filter
	// isolation as Search. Empty keywords returns no rows. Keyword
	// EXTRACTION is the caller's job (domain-specific) — see Hybrid.
	SearchKeyword(ctx context.Context, kb string, keywords []string, k int, f Filter) ([]Result, error)
	// DeleteSource removes every chunk of a source (e.g. statute repealed,
	// document withdrawn).
	DeleteSource(ctx context.Context, kb, tenantID, sourceURI string) error
}

// Embedder turns text into vectors using an ON-PREM model (case/tenant
// data must not leave the deployment). The default vertical impl routes
// through dock's LLM proxy to a local embedding llm_config (billed +
// audited, model stays on the box); air-gapped sites swap in a direct
// local-model impl. Dim must match the rag_chunks embedding column.
type Embedder interface {
	Embed(ctx context.Context, texts []string) ([][]float32, error)
	Dim() int
}
