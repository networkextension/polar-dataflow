// Package legal is a sample vertical plugin set for the lawyer project:
// a case/statute document referenced by object-storage URI, parsed into
// structured fields, chunked with a legal-aware strategy, embedded, and
// (on transfer completion + quality/dedup gate) written to a private
// RAG store. Demonstrates the document/knowledge path: large objects,
// OnTransferDone, and the rag_write task type.
package legal

import (
	"context"
	"fmt"

	"github.com/networkextension/polar-dataflow/core"
)

// Classifier tags legal documents based on the envelope's object ref
// metadata (set by the intake adapter).
type Classifier struct{}

func (Classifier) Name() string { return "legal-classifier" }

func (Classifier) Match(env *core.DataEnvelope) bool {
	if env.Ref == nil {
		return false
	}
	kind, _ := env.MetaString("doc_kind") // intake sets "case" | "statute" | "precedent"
	return kind != ""
}

func (Classifier) Classify(_ context.Context, env *core.DataEnvelope) error {
	kind, _ := env.MetaString("doc_kind")
	switch kind {
	case "case", "precedent":
		env.DataType = "legal_case"
	case "statute":
		env.DataType = "statute"
	default:
		return fmt.Errorf("unknown legal doc kind %q", kind)
	}
	return nil
}

// Parser extracts structured fields and chunks the document. In a real
// system this reads env.Ref.URI from object storage; here it is a stub
// showing where parsing/chunking/embedding hooks in. Only runs once the
// transfer is complete (large files are handled via OnTransferDone, but
// Process may also run pre-transfer for cheap metadata — this parser
// guards on completeness for the heavy work).
type Parser struct {
	// Extract pulls structured fields + raw text from the stored object.
	Extract func(uri string) (fields map[string]interface{}, text string, err error)
	// Chunk splits text using a legal-aware strategy (by article / by
	// section). Strategy name comes from the rule's task params.
	Chunk func(text, strategy string) []string
	// Embed turns chunks into vectors via a LOCAL model (no external API:
	// case data must not leave the deployment).
	Embed func(chunks []string) ([][]float32, error)
}

func (Parser) Name() string { return "legal-parser" }
func (Parser) Applies(dataType string) bool {
	return dataType == "legal_case" || dataType == "statute"
}

func (p Parser) Process(_ context.Context, env *core.DataEnvelope) error {
	if env.Ref == nil {
		return fmt.Errorf("legal doc has no object ref")
	}
	// Heavy parsing waits until the file is fully transferred.
	if !env.TransferComplete() {
		env.Meta["parse_deferred"] = true
		return nil
	}

	if p.Extract != nil {
		fields, text, err := p.Extract(env.Ref.URI)
		if err != nil {
			return fmt.Errorf("extract: %w", err)
		}
		for k, v := range fields {
			env.Meta[k] = v
		}
		env.Meta["text_len"] = len(text)

		// A real quality score would come from extraction confidence,
		// field completeness, OCR confidence, etc. Stubbed here.
		env.Meta["quality_score"] = scoreQuality(fields, text)
		// Dedup score (0 = unique, 1 = exact dup) from a similarity probe
		// against the target KB. Stubbed.
		env.Meta["dedup_score"] = 0.0

		if p.Chunk != nil {
			strategy, _ := env.MetaString("chunk_strategy")
			if strategy == "" {
				strategy = "by_article"
			}
			chunks := p.Chunk(text, strategy)
			env.Meta["chunk_count"] = len(chunks)
			if p.Embed != nil {
				vecs, err := p.Embed(chunks)
				if err != nil {
					return fmt.Errorf("embed: %w", err)
				}
				// Stash chunks+vectors for the dispatcher to write.
				env.Meta["_chunks"] = chunks
				env.Meta["_vectors"] = vecs
			}
		}
	}
	return nil
}

// RAGDispatcher handles the "rag_write" task: persist chunks + vectors +
// metadata to the private vector store / metadata DB / full-text index.
type RAGDispatcher struct {
	// Write performs the three-way write (vector + metadata + index).
	Write func(kb string, env *core.DataEnvelope, chunks []string, vectors [][]float32, meta map[string]interface{}) error
}

func (RAGDispatcher) Name() string                 { return "legal-rag-dispatcher" }
func (RAGDispatcher) Handles(taskType string) bool { return taskType == "rag_write" }

func (d RAGDispatcher) Dispatch(_ context.Context, env *core.DataEnvelope, task *core.TaskSpec) error {
	kb, _ := task.Params["target_kb"].(string)
	if kb == "" {
		return fmt.Errorf("rag_write missing target_kb")
	}
	chunks, _ := env.Meta["_chunks"].([]string)
	vecs, _ := env.Meta["_vectors"].([][]float32)
	if len(chunks) == 0 {
		return fmt.Errorf("nothing to write: no chunks")
	}
	// Carry provenance/version metadata for legal traceability.
	meta := map[string]interface{}{
		"source_uri":     env.Ref.URI,
		"data_type":      env.DataType,
		"tenant_id":      env.TenantID,
		"effective_date": env.Meta["effective_date"], // statute version/validity
		"status":         env.Meta["status"],         // in-force / repealed
		"case_no":        env.Meta["case_no"],
	}
	if d.Write != nil {
		return d.Write(kb, env, chunks, vecs, meta)
	}
	return nil
}

func scoreQuality(fields map[string]interface{}, text string) float64 {
	if len(text) == 0 {
		return 0
	}
	required := []string{"title"}
	have := 0
	for _, k := range required {
		if _, ok := fields[k]; ok {
			have++
		}
	}
	return float64(have) / float64(len(required))
}
