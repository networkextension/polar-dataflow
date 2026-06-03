package rag

import (
	"strings"
	"testing"
)

func TestVecLiteral(t *testing.T) {
	got := vecLiteral([]float32{0.1, -0.25, 3})
	if got != "[0.1,-0.25,3]" {
		t.Fatalf("vecLiteral = %q", got)
	}
	if vecLiteral(nil) != "[]" {
		t.Fatalf("empty vec = %q", vecLiteral(nil))
	}
}

func TestSanitizeKey(t *testing.T) {
	if got := sanitizeKey("case_no'; DROP TABLE x;--"); got != "case_noDROPTABLEx" {
		t.Fatalf("sanitizeKey = %q", got)
	}
}

func TestSchemaShape(t *testing.T) {
	ddl := Schema("rag_chunks", 1024)
	for _, want := range []string{"CREATE EXTENSION IF NOT EXISTS vector", "vector(1024)", "USING hnsw", "rag_chunks_ann"} {
		if !strings.Contains(ddl, want) {
			t.Fatalf("schema missing %q", want)
		}
	}
}
