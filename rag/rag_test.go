package rag

import (
	"context"
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
	for _, want := range []string{
		"CREATE EXTENSION IF NOT EXISTS vector", "vector(1024)", "USING hnsw", "rag_chunks_ann",
		"tsvector GENERATED ALWAYS", "USING gin (tsv)", "rag_chunks_tsv",
	} {
		if !strings.Contains(ddl, want) {
			t.Fatalf("schema missing %q", want)
		}
	}
}

func TestRRFFuse(t *testing.T) {
	// dense ranks [1,2,3]; keyword ranks [3,4]. Item 3 appears in both lists
	// so its fused score must beat the items that appear in only one.
	dense := []Result{{ID: 1}, {ID: 2}, {ID: 3}}
	kw := []Result{{ID: 3}, {ID: 4}}
	got := RRFFuse([][]Result{dense, kw}, 0)
	if len(got) != 4 {
		t.Fatalf("want 4 fused results, got %d", len(got))
	}
	if got[0].ID != 3 {
		t.Fatalf("item present in both lists must rank first; got ID %d", got[0].ID)
	}
	// score check: ID 3 = 1/(60+3) + 1/(60+1); ID 1 = 1/(60+1) only.
	if !(got[0].Score > got[1].Score) {
		t.Fatalf("fused overlap score %v should exceed runner-up %v", got[0].Score, got[1].Score)
	}
	// topN caps the output.
	if capped := RRFFuse([][]Result{dense, kw}, 2); len(capped) != 2 {
		t.Fatalf("topN=2 should cap to 2, got %d", len(capped))
	}
}

// fakeStore returns canned dense/keyword slices so Hybrid's fusion +
// fallback logic is testable without a database.
type fakeStore struct {
	dense, kw []Result
}

func (f *fakeStore) Upsert(context.Context, []Chunk, [][]float32) error { return nil }
func (f *fakeStore) DeleteSource(context.Context, string, string, string) error {
	return nil
}
func (f *fakeStore) Search(_ context.Context, _ string, _ []float32, k int, _ Filter) ([]Result, error) {
	if len(f.dense) > k {
		return f.dense[:k], nil
	}
	return f.dense, nil
}
func (f *fakeStore) SearchKeyword(_ context.Context, _ string, kws []string, k int, _ Filter) ([]Result, error) {
	if len(kws) == 0 {
		return nil, nil
	}
	if len(f.kw) > k {
		return f.kw[:k], nil
	}
	return f.kw, nil
}

func TestHybridFallback(t *testing.T) {
	ctx := context.Background()
	s := &fakeStore{
		dense: []Result{{ID: 1}, {ID: 2}, {ID: 3}, {ID: 4}},
		kw:    []Result{{ID: 9}},
	}
	// No keywords → dense only, trimmed to k.
	got, err := Hybrid(ctx, s, "kb", nil, nil, 2, Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != 1 {
		t.Fatalf("empty-keyword path should return dense[:k]; got %+v", got)
	}
	// Keywords present but store returns no keyword hits → still dense[:k].
	noKW := &fakeStore{dense: s.dense, kw: nil}
	got, err = Hybrid(ctx, noKW, "kb", nil, []string{"案号"}, 2, Filter{})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ID != 1 {
		t.Fatalf("no-keyword-hit path should fall back to dense[:k]; got %+v", got)
	}
	// Keywords + hits → fused (ID 9 from keyword leg appears).
	got, err = Hybrid(ctx, s, "kb", nil, []string{"案号"}, 4, Filter{})
	if err != nil {
		t.Fatal(err)
	}
	var sawKW bool
	for _, r := range got {
		if r.ID == 9 {
			sawKW = true
		}
	}
	if !sawKW {
		t.Fatalf("fused result must include the keyword-only hit; got %+v", got)
	}
}
