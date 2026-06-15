package rag

import (
	"context"
	"sort"
)

// RRFK is the Reciprocal Rank Fusion constant (the standard 60). An item's
// fused score is Σ 1/(RRFK + rank) across the ranked lists it appears in —
// no per-list weight tuning, robust across score scales.
const RRFK = 60

// RRFFuse merges ranked result lists by Reciprocal Rank Fusion, keyed by
// Result.ID. The fused score is exposed on each returned Result.Score; ties
// break by ID for a stable order. topN<=0 returns all. Generalized from
// polar-lawyer's rrfFuse.
func RRFFuse(lists [][]Result, topN int) []Result {
	type acc struct {
		hit   Result
		score float64
	}
	by := map[int64]*acc{}
	for _, list := range lists {
		for rank, r := range list {
			a := by[r.ID]
			if a == nil {
				a = &acc{hit: r}
				by[r.ID] = a
			}
			a.score += 1.0 / float64(RRFK+rank+1)
		}
	}
	merged := make([]*acc, 0, len(by))
	for _, a := range by {
		merged = append(merged, a)
	}
	sort.Slice(merged, func(i, j int) bool {
		if merged[i].score != merged[j].score {
			return merged[i].score > merged[j].score
		}
		return merged[i].hit.ID < merged[j].hit.ID
	})
	out := make([]Result, 0, len(merged))
	for i, a := range merged {
		if topN > 0 && i >= topN {
			break
		}
		h := a.hit
		h.Score = a.score // expose the fused score
		out = append(out, h)
	}
	return out
}

// Hybrid runs dense ANN ⊕ exact-keyword retrieval, fused by RRF. The caller
// supplies the query vector AND the already-extracted keywords — keyword
// extraction is domain-specific (legal 案号/法条号/金额, building room/scene…)
// and belongs in the vertical, not here. When there are no keywords (or no
// keyword hits) the dense ranking is already correct, so we just trim it to
// k. Generalized from polar-lawyer's flow.hybridSearch.
func Hybrid(ctx context.Context, s Store, kb string, queryVec []float32, keywords []string, k int, f Filter) ([]Result, error) {
	if k <= 0 {
		k = 8
	}
	dense, err := s.Search(ctx, kb, queryVec, k*2, f)
	if err != nil {
		return nil, err
	}
	trim := func(rs []Result) []Result {
		if len(rs) > k {
			return rs[:k]
		}
		return rs
	}
	if len(keywords) == 0 {
		return trim(dense), nil
	}
	kw, err := s.SearchKeyword(ctx, kb, keywords, k*2, f)
	if err != nil {
		return nil, err
	}
	if len(kw) == 0 {
		return trim(dense), nil
	}
	return RRFFuse([][]Result{dense, kw}, k), nil
}
