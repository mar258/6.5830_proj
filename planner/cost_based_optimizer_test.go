package planner

import (
	"testing"

	"mit.edu/dsg/godb/common"
)

// Test suite helpers
type JoinEstimatorSet struct {
	Estimators []JoinCostEstimator
}

// Enumerates through every algorithm and returns all applicable ones.
func (s *JoinEstimatorSet) ApplicableEstimates(input JoinCostInput) []NamedEstimate {
	var out []NamedEstimate
	for _, est := range s.Estimators {
		if est.CanApply(input) {
			out = append(out, NamedEstimate{
				Name:     est.Name(),
				Estimate: est.Estimate(input),
			})
		}
	}
	return out
}

type NamedEstimate struct {
	Name     string
	Estimate JoinCostEstimate
}

// Cost model comparison unit tests (2-way), does changing stats impact decision?
func TestSimpleEquiJoin(t *testing.T) {
	input := JoinCostInput{
		LeftRows:   1_000_000,
		RightRows:  1_000_000,
		JoinType:   Inner,
		Predicates: []Expr{
			NewComparisonExpression(
				NewConstantValueExpression(common.NewIntValue(0)),
				NewConstantValueExpression(common.NewIntValue(0)),
				Equal,
			),
		},
		AvailableBuffers: 100,
	}

	hash := &HashJoinCostEstimator{}
	bnlj := &BlockNestedLoopJoinCostEstimator{}

	if !hash.CanApply(input) {
		t.Fatal("hash join should apply")
	}

	hashCost := hash.Estimate(input)
	bnljCost := bnlj.Estimate(input)

	if hashCost.Cost >= bnljCost.Cost {
		t.Fatalf("expected hash join cheaper, got hash=%v bnlj=%v", hashCost.Cost, bnljCost.Cost)
	}
}

// Join order tests (3-way)

// Plan comparison unit tests (2-way), does it choose the right plan (hash vs BNLJ vs IndexLJ)?

// End to end logical rule + CBO integration tests
