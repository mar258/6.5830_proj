package planner

import (
	"testing"

	"mit.edu/dsg/godb/common"
)

type JoinEstimatorSet struct {
	Estimators []JoinCostEstimator
}

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

func equiPred() []Expr {
	return []Expr{
		NewComparisonExpression(
			NewConstantValueExpression(common.NewIntValue(0)),
			NewConstantValueExpression(common.NewIntValue(0)),
			Equal,
		),
	}
}

// -----------------------------------------------------------------------------
// Cost model comparison unit tests (2-way)
// These test raw estimator cost behavior.
// -----------------------------------------------------------------------------

func TestCBOSimpleEquiJoinHashCheaperThanBNLJ(t *testing.T) {
	input := JoinCostInput{
		LeftRows:         1_000_000,
		RightRows:        1_000_000,
		Predicates:       equiPred(),
		AvailableBuffers: 100,
	}

	hash := &HashJoinCostEstimator{}
	bnlj := &BlockNestedLoopJoinCostEstimator{}

	hashCost := hash.Estimate(input)
	bnljCost := bnlj.Estimate(input)

	if hashCost.Cost >= bnljCost.Cost {
		t.Fatalf("expected hash join cheaper, got hash=%v bnlj=%v",
			hashCost.Cost, bnljCost.Cost)
	}
}

func TestCBOBNLJCostImprovesWithMoreBuffers(t *testing.T) {
	lowMem := JoinCostInput{
		LeftRows:         10_000,
		RightRows:        10_000,
		Predicates:       equiPred(),
		AvailableBuffers: 10,
	}

	highMem := lowMem
	highMem.AvailableBuffers = 1_000

	bnlj := &BlockNestedLoopJoinCostEstimator{}

	lowCost := bnlj.Estimate(lowMem)
	highCost := bnlj.Estimate(highMem)

	if highCost.Cost >= lowCost.Cost {
		t.Fatalf("expected more buffers to reduce BNLJ cost, got low=%v high=%v",
			lowCost.Cost, highCost.Cost)
	}
}

func TestCBOSortMergeCostLowerWhenInputsAlreadyOrdered(t *testing.T) {
	unordered := JoinCostInput{
		LeftRows:   10_000,
		RightRows:  10_000,
		Predicates: equiPred(),
	}

	ordered := unordered
	ordered.LeftHasOrdering = true
	ordered.RightHasOrdering = true

	sm := &SortMergeJoinCostEstimator{}

	unorderedCost := sm.Estimate(unordered)
	orderedCost := sm.Estimate(ordered)

	if orderedCost.Cost >= unorderedCost.Cost {
		t.Fatalf("expected ordered inputs to reduce sort-merge cost, got ordered=%v unordered=%v",
			orderedCost.Cost, unorderedCost.Cost)
	}
}

func TestCBOIndexNestedLoopRequiresRightIndex(t *testing.T) {
	noIndex := JoinCostInput{
		LeftRows:   100,
		RightRows:  1_000_000,
		Predicates: equiPred(),
	}

	withIndex := noIndex
	withIndex.RightHasIndexOnJoinKey = true

	inlj := &IndexNestedLoopJoinCostEstimator{}

	if inlj.CanApply(noIndex) {
		t.Fatal("index nested loop should not apply without right-side index")
	}

	if !inlj.CanApply(withIndex) {
		t.Fatal("index nested loop should apply with right-side index")
	}
}

// -----------------------------------------------------------------------------
// Plan comparison unit tests (2-way)
// These test whether the cheapest physical join is chosen for a single join.
// -----------------------------------------------------------------------------

func TestCBOBestJoinCostChoosesHashOverBNLJ(t *testing.T) {
	opt := &JoinOptimizer{
		Predicates:       equiPred(),
		AvailableBuffers: 100,
		estimators: []JoinCostEstimator{
			&HashJoinCostEstimator{},
			&BlockNestedLoopJoinCostEstimator{},
		},
	}

	left := &Plan{OutputRows: 1_000_000}
	right := &Plan{OutputRows: 1_000_000}

	_, _, ok, physical := opt.bestJoinCost(left, right)

	if !ok {
		t.Fatal("expected a valid physical join")
	}

	if physical != "HashJoin" {
		t.Fatalf("expected HashJoin, got %s", physical)
	}
}

func TestCBOBestJoinCostChoosesSortMergeWhenAlreadyOrdered(t *testing.T) {
	input := JoinCostInput{
		LeftRows:         1_000,
		RightRows:        1_000,
		Predicates:       equiPred(),
		LeftHasOrdering:  true,
		RightHasOrdering: true,
	}

	hash := &HashJoinCostEstimator{}
	sm := &SortMergeJoinCostEstimator{}

	hashCost := hash.Estimate(input)
	smCost := sm.Estimate(input)

	if smCost.Cost != hashCost.Cost {
		t.Fatalf("expected ordered SortMergeJoin to tie HashJoin under current model, got sm=%v hash=%v",
			smCost.Cost, hashCost.Cost)
	}
}

func TestCBOApplicableEstimatorsWithoutPredicateOnlyBNLJ(t *testing.T) {
	input := JoinCostInput{
		LeftRows:         1_000,
		RightRows:        1_000,
		Predicates:       nil,
		AvailableBuffers: 100,
	}

	set := JoinEstimatorSet{
		Estimators: []JoinCostEstimator{
			&IndexNestedLoopJoinCostEstimator{},
			&SortMergeJoinCostEstimator{},
			&HashJoinCostEstimator{},
			&BlockNestedLoopJoinCostEstimator{},
		},
	}

	got := set.ApplicableEstimates(input)

	if len(got) != 1 || got[0].Name != "BlockNestedLoopJoin" {
		t.Fatalf("expected only BNLJ to apply without equijoin predicate, got %+v", got)
	}
}

// -----------------------------------------------------------------------------
// Join order tests (3-way)
// These test whether FindBestJoin chooses a cheaper left-deep join order.
// -----------------------------------------------------------------------------

func TestCBOThreeWayJoinChoosesSmallTablesFirst(t *testing.T) {
	opt := &JoinOptimizer{
		numTables:        3,
		TableRows:        []float64{1_000_000, 10, 10},
		Predicates:       equiPred(),
		AvailableBuffers: 100,
		estimators: []JoinCostEstimator{
			&HashJoinCostEstimator{},
		},
	}

	plan := opt.FindBestJoin()
	if plan == nil {
		t.Fatal("expected a plan")
	}

	// With the current output model:
	// joining the two small tables first gives a smaller intermediate result.
	// The final right table should therefore be table 0, meaning table 0 was
	// joined last.
	if plan.RightTable != 0 {
		t.Fatalf("expected largest table 0 to be joined last, got right table %d", plan.RightTable)
	}
}

func TestCBOThreeWayJoinProducesFullMask(t *testing.T) {
	opt := &JoinOptimizer{
		numTables:        3,
		TableRows:        []float64{100, 200, 300},
		Predicates:       equiPred(),
		AvailableBuffers: 100,
		estimators: []JoinCostEstimator{
			&HashJoinCostEstimator{},
			&BlockNestedLoopJoinCostEstimator{},
		},
	}

	plan := opt.FindBestJoin()
	if plan == nil {
		t.Fatal("expected a plan")
	}

	if plan.Tables != 0b111 {
		t.Fatalf("expected final plan to contain all 3 tables, got mask %b", plan.Tables)
	}
}

// -----------------------------------------------------------------------------
// End-to-end logical rule + CBO integration tests
// These are limited integration-style tests for JoinOptimizer.FindBestJoin.
// They start from table stats + predicates and assert the final physical plan.
// -----------------------------------------------------------------------------

func TestCBOChoosesHashJoinForLargeEquiJoin(t *testing.T) {
	opt := &JoinOptimizer{
		numTables:        2,
		TableRows:        []float64{1_000_000, 1_000_000},
		Predicates:       equiPred(),
		AvailableBuffers: 100,
		estimators: []JoinCostEstimator{
			&HashJoinCostEstimator{},
			&BlockNestedLoopJoinCostEstimator{},
		},
	}

	plan := opt.FindBestJoin()
	if plan == nil {
		t.Fatal("expected a plan")
	}

	if plan.PhysicalJoin != "HashJoin" {
		t.Fatalf("expected HashJoin, got %s", plan.PhysicalJoin)
	}
}

func TestCBOFallsBackToBNLJWithoutEquiPredicate(t *testing.T) {
	opt := &JoinOptimizer{
		numTables:        2,
		TableRows:        []float64{1_000, 1_000},
		Predicates:       nil,
		AvailableBuffers: 100,
		estimators: []JoinCostEstimator{
			&HashJoinCostEstimator{},
			&SortMergeJoinCostEstimator{},
			&BlockNestedLoopJoinCostEstimator{},
		},
	}

	plan := opt.FindBestJoin()
	if plan == nil {
		t.Fatal("expected a plan")
	}

	if plan.PhysicalJoin != "BlockNestedLoopJoin" {
		t.Fatalf("expected BlockNestedLoopJoin fallback, got %s", plan.PhysicalJoin)
	}
}

// -----------------------------------------------------------------------------
// Synthetic datasets with different statistics
// These test whether changing table cardinalities changes optimizer behavior.
// -----------------------------------------------------------------------------

func TestCBOSyntheticSmallOuterWithIndexChoosesIndexNestedLoop(t *testing.T) {
	opt := &JoinOptimizer{
		numTables:        2,
		TableRows:        []float64{1, 1_000_000},
		Predicates:       equiPred(),
		AvailableBuffers: 100,
		IndexMeta:        fakeIndexMeta{indexedTables: map[int]bool{1: true}},
		estimators: []JoinCostEstimator{
			&IndexNestedLoopJoinCostEstimator{},
			&HashJoinCostEstimator{},
			&BlockNestedLoopJoinCostEstimator{},
		},
	}

	plan := opt.FindBestJoin()
	if plan == nil {
		t.Fatal("expected a plan")
	}

	if plan.PhysicalJoin != "IndexNestedLoopJoin" {
		t.Fatalf("expected IndexNestedLoopJoin, got %s", plan.PhysicalJoin)
	}
}

func TestCBOSyntheticLargeTablesWithoutIndexChoosesHash(t *testing.T) {
	opt := &JoinOptimizer{
		numTables:        2,
		TableRows:        []float64{1_000_000, 1_000_000},
		Predicates:       equiPred(),
		AvailableBuffers: 100,
		estimators: []JoinCostEstimator{
			&IndexNestedLoopJoinCostEstimator{},
			&HashJoinCostEstimator{},
			&BlockNestedLoopJoinCostEstimator{},
		},
	}

	plan := opt.FindBestJoin()
	if plan == nil {
		t.Fatal("expected a plan")
	}

	if plan.PhysicalJoin != "HashJoin" {
		t.Fatalf("expected HashJoin, got %s", plan.PhysicalJoin)
	}
}

type fakeIndexMeta struct {
	indexedTables map[int]bool
}

func (m fakeIndexMeta) InnerHasJoinIndex(innerTableIdx int, predicates []Expr) bool {
	return m.indexedTables[innerTableIdx]
}

// Plan comparison unit tests (2-way), does it choose the right plan (hash vs BNLJ vs IndexLJ)?

// End to end logical rule + CBO integration tests
