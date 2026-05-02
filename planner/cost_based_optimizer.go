package planner

import (
	"fmt"
	"math"
	"math/bits"
	// "mit.edu/dsg/godb/common"
)

type Plan struct {
	Tables      uint32
	Join        JoinType
	Cost        float64
	OutputRows  float64
	LeftChild   *Plan
	RightChild  *Plan
}

type JoinOptimizer struct {
	memo       map[uint32]*Plan
	numTables  int
	estimators []JoinCostEstimator
	// TableRows[i] is the estimated row count for base table i; if shorter than numTables, missing entries default to 1.
	TableRows []float64
	JoinType  JoinType
	Predicates []Expr
	// AvailableBuffers is passed through to estimators (e.g. BNLJ).
	AvailableBuffers int
}

func (opt *JoinOptimizer) tableRowCount(tableIdx int) float64 {
	if tableIdx >= 0 && tableIdx < len(opt.TableRows) && opt.TableRows[tableIdx] > 0 {
		return opt.TableRows[tableIdx]
	}
	return 1
}

func (opt *JoinOptimizer) estimatorsForSearch() []JoinCostEstimator {
	if len(opt.estimators) > 0 {
		return opt.estimators
	}
	return []JoinCostEstimator{
		&IndexNestedLoopJoinCostEstimator{},
		&SortMergeJoinCostEstimator{},
		&HashJoinCostEstimator{},
		&BlockNestedLoopJoinCostEstimator{},
	}
}

// bestJoinCost finds the cheapest applicable physical join between two subtrees (tries both outer/inner orderings).
func (opt *JoinOptimizer) bestJoinCost(leftPlan, rightPlan *Plan) (joinCost float64, outputRows float64, ok bool) {
	joinCost = math.Inf(1)
	outputRows = math.Inf(1)
	buffers := opt.AvailableBuffers
	if buffers <= 0 {
		buffers = 1
	}
	try := func(outer, inner *Plan) {
		in := JoinCostInput{
			LeftRows:         outer.OutputRows,
			RightRows:        inner.OutputRows,
			LeftRowWidth:     1,
			RightRowWidth:    1,
			JoinType:         opt.JoinType,
			Predicates:       opt.Predicates,
			AvailableBuffers: buffers,
		}
		for _, est := range opt.estimatorsForSearch() {
			if !est.CanApply(in) {
				continue
			}
			e := est.Estimate(in)
			if !math.IsInf(e.Cost, 1) && e.Cost < joinCost {
				joinCost = e.Cost
				outputRows = e.OutputRows
				ok = true
			}
		}
	}
	try(leftPlan, rightPlan)
	try(rightPlan, leftPlan)
	return joinCost, outputRows, ok
}

func (opt *JoinOptimizer) FindBestJoin() *Plan {
	opt.memo = make(map[uint32]*Plan)

	// Base case: 0 cost for one table
	for i := 0; i < opt.numTables; i++ {
		opt.memo[1<<i] = &Plan{
			Tables:     uint32(1 << i),
			Join:       opt.JoinType,
			Cost:       0,
			OutputRows: opt.tableRowCount(i),
			LeftChild:  nil,
			RightChild: nil,
		}
	}

	fullMask := uint32((1 << opt.numTables) - 1)
	for nbits := 2; nbits <= opt.numTables; nbits++ {
		for mask := uint32(3); mask <= fullMask; mask++ {
			if countSetBits(mask) != nbits {
				continue
			}
			var best *Plan
			bestCost := math.Inf(1)

			// Left-deep only: each join adds one base table on the right; left is the
			// optimal plan for the remaining tables.
			for i := 0; i < opt.numTables; i++ {
				tableBit := uint32(1 << i)
				if mask&tableBit == 0 {
					continue
				}
				leftMask := mask ^ tableBit
				leftPlan := opt.memo[leftMask]
				rightPlan := opt.memo[tableBit]
				if leftPlan == nil || rightPlan == nil {
					continue
				}
				jc, outRows, ok := opt.bestJoinCost(leftPlan, rightPlan)
				if !ok {
					continue
				}
				total := leftPlan.Cost + rightPlan.Cost + jc
				if total < bestCost {
					bestCost = total
					best = &Plan{
						Tables:     mask,
						Join:       opt.JoinType,
						Cost:       total,
						OutputRows: outRows,
						LeftChild:  leftPlan,
						RightChild: rightPlan,
					}
				}
			}

			if best != nil {
				opt.memo[mask] = best
			}
		}
	}

	return opt.memo[fullMask]
}

func countSetBits(mask uint32) int {
	return bits.OnesCount32(mask)
}

type JoinCostEstimate struct {
	Cost       float64 // current cost
	OutputRows float64 // future cost
}

type JoinCostInput struct {
	LeftRows      float64
	RightRows     float64
	LeftRowWidth  float64
	RightRowWidth float64

	JoinType   JoinType
	Predicates []Expr

	LeftHasOrdering  bool
	RightHasOrdering bool

	LeftHasIndexOnJoinKey  bool
	RightHasIndexOnJoinKey bool

	AvailableBuffers int
}

// Each algorithm implements this to estimate costs.
type JoinCostEstimator interface {
	Name() string
	CanApply(input JoinCostInput) bool             // whether this join algorithm is valid for this join
	Estimate(input JoinCostInput) JoinCostEstimate // returns estimated cost/cardinality
}

type IndexNestedLoopJoinCostEstimator struct{}
type SortMergeJoinCostEstimator struct{}
type HashJoinCostEstimator struct{}
type BlockNestedLoopJoinCostEstimator struct{}

// Start IndexNestedLoopJoinCostEstimator
func (ce *IndexNestedLoopJoinCostEstimator) Name() string {
	return "IndexNestedLoopJoin"
}

func (ce *IndexNestedLoopJoinCostEstimator) CanApply(input JoinCostInput) bool {
	// Usually only valid when:
	// 1. this is an INNER join (for a first version),
	// 2. the predicate is an equijoin,
	// 3. the inner side has a usable index.
	//
	// Adjust this depending on whether your optimizer can choose which side is inner.
	if input.JoinType != Inner {
		return false
	}
	if !hasEquiJoinPredicate(input.Predicates) {
		return false
	}
	if !input.RightHasIndexOnJoinKey {
		return false
	}
	return true
}

func (ce *IndexNestedLoopJoinCostEstimator) Estimate(input JoinCostInput) JoinCostEstimate {
	if !ce.CanApply(input) {
		return impossibleCost()
	}

	// Template:
	// Cost ~= scan/build outer + one index probe per outer tuple.
	//
	// Replace these constants with something better later.
	indexProbeCost := 3.0
	perMatchFetchCost := 1.0

	outputRows := estimateJoinOutputRows(input)

	cost := input.LeftRows + (input.LeftRows * indexProbeCost) + (outputRows * perMatchFetchCost)

	return JoinCostEstimate{
		Cost:       cost,
		OutputRows: outputRows,
	}
}

// Start SortMergeJoinCostEstimator
func (ce *SortMergeJoinCostEstimator) Name() string {
	return "SortMergeJoin"
}

func (ce *SortMergeJoinCostEstimator) CanApply(input JoinCostInput) bool {
	// Usually requires a mergeable/sortable join predicate.
	// For a first version, assume only equijoins are supported.
	if input.JoinType != Inner {
		return false
	}
	if !hasEquiJoinPredicate(input.Predicates) {
		return false
	}
	return true
}

func (ce *SortMergeJoinCostEstimator) Estimate(input JoinCostInput) JoinCostEstimate {
	if !ce.CanApply(input) {
		return impossibleCost()
	}

	outputRows := estimateJoinOutputRows(input)

	leftSortCost := 0.0
	if !input.LeftHasOrdering {
		leftSortCost = estimateSortCost(input.LeftRows)
	}

	rightSortCost := 0.0
	if !input.RightHasOrdering {
		rightSortCost = estimateSortCost(input.RightRows)
	}

	mergeCost := input.LeftRows + input.RightRows

	return JoinCostEstimate{
		Cost:       leftSortCost + rightSortCost + mergeCost,
		OutputRows: outputRows,
	}
}

// Start HashJoinCostEstimator
func (ce *HashJoinCostEstimator) Name() string {
	return "HashJoin"
}

func (ce *HashJoinCostEstimator) CanApply(input JoinCostInput) bool {
	// Usually only valid for equijoins.
	if input.JoinType != Inner {
		return false
	}
	if !hasEquiJoinPredicate(input.Predicates) {
		return false
	}
	return true
}

func (ce *HashJoinCostEstimator) Estimate(input JoinCostInput) JoinCostEstimate {
	if !ce.CanApply(input) {
		return impossibleCost()
	}

	outputRows := estimateJoinOutputRows(input)

	// Template:
	// Cost ~= build + probe
	// You can later refine this with memory spill penalties, row width, etc.
	buildCost := input.RightRows
	probeCost := input.LeftRows

	return JoinCostEstimate{
		Cost:       buildCost + probeCost,
		OutputRows: outputRows,
	}
}

// Start BlockNestedLoopJoinCostEstimator
func (ce *BlockNestedLoopJoinCostEstimator) Name() string {
	return "BlockNestedLoopJoin"
}

func (ce *BlockNestedLoopJoinCostEstimator) CanApply(input JoinCostInput) bool {
	// BNLJ is the usual fallback and supports most join predicates.
	switch input.JoinType {
	case Inner:
		return true
	default:
		// Expand later if you support outer joins in physical planning.
		return false
	}
}

func (ce *BlockNestedLoopJoinCostEstimator) Estimate(input JoinCostInput) JoinCostEstimate {
	if !ce.CanApply(input) {
		return impossibleCost()
	}

	outputRows := estimateJoinOutputRows(input)

	// Template:
	// Cost depends on how many blocks/chunks of the outer relation
	// we can keep in memory at once.
	//
	// Very rough first pass:
	buffers := math.Max(1, float64(input.AvailableBuffers))
	outerBlocks := math.Ceil(input.LeftRows / buffers)

	cost := input.LeftRows + (outerBlocks * input.RightRows)

	return JoinCostEstimate{
		Cost:       cost,
		OutputRows: outputRows,
	}
}

// Start helper functions
func impossibleCost() JoinCostEstimate {
	return JoinCostEstimate{
		Cost:       math.Inf(1),
		OutputRows: math.Inf(1),
	}
}

func hasEquiJoinPredicate(preds []Expr) bool {
	// TODO:
	// Replace this stub with real predicate inspection.
	//
	// For example, you may want to detect a ComparisonExpression
	// with equality operator between columns from different sides.
	return len(preds) > 0
}

func estimateJoinOutputRows(input JoinCostInput) float64 {
	// TODO:
	// Replace with a better selectivity/cardinality model.
	//
	// For now:
	// - equijoin gets a modest selectivity assumption
	// - otherwise fall back to a more pessimistic estimate
	if hasEquiJoinPredicate(input.Predicates) {
		return math.Max(1, 0.1*input.LeftRows*input.RightRows)
	}
	return math.Max(1, 0.3*input.LeftRows*input.RightRows)
}

func estimateSortCost(rows float64) float64 {
	// Very rough n log n template.
	if rows <= 1 {
		return rows
	}
	return rows * math.Log2(rows)
}

func debugJoinCost(name string, input JoinCostInput, est JoinCostEstimate) string {
	return fmt.Sprintf(
		"%s: leftRows=%.2f rightRows=%.2f cost=%.2f outRows=%.2f",
		name,
		input.LeftRows,
		input.RightRows,
		est.Cost,
		est.OutputRows,
	)
}
