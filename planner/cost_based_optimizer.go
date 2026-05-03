package planner

import (
	"fmt"
	"math"
	"math/bits"
	// "mit.edu/dsg/godb/common"
)

type Plan struct {
	Tables       uint32
	PhysicalJoin string   // physical op chosen at this node (estimator Name); empty for a leaf scan
	Cost         float64
	OutputRows   float64
	JoinCount	 int
	LeftChild    *Plan // nil only for a leaf
	RightTable   int   // base table index: sole table if leaf, otherwise table joined on the right

	Candidates []JoinCandidate // candidate physical joins considered at this step
}

type JoinIndexMetadata interface {
	InnerHasJoinIndex(innerTableIdx int, predicates []Expr) bool
}

type JoinOptimizer struct {
	memo       map[uint32]*Plan
	numTables  int
	estimators []JoinCostEstimator
	// TableRows[i] is the estimated row count for base table i; if shorter than numTables, missing entries default to 1.
	TableRows []float64
	Predicates []Expr
	// AvailableBuffers is passed through to estimators (e.g. BNLJ).
	AvailableBuffers int
	// IndexMeta wires catalog/index info into join costing; optional.
	IndexMeta JoinIndexMetadata
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

// bestJoinCost picks the cheapest physical join
func (opt *JoinOptimizer) bestJoinCost(leftPlan, rightPlan *Plan) (joinCost float64, outputRows float64, ok bool, physicalJoin string) {
	joinCost = math.Inf(1)
	outputRows = math.Inf(1)

	for _, c := range opt.joinCandidates(leftPlan, rightPlan) {
		if !c.Applicable || math.IsInf(c.Cost, 1) {
			continue
		}
		if c.Cost < joinCost {
			joinCost = c.Cost
			outputRows = c.OutputRows
			ok = true
			physicalJoin = c.PhysicalJoin
		}
	}

	return joinCost, outputRows, ok, physicalJoin
}

func (opt *JoinOptimizer) joinCandidates(leftPlan, rightPlan *Plan) []JoinCandidate {
	buffers := opt.AvailableBuffers
	if buffers <= 0 {
		buffers = 1
	}

	input := JoinCostInput{
		LeftRows:               leftPlan.OutputRows,
		RightRows:              rightPlan.OutputRows,
		LeftRowWidth:           1,
		RightRowWidth:          1,
		Predicates:             opt.Predicates,
		AvailableBuffers:       buffers,
		RightHasIndexOnJoinKey: opt.rightLeafHasJoinIndex(rightPlan),
	}

	var candidates []JoinCandidate
	for _, est := range opt.estimatorsForSearch() {
		if !est.CanApply(input) {
			candidates = append(candidates, JoinCandidate{
				PhysicalJoin: est.Name(),
				Applicable:   false,
			})
			continue
		}

		e := est.Estimate(input)
		candidates = append(candidates, JoinCandidate{
			PhysicalJoin: est.Name(),
			Cost:         e.Cost,
			OutputRows:   e.OutputRows,
			Applicable:   true,
		})
	}

	return candidates
}

// rightLeafHasJoinIndex reports whether the single-table child of this join step has a
// join index on the inner side
func (opt *JoinOptimizer) rightLeafHasJoinIndex(rightPlan *Plan) bool {
	if opt.IndexMeta == nil || rightPlan == nil || rightPlan.LeftChild != nil {
		return false
	}
	return opt.IndexMeta.InnerHasJoinIndex(rightPlan.RightTable, opt.Predicates)
}

func (opt *JoinOptimizer) FindBestJoin() *Plan {
	opt.memo = make(map[uint32]*Plan)

	// Base case: 0 cost for one table
	for i := 0; i < opt.numTables; i++ {
		opt.memo[1<<i] = &Plan{
			Tables:       uint32(1 << i),
			PhysicalJoin: "",
			Cost:         0,
			OutputRows:   opt.tableRowCount(i),
			JoinCount:    0,
			LeftChild:    nil,
			RightTable:   i,
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
				candidates := opt.joinCandidates(leftPlan, rightPlan)
				jc, outRows, ok, physicalJoin := bestCandidate(candidates)
				if !ok {
					continue
				}
				total := leftPlan.Cost + rightPlan.Cost + jc
				if total < bestCost {
					bestCost = total
					best = &Plan{
						Tables:       mask,
						PhysicalJoin: physicalJoin,
						Cost:         total,
						OutputRows:   outRows,
						JoinCount:    leftPlan.JoinCount + 1,
						LeftChild:    leftPlan,
						RightTable:   i,
						Candidates: candidates,
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

func bestCandidate(candidates []JoinCandidate) (joinCost float64, outputRows float64, ok bool, physicalJoin string) {
	joinCost = math.Inf(1)
	outputRows = math.Inf(1)

	for _, c := range candidates {
		if !c.Applicable || math.IsInf(c.Cost, 1) {
			continue
		}
		if c.Cost < joinCost {
			joinCost = c.Cost
			outputRows = c.OutputRows
			ok = true
			physicalJoin = c.PhysicalJoin
		}
	}

	return joinCost, outputRows, ok, physicalJoin
}

func countSetBits(mask uint32) int {
	return bits.OnesCount32(mask)
}

type JoinCandidate struct {
	PhysicalJoin string
	Cost         float64
	OutputRows   float64
	Applicable   bool
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
	return true
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
	// Prototype assumption:
	// The optimizer is currently passed only join predicates, and synthetic
	// test inputs use equality predicates. A full integration should inspect
	// Expr structure and verify that the predicate is column = column.
	return len(preds) > 0
}

func estimateJoinOutputRows(input JoinCostInput) float64 {
	if hasEquiJoinPredicate(input.Predicates) {
		// Simple equijoin model: assume output is roughly bounded by
		// the smaller input relation.
		return math.Max(1, math.Min(input.LeftRows, input.RightRows))
	}

	// Non-equi/cross-style fallback.
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
