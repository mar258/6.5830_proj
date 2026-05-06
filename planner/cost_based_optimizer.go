package planner

import (
	"fmt"
	"math"
	"math/bits"

	"mit.edu/dsg/godb/common"
)

type Plan struct {
	Tables       uint32
	PhysicalJoin string // physical op chosen at this node (estimator Name); empty for a leaf scan
	Cost         float64
	OutputRows   float64
	JoinCount    int
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
	TableRows   []float64
	Predicates  []Expr
	TableRefIDs []uint64 // TableRefIDs[i] is the LogicalScan/TableRef refID for base table i.
	// AvailableBuffers is passed through to estimators (e.g. BNLJ).
	AvailableBuffers int
	// IndexMeta wires catalog/index info into join costing; optional.
	IndexMeta              JoinIndexMetadata
	AssumeSortedJoinInputs bool
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
func (opt *JoinOptimizer) bestJoinCost(leftPlan, rightPlan *Plan, predicates []Expr) (joinCost float64, outputRows float64, ok bool, physicalJoin string) {
	joinCost = math.Inf(1)
	outputRows = math.Inf(1)

	for _, c := range opt.joinCandidates(leftPlan, rightPlan, predicates) {
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

func (opt *JoinOptimizer) joinCandidates(leftPlan, rightPlan *Plan, predicates []Expr) []JoinCandidate {
	buffers := opt.AvailableBuffers
	if buffers <= 0 {
		buffers = 1
	}

	input := JoinCostInput{
		LeftRows:               leftPlan.OutputRows,
		RightRows:              rightPlan.OutputRows,
		LeftRowWidth:           1,
		RightRowWidth:          1,
		Predicates:             predicates,
		AvailableBuffers:       buffers,
		RightHasIndexOnJoinKey: opt.rightLeafHasJoinIndex(rightPlan, predicates),
		LeftHasOrdering:        opt.AssumeSortedJoinInputs,
		RightHasOrdering:       opt.AssumeSortedJoinInputs,
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
func (opt *JoinOptimizer) rightLeafHasJoinIndex(rightPlan *Plan, predicates []Expr) bool {
	if opt.IndexMeta == nil || rightPlan == nil || rightPlan.LeftChild != nil {
		return false
	}
	return opt.IndexMeta.InnerHasJoinIndex(rightPlan.RightTable, predicates)
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
				joinPredicates := opt.predicatesForJoin(leftMask, i)

				candidates := opt.joinCandidates(leftPlan, rightPlan, joinPredicates)
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
						Candidates:   candidates,
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
func (opt *JoinOptimizer) predicatesForJoin(leftMask uint32, rightTable int) []Expr {
	// Legacy fallback for synthetic tests that construct JoinOptimizer directly
	// without table ref IDs.
	if len(opt.TableRefIDs) < opt.numTables {
		return opt.Predicates
	}

	var result []Expr
	for _, pred := range opt.Predicates {
		if opt.predicateConnectsLeftToRight(pred, leftMask, rightTable) {
			result = append(result, pred)
		}
	}
	return result
}

func (opt *JoinOptimizer) predicateConnectsLeftToRight(pred Expr, leftMask uint32, rightTable int) bool {
	refs := pred.GetReferencedColumns()

	mentionsRight := false
	mentionsLeft := false

	for _, col := range refs {
		if col == nil || col.origin == nil {
			continue
		}

		tableIdx := opt.tableIndexForRefID(col.origin.refID)
		if tableIdx < 0 {
			continue
		}

		if tableIdx == rightTable {
			mentionsRight = true
		}

		if leftMask&(uint32(1)<<tableIdx) != 0 {
			mentionsLeft = true
		}
	}

	return mentionsRight && mentionsLeft
}

func (opt *JoinOptimizer) tableIndexForRefID(refID uint64) int {
	for i, id := range opt.TableRefIDs {
		if id == refID {
			return i
		}
	}
	return -1
}

func impossibleCost() JoinCostEstimate {
	return JoinCostEstimate{
		Cost:       math.Inf(1),
		OutputRows: math.Inf(1),
	}
}

// hasEquiJoinPredicate reports whether the join condition contains at least one
// equality that can drive hash / sort-merge / index nested-loop style equijoins:
//   - column = column with different table origins (including self-join aliases), or
//   - legacy synthetic tests that use constant = constant.
//
// It returns false for empty predicates, pure range joins (<, >), column op constant
// filters, same-table column = column (single-table predicate), and unsupported shapes.
func hasEquiJoinPredicate(preds []Expr) bool {
	if len(preds) == 0 {
		return false
	}
	for _, p := range preds {
		if exprHasEquiJoinPredicate(p) {
			return true
		}
	}
	return false
}

func exprHasEquiJoinPredicate(e Expr) bool {
	switch x := e.(type) {
	case *ComparisonExpression:
		return isJoinEqualityComparison(x)
	case *BinaryLogicExpression:
		switch x.logicType {
		case And, Or:
			return exprHasEquiJoinPredicate(x.left) || exprHasEquiJoinPredicate(x.right)
		}
	case *NegationExpression:
		return false
	}
	return false
}

func isJoinEqualityComparison(cmp *ComparisonExpression) bool {
	if cmp.compType != Equal {
		return false
	}
	lc, lok := cmp.left.(*LogicalColumn)
	rc, rok := cmp.right.(*LogicalColumn)
	if lok && rok {
		if lc.origin == nil || rc.origin == nil {
			return false
		}
		if lc.origin.Equals(rc.origin) {
			return false
		}
		return true
	}
	_, lcok := cmp.left.(*ConstantValueExpr)
	_, rcok := cmp.right.(*ConstantValueExpr)
	return lcok && rcok
}

func estimateJoinOutputRows(input JoinCostInput) float64 {
	// A standard naive heuristic: assume an inner join output size
	// is roughly bound by the larger of the two relations (e.g., an FK -> PK join)
	// but scale down slightly to account for some rows not matching.
	baseEstimate := math.Max(input.LeftRows, input.RightRows) * 0.8

	return math.Max(1.0, baseEstimate)
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

// USED FOR CALCULATING COST OF RBO in cost_based_optimizer_eval
// unwrapPhysicalPlanDecorators strips planner decoration nodes so costing sees scans and joins.
func unwrapPhysicalPlanDecorators(node PlanNode) PlanNode {
	for {
		switch n := node.(type) {
		case *ProjectionNode:
			node = n.Child
		case *FilterNode:
			node = n.Child
		case *MaterializeNode:
			node = n.Child
		default:
			return node
		}
	}
}

func physicalLeafTableOid(node PlanNode) (common.ObjectID, bool) {
	switch n := unwrapPhysicalPlanDecorators(node).(type) {
	case *SeqScanNode:
		return n.TableOid, true
	case *IndexScanNode:
		return n.TableOid, true
	case *IndexLookupNode:
		return n.TableOid, true
	default:
		return 0, false
	}
}

func physicalRightLeafHasJoinIndex(right PlanNode, predicates []Expr, indexMeta JoinIndexMetadata, oidToBaseTableIdx map[common.ObjectID]int) bool {
	if indexMeta == nil || oidToBaseTableIdx == nil {
		return false
	}
	oid, ok := physicalLeafTableOid(right)
	if !ok {
		return false
	}
	ti, ok := oidToBaseTableIdx[oid]
	if !ok {
		return false
	}
	return indexMeta.InnerHasJoinIndex(ti, predicates)
}

func joinCostInputPhysical(leftRows, rightRows float64, predicates []Expr, availableBuffers int, indexMeta JoinIndexMetadata, oidToBaseTableIdx map[common.ObjectID]int, right PlanNode, inljInnerOid common.ObjectID, inljInnerKnown bool) JoinCostInput {
	input := JoinCostInput{
		LeftRows:         leftRows,
		RightRows:        rightRows,
		LeftRowWidth:     1,
		RightRowWidth:    1,
		Predicates:       predicates,
		AvailableBuffers: availableBuffers,
	}
	if inljInnerKnown {
		if ti, ok := oidToBaseTableIdx[inljInnerOid]; ok && indexMeta != nil {
			input.RightHasIndexOnJoinKey = indexMeta.InnerHasJoinIndex(ti, predicates)
		}
	} else {
		input.RightHasIndexOnJoinKey = physicalRightLeafHasJoinIndex(right, predicates, indexMeta, oidToBaseTableIdx)
	}
	return input
}

// EstimatePhysicalPlanJoinOptimizerCost sums join costs the same way as JoinOptimizer.FindBestJoin:
// base-table leaves contribute cost 0 with cardinality from rowCountsByOID; each physical join node
// adds the matching JoinCostEstimator cost (hash / sort-merge / block NL / index NL).
//
// predicates and indexMeta should match the JoinOptimizer inputs used for comparison.
//
// oidToBaseTableIdx maps table OID -> scan ordinal i (same order as collectJoinOptimizerInputs /
// newCatalogJoinIndexMeta). It must not use raw eval_t0..eval_{n-1} positions when the logical plan
// permutes tables — InnerHasJoinIndex(i) indexes catalogJoinIndexMeta.tableByIdx[i] by scan order.
func EstimatePhysicalPlanJoinOptimizerCost(root PlanNode, rowCountsByOID map[common.ObjectID]float64, predicates []Expr, availableBuffers int, indexMeta JoinIndexMetadata, oidToBaseTableIdx map[common.ObjectID]int) (cost float64, outputRows float64) {
	buffers := availableBuffers
	if buffers <= 0 {
		buffers = 1
	}
	return estimatePhysicalPlanJoinOptimizerCost(root, rowCountsByOID, predicates, buffers, indexMeta, oidToBaseTableIdx)
}

func estimatePhysicalPlanJoinOptimizerCost(node PlanNode, rowCountsByOID map[common.ObjectID]float64, predicates []Expr, availableBuffers int, indexMeta JoinIndexMetadata, oidToBaseTableIdx map[common.ObjectID]int) (cost float64, outputRows float64) {
	node = unwrapPhysicalPlanDecorators(node)

	switch n := node.(type) {
	case *SeqScanNode:
		r := tableRowsFromJoinStats(rowCountsByOID, n.TableOid)
		return 0, r
	case *IndexScanNode:
		r := tableRowsFromJoinStats(rowCountsByOID, n.TableOid)
		return 0, r
	case *IndexLookupNode:
		r := tableRowsFromJoinStats(rowCountsByOID, n.TableOid)
		return 0, r
	case *HashJoinNode:
		lc, lr := estimatePhysicalPlanJoinOptimizerCost(n.Left, rowCountsByOID, predicates, availableBuffers, indexMeta, oidToBaseTableIdx)
		rc, rr := estimatePhysicalPlanJoinOptimizerCost(n.Right, rowCountsByOID, predicates, availableBuffers, indexMeta, oidToBaseTableIdx)
		in := joinCostInputPhysical(lr, rr, predicates, availableBuffers, indexMeta, oidToBaseTableIdx, n.Right, 0, false)
		est := (&HashJoinCostEstimator{}).Estimate(in)
		return lc + rc + est.Cost, est.OutputRows
	case *SortMergeJoinNode:
		lc, lr := estimatePhysicalPlanJoinOptimizerCost(n.Left, rowCountsByOID, predicates, availableBuffers, indexMeta, oidToBaseTableIdx)
		rc, rr := estimatePhysicalPlanJoinOptimizerCost(n.Right, rowCountsByOID, predicates, availableBuffers, indexMeta, oidToBaseTableIdx)
		in := joinCostInputPhysical(lr, rr, predicates, availableBuffers, indexMeta, oidToBaseTableIdx, n.Right, 0, false)
		est := (&SortMergeJoinCostEstimator{}).Estimate(in)
		return lc + rc + est.Cost, est.OutputRows
	case *NestedLoopJoinNode:
		lc, lr := estimatePhysicalPlanJoinOptimizerCost(n.Left, rowCountsByOID, predicates, availableBuffers, indexMeta, oidToBaseTableIdx)
		rc, rr := estimatePhysicalPlanJoinOptimizerCost(n.Right, rowCountsByOID, predicates, availableBuffers, indexMeta, oidToBaseTableIdx)
		in := joinCostInputPhysical(lr, rr, predicates, availableBuffers, indexMeta, oidToBaseTableIdx, n.Right, 0, false)
		est := (&BlockNestedLoopJoinCostEstimator{}).Estimate(in)
		return lc + rc + est.Cost, est.OutputRows
	case *IndexNestedLoopJoinNode:
		lc, lr := estimatePhysicalPlanJoinOptimizerCost(n.Left, rowCountsByOID, predicates, availableBuffers, indexMeta, oidToBaseTableIdx)
		rr := tableRowsFromJoinStats(rowCountsByOID, n.RightTableOid)
		in := joinCostInputPhysical(lr, rr, predicates, availableBuffers, indexMeta, oidToBaseTableIdx, nil, n.RightTableOid, true)
		est := (&IndexNestedLoopJoinCostEstimator{}).Estimate(in)
		return lc + est.Cost, est.OutputRows
	default:
		children := node.Children()
		if len(children) == 0 {
			return 0, 1
		}
		var total float64
		var maxRows float64
		for _, ch := range children {
			c, r := estimatePhysicalPlanJoinOptimizerCost(ch, rowCountsByOID, predicates, availableBuffers, indexMeta, oidToBaseTableIdx)
			total += c
			if r > maxRows {
				maxRows = r
			}
		}
		return total, math.Max(1.0, maxRows)
	}
}

func tableRowsFromJoinStats(rowCountsByOID map[common.ObjectID]float64, oid common.ObjectID) float64 {
	r := rowCountsByOID[oid]
	if r <= 0 {
		return 1
	}
	return r
}
