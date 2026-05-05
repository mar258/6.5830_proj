package planner

import (
	"fmt"
	"math"
	"sort"
	"strings"
	"testing"

	"mit.edu/dsg/godb/catalog"
	"mit.edu/dsg/godb/common"
)

// physicalRulesJoinEval is the same join policy as join_rules.go: try rules in
// priority order (see PhysicalPlanBuilder) — INLJ, SMJ, Hash, BNLJ (plus scan rules).
func physicalRulesJoinEval() []PhysicalConversionRule {
	return []PhysicalConversionRule{
		&SeqScanRule{},
		&IndexScanRule{},
		&IndexLookupRule{},
		&IndexNestedLoopJoinRule{},
		&SortMergeJoinRule{},
		&HashJoinRule{},
		&BlockNestedLoopJoinRule{},
	}
}

func unwrapPhysicalJoinKind(n PlanNode) string {
	n = unwrapPhysicalPlanDecorators(n)
	switch n.(type) {
	case *IndexNestedLoopJoinNode:
		return "IndexNestedLoopJoin"
	case *SortMergeJoinNode:
		return "SortMergeJoin"
	case *HashJoinNode:
		return "HashJoin"
	case *NestedLoopJoinNode:
		return "BlockNestedLoopJoin"
	default:
		return fmt.Sprintf("%T", n)
	}
}

// collectPhysicalJoinOperators lists physical join operators in evaluation order (inner joins first).
func collectPhysicalJoinOperators(n PlanNode) []string {
	n = unwrapPhysicalPlanDecorators(n)
	switch j := n.(type) {
	case *HashJoinNode:
		return appendJoinPhysicalOps(j.Left, j.Right, "HashJoin")
	case *SortMergeJoinNode:
		return appendJoinPhysicalOps(j.Left, j.Right, "SortMergeJoin")
	case *NestedLoopJoinNode:
		return appendJoinPhysicalOps(j.Left, j.Right, "BlockNestedLoopJoin")
	case *IndexNestedLoopJoinNode:
		return append(collectPhysicalJoinOperators(j.Left), "IndexNestedLoopJoin")
	default:
		return nil
	}
}

func appendJoinPhysicalOps(left, right PlanNode, op string) []string {
	a := collectPhysicalJoinOperators(left)
	b := collectPhysicalJoinOperators(right)
	return append(append(a, b...), op)
}

// collectCBOJoinOperators lists PhysicalJoin choices along the best left-deep CBO plan (inner to outer).
func collectCBOJoinOperators(p *Plan) []string {
	if p == nil || p.LeftChild == nil {
		return nil
	}
	inner := collectCBOJoinOperators(p.LeftChild)
	return append(inner, p.PhysicalJoin)
}

func formatJoinSequence(ops []string) string {
	if len(ops) == 0 {
		return "(none)"
	}
	return strings.Join(ops, " → ")
}

// evalLogJoinCompare prints rule-built physical costing vs CBO in a fixed layout (use with go test -v).
func evalLogJoinCompare(t *testing.T, title string, physicalCost float64, physicalOps []string, cboCost float64, cboOps []string, cboPlan *Plan) {
	t.Helper()
	t.Log(title)
	t.Log("  Physical plan (rule-built tree, join-estimator cost)")
	t.Logf("    cost:  %.6g", physicalCost)
	t.Logf("    joins: %s", formatJoinSequence(physicalOps))
	t.Log("  CBO (FindBestJoin)")
	t.Logf("    cost:  %.6g", cboCost)
	t.Logf("    joins: %s", formatJoinSequence(cboOps))
	if cboPlan != nil {
		t.Logf("    tree:  %s", cboPlan.String())
	}
}

func evalLogPhysicalJoinPlan(t *testing.T, title string, cost float64, ops []string) {
	t.Helper()
	t.Log(title)
	t.Logf("  cost:  %.6g", cost)
	t.Logf("  joins: %s", formatJoinSequence(ops))
}

func evalLogCBOPlan(t *testing.T, title string, best *Plan) {
	t.Helper()
	t.Log(title)
	t.Logf("  cost:  %.6g", best.Cost)
	t.Logf("  joins: %s", formatJoinSequence(collectCBOJoinOperators(best)))
	t.Logf("  tree:  %s", best.String())
}

func newEvalNTableChainCatalogTB(tb testing.TB, n int) *catalog.Catalog {
	c, err := catalog.NewCatalog(catalog.NullPersistenceProvider{})
	if err != nil {
		tb.Fatalf("catalog: %v", err)
	}
	for i := 0; i < n; i++ {
		name := fmt.Sprintf("eval_t%d", i)
		if _, err := c.AddTable(name, []catalog.Column{
			{Name: "id", Type: common.IntType},
			{Name: "k", Type: common.IntType},
		}); err != nil {
			tb.Fatalf("AddTable %s: %v", name, err)
		}
		if _, err := c.AddIndex(fmt.Sprintf("%s_id_idx", name), name, "btree", []string{"id"}); err != nil {
			tb.Fatalf("AddIndex: %v", err)
		}
	}
	return c
}

func newEvalNTableChainLogicalJoinTB(tb testing.TB, cat *catalog.Catalog, n int) LogicalPlanNode {
	if n < 2 {
		tb.Fatal("n must be >= 2")
	}
	refs := make([]*TableRef, n)
	scans := make([]*LogicalScanNode, n)
	for i := 0; i < n; i++ {
		tmeta, err := cat.GetTableMetadata(fmt.Sprintf("eval_t%d", i))
		if err != nil {
			tb.Fatalf("GetTableMetadata eval_t%d: %v", i, err)
		}
		refs[i] = makeTableRef(tmeta, fmt.Sprintf("t%d", i), uint64(400+i))
		scans[i] = NewLogicalScanNode(refs[i], false)
	}
	var plan LogicalPlanNode = scans[0]
	for i := 1; i < n; i++ {
		cond := NewComparisonExpression(
			findCol(refs[i-1], "k"),
			findCol(refs[i], "id"),
			Equal,
		)
		plan = NewLogicalJoinNode(plan, scans[i], []Expr{cond}, Inner)
	}
	return plan
}

func newEvalOrderedChainLogicalJoinTB(tb testing.TB, cat *catalog.Catalog, tableOrder []int) LogicalPlanNode {
	if len(tableOrder) < 2 {
		tb.Fatal("tableOrder must have length >= 2")
	}

	refs := make([]*TableRef, len(tableOrder))
	scans := make([]*LogicalScanNode, len(tableOrder))
	for i, tableIdx := range tableOrder {
		tmeta, err := cat.GetTableMetadata(fmt.Sprintf("eval_t%d", tableIdx))
		if err != nil {
			tb.Fatalf("GetTableMetadata eval_t%d: %v", tableIdx, err)
		}
		refs[i] = makeTableRef(tmeta, fmt.Sprintf("t%d", tableIdx), uint64(800+i))
		scans[i] = NewLogicalScanNode(refs[i], false)
	}

	var plan LogicalPlanNode = scans[0]
	for i := 1; i < len(scans); i++ {
		cond := NewComparisonExpression(
			findCol(refs[i-1], "k"),
			findCol(refs[i], "id"),
			Equal,
		)
		plan = NewLogicalJoinNode(plan, scans[i], []Expr{cond}, Inner)
	}

	return plan
}

func evalRowLookupForChainTB(tb testing.TB, cat *catalog.Catalog, tableRows []float64) map[common.ObjectID]float64 {
	out := make(map[common.ObjectID]float64, len(tableRows))
	for i := range tableRows {
		tmeta, err := cat.GetTableMetadata(fmt.Sprintf("eval_t%d", i))
		if err != nil {
			tb.Fatalf("GetTableMetadata eval_t%d: %v", i, err)
		}
		out[tmeta.Oid] = tableRows[i]
	}
	return out
}

// evalOidToScanOrdinal maps table OID -> scan position i in collectJoinOptimizerInputs order.
// catalogJoinIndexMeta.tableByIdx[i] and InnerHasJoinIndex(i, ...) use this ordering. Using raw
// eval_t0..eval_{n-1} indices breaks when the logical join chain permutes tables (see skewed tests).
func evalOidToScanOrdinal(scans []*LogicalScanNode) map[common.ObjectID]int {
	out := make(map[common.ObjectID]int, len(scans))
	for i, s := range scans {
		if s == nil {
			continue
		}
		out[s.GetTableOid()] = i
	}
	return out
}

// evalOidToEvalTableIndex maps catalog table OID -> eval_t{k} index k (stats slice position).
func evalOidToEvalTableIndex(tb testing.TB, cat *catalog.Catalog, n int) map[common.ObjectID]int {
	out := make(map[common.ObjectID]int)
	for k := 0; k < n; k++ {
		tmeta, err := cat.GetTableMetadata(fmt.Sprintf("eval_t%d", k))
		if err != nil {
			tb.Fatalf("GetTableMetadata eval_t%d: %v", k, err)
		}
		out[tmeta.Oid] = k
	}
	return out
}

// evalStatsPermutedForScans lays out cardinality estimates in JoinOptimizer scan order (matches TableRows[i]).
func evalStatsPermutedForScans(scans []*LogicalScanNode, statsByEvalIdx []float64, oidToEvalIdx map[common.ObjectID]int) []float64 {
	out := make([]float64, len(scans))
	for i, s := range scans {
		k := oidToEvalIdx[s.GetTableOid()]
		out[i] = statsByEvalIdx[k]
	}
	return out
}

// benchRows150k is the estimated row count per base table for join-chain eval benchmarks.
const benchRows150k = 150_000

func TestEvalJoinTwoTableOuter100Inner10k(t *testing.T) {
	const n = 2
	const outerRows = 100
	const innerRows = 10_000

	cat := newEvalNTableChainCatalogTB(t, n)
	logicalRoot := newEvalNTableChainLogicalJoinTB(t, cat, n)
	builder := NewPhysicalPlanBuilder(cat, physicalRulesJoinEval())

	tableRows := []float64{outerRows, innerRows}
	rowByOID := evalRowLookupForChainTB(t, cat, tableRows)

	scans, joinPredicates := collectJoinOptimizerInputs(logicalRoot)
	if len(joinPredicates) == 0 {
		t.Fatal("expected join predicates from logical plan")
	}

	p, err := builder.Build(logicalRoot)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	indexMeta := newCatalogJoinIndexMeta(scans)
	oidToScan := evalOidToScanOrdinal(scans)
	planCost, _ := EstimatePhysicalPlanJoinOptimizerCost(p, rowByOID, joinPredicates, 100, indexMeta, oidToScan)
	if planCost <= 0 || math.IsNaN(planCost) || math.IsInf(planCost, 0) {
		t.Fatalf("unexpected physical join-estimator cost: %v", planCost)
	}

	cbo := &JoinOptimizer{
		numTables:        n,
		TableRows:        tableRows,
		Predicates:       joinPredicates,
		AvailableBuffers: 100,
		IndexMeta:        indexMeta,
		estimators: []JoinCostEstimator{
			&IndexNestedLoopJoinCostEstimator{},
			&SortMergeJoinCostEstimator{},
			&HashJoinCostEstimator{},
			&BlockNestedLoopJoinCostEstimator{},
		},
	}
	best := cbo.FindBestJoin()
	if best == nil {
		t.Fatal("nil CBO plan")
	}
	if best.Cost <= 0 || math.IsNaN(best.Cost) || math.IsInf(best.Cost, 0) {
		t.Fatalf("unexpected CBO cost: %v", best.Cost)
	}

	// Same estimator formulas for both paths. Totals match only when the rule-built plan picks the
	// same physical join as the CBO (here HashJoin is cheaper than IndexNestedLoopJoin, but rule
	// order tries INLJ first when applicable, so costs can differ).
	rboJoin := unwrapPhysicalJoinKind(p)
	if rboJoin == best.PhysicalJoin && planCost != best.Cost {
		t.Fatalf("same operator %s but cost mismatch: physical=%g cbo=%g", rboJoin, planCost, best.Cost)
	}

	evalLogJoinCompare(t, "── 2-table join (100 × 10k rows) ──",
		planCost, collectPhysicalJoinOperators(p), best.Cost, collectCBOJoinOperators(best), best)
}

// TestEvalJoinTwoTableOuter1kInner10k exercises the eval harness on a single equi-join with
// asymmetric cardinalities: outer (left / eval_t0) 1k rows, inner (right / eval_t1) 10k rows.
func TestEvalJoinTwoTableOuter1kInner10k(t *testing.T) {
	const n = 2
	const outerRows = 1000
	const innerRows = 10_000

	cat := newEvalNTableChainCatalogTB(t, n)
	logicalRoot := newEvalNTableChainLogicalJoinTB(t, cat, n)
	builder := NewPhysicalPlanBuilder(cat, physicalRulesJoinEval())

	tableRows := []float64{outerRows, innerRows}
	rowByOID := evalRowLookupForChainTB(t, cat, tableRows)

	scans, joinPredicates := collectJoinOptimizerInputs(logicalRoot)
	if len(joinPredicates) == 0 {
		t.Fatal("expected join predicates from logical plan")
	}

	p, err := builder.Build(logicalRoot)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	indexMeta := newCatalogJoinIndexMeta(scans)
	oidToScan := evalOidToScanOrdinal(scans)
	planCost, _ := EstimatePhysicalPlanJoinOptimizerCost(p, rowByOID, joinPredicates, 100, indexMeta, oidToScan)
	if planCost <= 0 || math.IsNaN(planCost) || math.IsInf(planCost, 0) {
		t.Fatalf("unexpected physical join-estimator cost: %v", planCost)
	}

	cbo := &JoinOptimizer{
		numTables:        n,
		TableRows:        tableRows,
		Predicates:       joinPredicates,
		AvailableBuffers: 100,
		IndexMeta:        indexMeta,
		estimators: []JoinCostEstimator{
			&IndexNestedLoopJoinCostEstimator{},
			&SortMergeJoinCostEstimator{},
			&HashJoinCostEstimator{},
			&BlockNestedLoopJoinCostEstimator{},
		},
	}
	best := cbo.FindBestJoin()
	if best == nil {
		t.Fatal("nil CBO plan")
	}
	if best.Cost <= 0 || math.IsNaN(best.Cost) || math.IsInf(best.Cost, 0) {
		t.Fatalf("unexpected CBO cost: %v", best.Cost)
	}

	// Same estimator formulas for both paths. Totals match only when the rule-built plan picks the
	// same physical join as the CBO (here HashJoin is cheaper than IndexNestedLoopJoin, but rule
	// order tries INLJ first when applicable, so costs can differ).
	rboJoin := unwrapPhysicalJoinKind(p)
	if rboJoin == best.PhysicalJoin && planCost != best.Cost {
		t.Fatalf("same operator %s but cost mismatch: physical=%g cbo=%g", rboJoin, planCost, best.Cost)
	}

	evalLogJoinCompare(t, "── 2-table join (1k × 10k rows) ──",
		planCost, collectPhysicalJoinOperators(p), best.Cost, collectCBOJoinOperators(best), best)
}

// TestEvalJoinChain150kTablesIOCost logs join-estimator costs and join operators for the rule-built
// plan vs CBO for each chain length (same estimator definitions as FindBestJoin).
func TestEvalJoinChain150kTablesIOCost(t *testing.T) {
	for _, n := range []int{2, 3, 4, 5, 6, 7} {
		n := n
		t.Run(fmt.Sprintf("tables_%02d", n), func(t *testing.T) {
			cat := newEvalNTableChainCatalogTB(t, n)
			logicalRoot := newEvalNTableChainLogicalJoinTB(t, cat, n)
			builder := NewPhysicalPlanBuilder(cat, physicalRulesJoinEval())

			tableRows := make([]float64, n)
			for i := range tableRows {
				tableRows[i] = benchRows150k
			}
			rowByOID := evalRowLookupForChainTB(t, cat, tableRows)
			scans, joinPredicates := collectJoinOptimizerInputs(logicalRoot)
			indexMeta := newCatalogJoinIndexMeta(scans)
			oidToScan := evalOidToScanOrdinal(scans)
			cbo := &JoinOptimizer{
				numTables:        n,
				TableRows:        tableRows,
				Predicates:       joinPredicates,
				AvailableBuffers: 100,
				IndexMeta:        indexMeta,
				estimators: []JoinCostEstimator{
					&IndexNestedLoopJoinCostEstimator{},
					&SortMergeJoinCostEstimator{},
					&HashJoinCostEstimator{},
					&BlockNestedLoopJoinCostEstimator{},
				},
			}

			p, err := builder.Build(logicalRoot)
			if err != nil {
				t.Fatalf("Build: %v", err)
			}
			planCost, _ := EstimatePhysicalPlanJoinOptimizerCost(p, rowByOID, joinPredicates, 100, indexMeta, oidToScan)
			best := cbo.FindBestJoin()
			if best == nil {
				t.Fatal("nil CBO plan")
			}
			evalLogJoinCompare(t, fmt.Sprintf("── %d-table chain (≈150k rows/table) ──", n),
				planCost, collectPhysicalJoinOperators(p), best.Cost, collectCBOJoinOperators(best), best)
		})
	}
}

// evalJoinSkewedSizes is one row-count estimate per base table (eval_t0 … eval_t4) for a 5-way chain.
var evalJoinSkewedSizes = []float64{6_000_000, 1_500_000, 150_000, 100_000, 25}

// TestEvalJoinChainSkewedSizesIOCost logs costs and join operators for skewed 5-table chains.
func TestEvalJoinChainSkewedSizesIOCost(t *testing.T) {
	const n = 5
	if len(evalJoinSkewedSizes) != n {
		t.Fatalf("evalJoinSkewedSizes must have length %d", n)
	}

	cat := newEvalNTableChainCatalogTB(t, n)
	ascendingOrder := make([]int, n)
	for i := range ascendingOrder {
		ascendingOrder[i] = i
	}
	sort.Slice(ascendingOrder, func(i, j int) bool {
		return evalJoinSkewedSizes[ascendingOrder[i]] < evalJoinSkewedSizes[ascendingOrder[j]]
	})
	descendingOrder := make([]int, n)
	for i := range descendingOrder {
		descendingOrder[i] = ascendingOrder[n-1-i]
	}
	logicalRootAscendingToDescending := newEvalOrderedChainLogicalJoinTB(t, cat, ascendingOrder)
	logicalRootDescendingToAscending := newEvalOrderedChainLogicalJoinTB(t, cat, descendingOrder)
	builder := NewPhysicalPlanBuilder(cat, physicalRulesJoinEval())
	rowByOID := evalRowLookupForChainTB(t, cat, evalJoinSkewedSizes)

	scansAsc, joinPredicatesAsc := collectJoinOptimizerInputs(logicalRootAscendingToDescending)
	indexMetaAsc := newCatalogJoinIndexMeta(scansAsc)
	scansDesc, joinPredicatesDesc := collectJoinOptimizerInputs(logicalRootDescendingToAscending)
	indexMetaDesc := newCatalogJoinIndexMeta(scansDesc)

	oidToEval := evalOidToEvalTableIndex(t, cat, n)
	tableRowsCBO := evalStatsPermutedForScans(scansAsc, evalJoinSkewedSizes, oidToEval)
	oidAsc := evalOidToScanOrdinal(scansAsc)
	oidDesc := evalOidToScanOrdinal(scansDesc)

	cbo := &JoinOptimizer{
		numTables:        n,
		TableRows:        tableRowsCBO,
		Predicates:       joinPredicatesAsc,
		AvailableBuffers: 100,
		IndexMeta:        indexMetaAsc,
		estimators: []JoinCostEstimator{
			&IndexNestedLoopJoinCostEstimator{},
			&SortMergeJoinCostEstimator{},
			&HashJoinCostEstimator{},
			&BlockNestedLoopJoinCostEstimator{},
		},
	}

	t.Run("physical_small_to_large", func(t *testing.T) {
		p, err := builder.Build(logicalRootAscendingToDescending)
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		cost, _ := EstimatePhysicalPlanJoinOptimizerCost(p, rowByOID, joinPredicatesAsc, 100, indexMetaAsc, oidAsc)
		evalLogPhysicalJoinPlan(t, "── Physical plan (table order: ascending size →) ──", cost, collectPhysicalJoinOperators(p))
	})
	t.Run("physical_large_to_small", func(t *testing.T) {
		p, err := builder.Build(logicalRootDescendingToAscending)
		if err != nil {
			t.Fatalf("Build: %v", err)
		}
		cost, _ := EstimatePhysicalPlanJoinOptimizerCost(p, rowByOID, joinPredicatesDesc, 100, indexMetaDesc, oidDesc)
		evalLogPhysicalJoinPlan(t, "── Physical plan (table order: descending size →) ──", cost, collectPhysicalJoinOperators(p))
	})
	t.Run("cbo", func(t *testing.T) {
		best := cbo.FindBestJoin()
		if best == nil {
			t.Fatal("nil CBO plan")
		}
		evalLogCBOPlan(t, "── CBO (skewed stats, scan order = ascending size) ──", best)
	})
}

// TestEvalJoinCBOMixesJoinAlgorithms builds a 4-way chain where cardinality estimates and partial
// join-index availability force the CBO to pick different physical joins at different steps.
//
// AssumeSortedJoinInputs sets ordering flags so SortMergeJoinCostEstimator omits sort phases (inputs
// assumed ordered on join keys). Sort-merge then ties HashJoin at cost L+R; SortMergeJoin is listed
// before HashJoin among estimators, so the CBO chooses SortMergeJoin when IndexNestedLoopJoin does not
// apply (here: inner eval_t2 has join index disabled via fake IndexMeta).
//
// It logs an RBO comparison: PhysicalPlanBuilder + catalog-based costing vs this CBO setup.
func TestEvalJoinCBOMixesJoinAlgorithms(t *testing.T) {
	const n = 4
	// Tiny × huge encourages IndexNestedLoopJoin when the inner has an index; fakeIndexMeta disables
	// join index on eval_t2 so INLJ cannot apply when t2 is inner — with AssumeSortedJoinInputs,
	// SortMergeJoin wins over HashJoin on the tie.
	statsByEvalIdx := []float64{
		50,          // eval_t0
		12_000_000,  // eval_t1
		12_000_000,  // eval_t2 — large; index gate controlled below
		400_000,     // eval_t3
	}

	cat := newEvalNTableChainCatalogTB(t, n)
	logicalRoot := newEvalNTableChainLogicalJoinTB(t, cat, n)
	scans, preds := collectJoinOptimizerInputs(logicalRoot)
	if len(preds) == 0 {
		t.Fatal("expected join predicates")
	}

	rowByOID := evalRowLookupForChainTB(t, cat, statsByEvalIdx)
	oidToEval := evalOidToEvalTableIndex(t, cat, n)
	tableRows := evalStatsPermutedForScans(scans, statsByEvalIdx, oidToEval)

	// RBO: fixed rule order (physicalRulesJoinEval); cost the built plan with catalog index metadata
	// so InnerHasJoinIndex matches real btree indexes on id.
	builder := NewPhysicalPlanBuilder(cat, physicalRulesJoinEval())
	pRBO, err := builder.Build(logicalRoot)
	if err != nil {
		t.Fatalf("RBO Build: %v", err)
	}
	indexMetaCatalog := newCatalogJoinIndexMeta(scans)
	oidToScan := evalOidToScanOrdinal(scans)
	rboCost, _ := EstimatePhysicalPlanJoinOptimizerCost(pRBO, rowByOID, preds, 100, indexMetaCatalog, oidToScan)
	rboJoins := collectPhysicalJoinOperators(pRBO)

	opt := &JoinOptimizer{
		numTables:              n,
		TableRows:              tableRows,
		Predicates:             preds,
		AvailableBuffers:       100,
		AssumeSortedJoinInputs: true,
		IndexMeta: fakeIndexMeta{
			indexedTables: map[int]bool{
				0: true,
				1: true,
				2: false, // no join-index hint for table 2 → INLJ inapplicable when t2 is inner leaf
				3: true,
			},
		},
		estimators: []JoinCostEstimator{
			&IndexNestedLoopJoinCostEstimator{},
			&SortMergeJoinCostEstimator{},
			&HashJoinCostEstimator{},
			&BlockNestedLoopJoinCostEstimator{},
		},
	}

	best := opt.FindBestJoin()
	if best == nil {
		t.Fatal("nil CBO plan")
	}

	ops := collectCBOJoinOperators(best)
	distinct := distinctStrings(ops)
	hasINLJ, hasSMJ := false, false
	for _, d := range distinct {
		switch d {
		case "IndexNestedLoopJoin":
			hasINLJ = true
		case "SortMergeJoin":
			hasSMJ = true
		}
	}
	if !hasINLJ || !hasSMJ {
		t.Fatalf("expected both IndexNestedLoopJoin and SortMergeJoin in plan, distinct=%v ops=%v plan=%s",
			distinct, ops, best.String())
	}

	t.Log("── 4-table chain: rule-built vs CBO ──")
	t.Log("  Physical plan (catalog indexes; rule-built operators)")
	t.Logf("    cost:  %.6g", rboCost)
	t.Logf("    joins: %s", formatJoinSequence(rboJoins))
	t.Log("  CBO (AssumeSortedJoinInputs + fake IndexMeta on eval_t2)")
	t.Logf("    cost:  %.6g", best.Cost)
	t.Logf("    joins: %s", formatJoinSequence(ops))
	t.Logf("    tree:  %s", best.String())
	t.Logf("    distinct operators: %s (%d)", strings.Join(distinct, ", "), len(distinct))
}

func distinctStrings(in []string) []string {
	seen := make(map[string]struct{}, len(in))
	out := make([]string, 0, len(in))
	for _, s := range in {
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		out = append(out, s)
	}
	return out
}

