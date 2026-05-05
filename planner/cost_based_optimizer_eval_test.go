package planner

import (
	"fmt"
	"math"
	"sort"
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
	switch x := n.(type) {
	case *ProjectionNode:
		return unwrapPhysicalJoinKind(x.Child)
	case *FilterNode:
		return unwrapPhysicalJoinKind(x.Child)
	case *MaterializeNode:
		return unwrapPhysicalJoinKind(x.Child)
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

func evalEquiPredicates() []Expr {
	return []Expr{
		NewComparisonExpression(
			NewConstantValueExpression(common.NewIntValue(0)),
			NewConstantValueExpression(common.NewIntValue(0)),
			Equal,
		),
	}
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

var evalIOCostSink float64

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

func estimateRuleBasedIOCost(node PlanNode, rowByOID map[common.ObjectID]float64, buffers int) (cost float64, outputRows float64) {
	if buffers <= 0 {
		buffers = 1
	}
	switch n := node.(type) {
	case *ProjectionNode:
		return estimateRuleBasedIOCost(n.Child, rowByOID, buffers)
	case *FilterNode:
		return estimateRuleBasedIOCost(n.Child, rowByOID, buffers)
	case *MaterializeNode:
		return estimateRuleBasedIOCost(n.Child, rowByOID, buffers)
	case *SeqScanNode:
		rows := rowByOID[n.TableOid]
		if rows <= 0 {
			rows = 1
		}
		return rows, rows
	case *IndexScanNode:
		rows := rowByOID[n.TableOid]
		if rows <= 0 {
			rows = 1
		}
		// Roughly cheaper than full scan when using index access.
		return rows * 0.25, rows
	case *IndexLookupNode:
		rows := rowByOID[n.TableOid]
		if rows <= 0 {
			rows = 1
		}
		// Point lookup modeled as near-constant probes.
		return 3.0, math.Max(1.0, rows*0.001)
	case *HashJoinNode:
		lc, lr := estimateRuleBasedIOCost(n.Left, rowByOID, buffers)
		rc, rr := estimateRuleBasedIOCost(n.Right, rowByOID, buffers)
		out := math.Max(lr, rr) * 0.8
		return lc + rc + lr + rr, math.Max(1.0, out)
	case *SortMergeJoinNode:
		lc, lr := estimateRuleBasedIOCost(n.Left, rowByOID, buffers)
		rc, rr := estimateRuleBasedIOCost(n.Right, rowByOID, buffers)
		out := math.Max(lr, rr) * 0.8
		sortCost := estimateSortCost(lr) + estimateSortCost(rr)
		return lc + rc + sortCost + lr + rr, math.Max(1.0, out)
	case *NestedLoopJoinNode:
		lc, lr := estimateRuleBasedIOCost(n.Left, rowByOID, buffers)
		rc, rr := estimateRuleBasedIOCost(n.Right, rowByOID, buffers)
		out := math.Max(lr, rr) * 0.8
		outerBlocks := math.Ceil(lr / float64(buffers))
		return lc + rc + lr + (outerBlocks * rr), math.Max(1.0, out)
	case *IndexNestedLoopJoinNode:
		lc, lr := estimateRuleBasedIOCost(n.Left, rowByOID, buffers)
		rr := rowByOID[n.RightTableOid]
		if rr <= 0 {
			rr = 1
		}
		out := math.Max(lr, rr) * 0.8
		// Same constants as IndexNestedLoopJoinCostEstimator.
		return lc + lr + (lr * 3.0) + out, math.Max(1.0, out)
	default:
		children := n.Children()
		if len(children) == 0 {
			return 1, 1
		}
		total := 0.0
		rows := 0.0
		for _, ch := range children {
			c, r := estimateRuleBasedIOCost(ch, rowByOID, buffers)
			total += c
			rows = math.Max(rows, r)
		}
		return total, math.Max(1.0, rows)
	}
}

// benchRows150k is the estimated row count per base table for join-chain eval benchmarks.
const benchRows150k = 150_000


// BenchmarkEvalJoinChain150kTablesIOCost reports estimated I/O cost/op for the same
// 2..7-table chain setups using a rough plan-cost model (rule-based) vs CBO cost.
func BenchmarkEvalJoinChain150kTablesIOCost(b *testing.B) {
	for _, n := range []int{2, 3, 4, 5, 6, 7} {
		n := n
		b.Run(fmt.Sprintf("tables_%02d", n), func(b *testing.B) {
			cat := newEvalNTableChainCatalogTB(b, n)
			logicalRoot := newEvalNTableChainLogicalJoinTB(b, cat, n)
			builder := NewPhysicalPlanBuilder(cat, physicalRulesJoinEval())

			tableRows := make([]float64, n)
			for i := range tableRows {
				tableRows[i] = benchRows150k
			}
			rowByOID := evalRowLookupForChainTB(b, cat, tableRows)
			cbo := &JoinOptimizer{
				numTables:        n,
				TableRows:        tableRows,
				Predicates:       evalEquiPredicates(),
				AvailableBuffers: 100,
				estimators: []JoinCostEstimator{
					&HashJoinCostEstimator{},
					&BlockNestedLoopJoinCostEstimator{},
				},
			}

			b.Run("RuleBasedEstimatedIOCost", func(b *testing.B) {
				b.ReportAllocs()
				var total float64
				for b.Loop() {
					p, err := builder.Build(logicalRoot)
					if err != nil {
						b.Fatal(err)
					}
					cost, _ := estimateRuleBasedIOCost(p, rowByOID, 100)
					total += cost
				}
				evalIOCostSink = total
				b.ReportMetric(total/float64(b.N), "io_cost/op")
			})
			b.Run("CBOEstimatedIOCost", func(b *testing.B) {
				b.ReportAllocs()
				var total float64
				for b.Loop() {
					p := cbo.FindBestJoin()
					if p == nil {
						b.Fatal("nil CBO plan")
					}
					total += p.Cost
				}
				evalIOCostSink = total
				b.ReportMetric(total/float64(b.N), "io_cost/op")
			})
		})
	}
}

// evalJoinSkewedSizes is one row-count estimate per base table (eval_t0 … eval_t4) for a 5-way chain.
var evalJoinSkewedSizes = []float64{6_000_000, 1_500_000, 150_000, 100_000, 25}

// BenchmarkEvalJoinChainSkewedSizesIOCost reports estimated I/O cost/op for the same
// skewed 5-table setup using rough rule-based plan costing vs CBO cost.
func BenchmarkEvalJoinChainSkewedSizesIOCost(b *testing.B) {
	const n = 5
	if len(evalJoinSkewedSizes) != n {
		b.Fatalf("evalJoinSkewedSizes must have length %d", n)
	}

	cat := newEvalNTableChainCatalogTB(b, n)
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
	logicalRootAscendingToDescending := newEvalOrderedChainLogicalJoinTB(b, cat, ascendingOrder)
	logicalRootDescendingToAscending := newEvalOrderedChainLogicalJoinTB(b, cat, descendingOrder)
	builder := NewPhysicalPlanBuilder(cat, physicalRulesJoinEval())
	rowByOID := evalRowLookupForChainTB(b, cat, evalJoinSkewedSizes)

	tableRows := append([]float64(nil), evalJoinSkewedSizes...)
	cbo := &JoinOptimizer{
		numTables:        n,
		TableRows:        tableRows,
		Predicates:       evalEquiPredicates(),
		AvailableBuffers: 100,
		estimators: []JoinCostEstimator{
			&HashJoinCostEstimator{},
			&BlockNestedLoopJoinCostEstimator{},
		},
	}

	b.Run("RuleBasedEstimatedIOCostSmallToLarge", func(b *testing.B) {
		b.ReportAllocs()
		var total float64
		for b.Loop() {
			p, err := builder.Build(logicalRootAscendingToDescending)
			if err != nil {
				b.Fatal(err)
			}
			cost, _ := estimateRuleBasedIOCost(p, rowByOID, 100)
			total += cost
		}
		evalIOCostSink = total
		b.ReportMetric(total/float64(b.N), "io_cost/op")
	})
	b.Run("RuleBasedEstimatedIOCostLargeToSmall", func(b *testing.B) {
		b.ReportAllocs()
		var total float64
		for b.Loop() {
			p, err := builder.Build(logicalRootDescendingToAscending)
			if err != nil {
				b.Fatal(err)
			}
			cost, _ := estimateRuleBasedIOCost(p, rowByOID, 100)
			total += cost
		}
		evalIOCostSink = total
		b.ReportMetric(total/float64(b.N), "io_cost/op")
	})
	b.Run("CBOEstimatedIOCost", func(b *testing.B) {
		b.ReportAllocs()
		var total float64
		for b.Loop() {
			p := cbo.FindBestJoin()
			if p == nil {
				b.Fatal("nil CBO plan")
			}
			total += p.Cost
		}
		evalIOCostSink = total
		b.ReportMetric(total/float64(b.N), "io_cost/op")
	})
}

