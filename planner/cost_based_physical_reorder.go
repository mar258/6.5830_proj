package planner

import (
	"fmt"

	"mit.edu/dsg/godb/catalog"
	"mit.edu/dsg/godb/common"
)

type cboCatalogIndexMeta struct {
	catalog *catalog.Catalog
	scans   []*LogicalScanNode
}

func (m cboCatalogIndexMeta) InnerHasJoinIndex(innerTableIdx int, predicates []Expr) bool {
	if innerTableIdx < 0 || innerTableIdx >= len(m.scans) {
		return false
	}
	scan := m.scans[innerTableIdx]
	table, err := m.catalog.GetTableByOid(scan.GetTableOid())
	if err != nil {
		return false
	}

	innerRefID := scan.TableRef.refID

	for _, pred := range predicates {
		cmp, ok := pred.(*ComparisonExpression)
		if !ok || cmp.compType != Equal {
			continue
		}

		for _, side := range []Expr{cmp.left, cmp.right} {
			col, ok := side.(*LogicalColumn)
			if !ok || col.origin == nil || col.origin.refID != innerRefID {
				continue
			}

			for _, idx := range table.Indexes {
				if len(idx.KeySchema) > 0 && idx.KeySchema[0] == col.cname {
					return true
				}
			}
		}
	}

	return false
}

func collectCBOJoinInputs(node LogicalPlanNode) ([]*LogicalScanNode, []Expr, bool) {
	switch n := node.(type) {
	case *LogicalScanNode:
		return []*LogicalScanNode{n}, nil, true

	case *LogicalJoinNode:
		if n.joinType != Inner {
			return nil, nil, false
		}

		leftScans, leftPreds, ok := collectCBOJoinInputs(n.Left)
		if !ok {
			return nil, nil, false
		}
		rightScans, rightPreds, ok := collectCBOJoinInputs(n.Right)
		if !ok {
			return nil, nil, false
		}

		scans := append(leftScans, rightScans...)
		preds := append(leftPreds, rightPreds...)
		preds = append(preds, n.joinOn...)
		return scans, preds, true

	default:
		return nil, nil, false
	}
}

func tableRefIDsFromScans(scans []*LogicalScanNode) []uint64 {
	ids := make([]uint64, len(scans))
	for i, scan := range scans {
		ids[i] = scan.TableRef.refID
	}
	return ids
}

func defaultTableRows(scans []*LogicalScanNode) []float64 {
	rows := make([]float64, len(scans))
	for i := range rows {
		rows[i] = 1
	}
	return rows
}

func (b *PhysicalPlanBuilder) estimateRowsForCBOScans(scans []*LogicalScanNode) ([]float64, error) {
	rows := make([]float64, len(scans))

	for i, scan := range scans {
		if b.cboRowEstimator == nil {
			rows[i] = 1
			continue
		}

		table, err := b.catalog.GetTableByOid(scan.GetTableOid())
		if err != nil {
			return nil, err
		}

		r, err := b.cboRowEstimator(table.Name)
		if err != nil {
			return nil, err
		}
		if r <= 0 {
			r = 1
		}
		rows[i] = r
	}

	return rows, nil
}

func (b *PhysicalPlanBuilder) TryBuildCBOReorderedJoin(logicalPlan LogicalPlanNode) (PlanNode, bool, error) {
	scans, predicates, ok := collectCBOJoinInputs(logicalPlan)
	if !ok || len(scans) < 2 {
		return nil, false, nil
	}

	physicalScans := make([]PlanNode, len(scans))
	logicalSchemas := make([]LogicalSchema, len(scans))

	for i, scan := range scans {
		phys, err := b.Build(scan)
		if err != nil {
			return nil, true, err
		}
		physicalScans[i] = phys
		logicalSchemas[i] = scan.OutputSchema()
	}

	tableRows, err := b.estimateRowsForCBOScans(scans)
	if err != nil {
		return nil, true, err
	}

	availableBuffers := b.cboAvailableBuffers
	if availableBuffers <= 0 {
		availableBuffers = 100
	}

	opt := &JoinOptimizer{
		numTables:        len(scans),
		TableRows:        tableRows,
		Predicates:       predicates,
		TableRefIDs:      tableRefIDsFromScans(scans),
		AvailableBuffers: availableBuffers,
		IndexMeta: cboCatalogIndexMeta{
			catalog: b.catalog,
			scans:   scans,
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
		return nil, false, nil
	}

	node, _, err := b.buildPlanNodeFromCBOPlan(best, opt, physicalScans, logicalSchemas)
	if err != nil {
		return nil, true, err
	}
	return node, true, nil
}

func (b *PhysicalPlanBuilder) buildPlanNodeFromCBOPlan(
	p *Plan,
	opt *JoinOptimizer,
	physicalScans []PlanNode,
	logicalSchemas []LogicalSchema,
) (PlanNode, LogicalSchema, error) {
	if p == nil {
		return nil, nil, fmt.Errorf("nil CBO plan")
	}

	if p.LeftChild == nil {
		if p.RightTable < 0 || p.RightTable >= len(physicalScans) {
			return nil, nil, fmt.Errorf("bad table index %d", p.RightTable)
		}
		return physicalScans[p.RightTable], logicalSchemas[p.RightTable], nil
	}

	leftNode, leftLogical, err := b.buildPlanNodeFromCBOPlan(p.LeftChild, opt, physicalScans, logicalSchemas)
	if err != nil {
		return nil, nil, err
	}

	rightIdx := p.RightTable
	rightNode := physicalScans[rightIdx]
	rightLogical := logicalSchemas[rightIdx]

	preds := opt.predicatesForJoin(p.LeftChild.Tables, rightIdx)
	if len(preds) == 0 {
		return nil, nil, fmt.Errorf("CBO chose join with no connecting predicate")
	}

	fullLogical := append(append(LogicalSchema{}, leftLogical...), rightLogical...)
	fullPhysical := append(append([]common.Type{}, leftNode.OutputSchema()...), rightNode.OutputSchema()...)

	switch p.PhysicalJoin {
	case "HashJoin":
		keys := equiKeysForLogicalSchemas(preds, leftLogical, rightLogical)
		if len(keys) == 0 {
			return nil, nil, fmt.Errorf("HashJoin chosen without equi keys")
		}

		leftKeys := make([]Expr, len(keys))
		rightKeys := make([]Expr, len(keys))
		for i, k := range keys {
			leftKeys[i] = b.exprBinder.BindExpr(k.Left, leftLogical, leftNode.OutputSchema())
			rightKeys[i] = b.exprBinder.BindExpr(k.Right, rightLogical, rightNode.OutputSchema())
		}

		node := NewHashJoinNode(leftNode, rightNode, leftKeys, rightKeys)
		return wrapResidualsIfNeeded(node, preds, keys, fullLogical, b.exprBinder), fullLogical, nil

	case "SortMergeJoin":
		keys := equiKeysForLogicalSchemas(preds, leftLogical, rightLogical)
		if len(keys) == 0 {
			return nil, nil, fmt.Errorf("SortMergeJoin chosen without equi keys")
		}

		leftKeys := make([]Expr, len(keys))
		rightKeys := make([]Expr, len(keys))
		for i, k := range keys {
			leftKeys[i] = b.exprBinder.BindExpr(k.Left, leftLogical, leftNode.OutputSchema())
			rightKeys[i] = b.exprBinder.BindExpr(k.Right, rightLogical, rightNode.OutputSchema())
		}

		node := NewSortMergeJoinNode(leftNode, rightNode, leftKeys, rightKeys)
		return wrapResidualsIfNeeded(node, preds, keys, fullLogical, b.exprBinder), fullLogical, nil

	case "BlockNestedLoopJoin":
		finalPred := MergePredicates(preds)
		physPred := b.exprBinder.BindExpr(finalPred, fullLogical, fullPhysical)

		switch rightNode.(type) {
		case *SeqScanNode, *IndexScanNode, *IndexLookupNode, *MaterializeNode:
		default:
			rightNode = NewMaterializeNode(rightNode)
		}

		node := NewBlockNestedLoopJoinNode(leftNode, rightNode, physPred)
		return node, fullLogical, nil

	case "IndexNestedLoopJoin":
		node, err := b.buildIndexNestedLoopFromCBO(
			leftNode,
			rightNode,
			leftLogical,
			rightLogical,
			preds,
		)
		if err != nil {
			return nil, nil, err
		}
		return node, fullLogical, nil

	default:
		return nil, nil, fmt.Errorf("unknown CBO physical join %q", p.PhysicalJoin)
	}
}

func equiKeysForLogicalSchemas(preds []Expr, left, right LogicalSchema) []JoinKeyPair {
	var out []JoinKeyPair
	for _, pred := range preds {
		if l, r, ok := isEquiJoin(pred, left, right); ok {
			for i := range l {
				out = append(out, JoinKeyPair{
					Left:         l[i],
					Right:        r[i],
					OriginalExpr: pred,
				})
			}
		}
	}
	return out
}

func wrapResidualsIfNeeded(
	node PlanNode,
	preds []Expr,
	keys []JoinKeyPair,
	fullLogical LogicalSchema,
	exprBinder *ExpressionBinder,
) PlanNode {
	used := make(map[Expr]bool)
	for _, k := range keys {
		used[k.OriginalExpr] = true
	}

	var residuals []Expr
	for _, pred := range preds {
		if !used[pred] {
			residuals = append(residuals, pred)
		}
	}

	if len(residuals) == 0 {
		return node
	}
	return wrapInFilter(node, residuals, fullLogical, exprBinder)
}

func (b *PhysicalPlanBuilder) buildIndexNestedLoopFromCBO(
	leftNode PlanNode,
	rightNode PlanNode,
	leftLogical LogicalSchema,
	rightLogical LogicalSchema,
	preds []Expr,
) (PlanNode, error) {
	rightTableOID, ok := physicalLeafTableOIDForCBO(rightNode)
	if !ok {
		return nil, fmt.Errorf("INLJ right child must be base table access")
	}

	table, err := b.catalog.GetTableByOid(rightTableOID)
	if err != nil {
		return nil, err
	}

	keys := equiKeysForLogicalSchemas(preds, leftLogical, rightLogical)
	if len(keys) == 0 {
		return nil, fmt.Errorf("INLJ chosen without equi keys")
	}

	var bestIdx *catalog.Index
	var bestMatch *JoinIndexMatch

	for _, idx := range table.Indexes {
		currentIdx := idx
		match := analyzeJoinIndex(&currentIdx, keys)
		if match.NumMatchedColumns == len(idx.KeySchema) {
			bestIdx = &currentIdx
			bestMatch = match
			break
		}
	}

	if bestIdx == nil || bestMatch == nil {
		return nil, fmt.Errorf("INLJ chosen but no usable index found")
	}

	physProbeKeys := make([]Expr, len(bestMatch.ProbeKeys))
	for i, pk := range bestMatch.ProbeKeys {
		physProbeKeys[i] = b.exprBinder.BindExpr(pk, leftLogical, leftNode.OutputSchema())
	}

	node := NewIndexNestedLoopJoinNode(
		leftNode,
		rightTableOID,
		bestIdx.Oid,
		physProbeKeys,
		tableSchemaToTypes(table),
		false,
	)

	used := make(map[Expr]bool)
	for _, e := range bestMatch.MatchedJoinOns {
		used[e] = true
	}

	var residuals []Expr
	for _, pred := range preds {
		if !used[pred] {
			residuals = append(residuals, pred)
		}
	}

	if len(residuals) > 0 {
		fullLogical := append(append(LogicalSchema{}, leftLogical...), rightLogical...)
		return wrapInFilter(node, residuals, fullLogical, b.exprBinder), nil
	}

	return node, nil
}

func physicalLeafTableOIDForCBO(node PlanNode) (common.ObjectID, bool) {
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
