package planner

import (
	"fmt"

	"github.com/xwb1989/sqlparser"
	"mit.edu/dsg/godb/catalog"
	"mit.edu/dsg/godb/common"
)

// ExplainJoinOptimizer builds JoinOptimizer input from a SQL query and returns
// a human-readable explanation of the best left-deep join plan.
func (p *SQLPlanner) ExplainJoinOptimizer(sql string, availableBuffers int, rowEstimator func(tableName string) (float64, error)) (string, error) {
	opt, ok, err := p.buildJoinOptimizer(sql, availableBuffers, rowEstimator)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("query does not contain at least 2 base tables")
	}

	return ExplainBestJoin(opt), nil
}

// EstimateExecutedPhysicalPlanJoinCost estimates the join cost of the physical
// plan that was actually produced by the rule-based physical planner.
func (p *SQLPlanner) EstimateExecutedPhysicalPlanJoinCost(
	sql string,
	physicalPlan PlanNode,
	availableBuffers int,
	rowEstimator func(tableName string) (float64, error),
) (float64, bool, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return 0, false, fmt.Errorf("parse error: %w", err)
	}

	lb := NewLogicalPlanBuilder(p.catalog)
	logicalPlan, err := lb.Plan(stmt)
	if err != nil {
		return 0, false, fmt.Errorf("logical planning error: %w", err)
	}

	scans, predicates := collectJoinOptimizerInputs(logicalPlan)
	if len(scans) < 2 {
		return 0, false, nil
	}

	rowCountsByOID := make(map[common.ObjectID]float64)
	oidToBaseTableIdx := make(map[common.ObjectID]int)

	for i, scan := range scans {
		if scan == nil || scan.TableRef == nil || scan.TableRef.table == nil {
			continue
		}

		oid := scan.GetTableOid()
		oidToBaseTableIdx[oid] = i

		rows := 1.0
		if rowEstimator != nil {
			estimatedRows, err := rowEstimator(scan.TableRef.table.Name)
			if err != nil {
				return 0, true, fmt.Errorf("row estimation failed for table '%s': %w", scan.TableRef.table.Name, err)
			}
			if estimatedRows > 0 {
				rows = estimatedRows
			}
		}
		rowCountsByOID[oid] = rows
	}

	cost, _ := EstimatePhysicalPlanJoinOptimizerCost(
		physicalPlan,
		rowCountsByOID,
		predicates,
		availableBuffers,
		newCatalogJoinIndexMeta(scans),
		oidToBaseTableIdx,
	)

	return cost, true, nil
}

// EstimateJoinOptimizerCost returns the estimated CBO join cost for SQL queries
// that contain joins. The boolean is false when there is no join to cost.
func (p *SQLPlanner) EstimateJoinOptimizerCost(sql string, availableBuffers int, rowEstimator func(tableName string) (float64, error)) (float64, bool, error) {
	opt, ok, err := p.buildJoinOptimizer(sql, availableBuffers, rowEstimator)
	if err != nil || !ok {
		return 0, ok, err
	}

	best := opt.FindBestJoin()
	if best == nil {
		return 0, true, fmt.Errorf("no valid join plan found")
	}
	return best.Cost, true, nil
}

func (p *SQLPlanner) buildJoinOptimizer(sql string, availableBuffers int, rowEstimator func(tableName string) (float64, error)) (*JoinOptimizer, bool, error) {
	stmt, err := sqlparser.Parse(sql)
	if err != nil {
		return nil, false, fmt.Errorf("parse error: %w", err)
	}

	lb := NewLogicalPlanBuilder(p.catalog)
	logicalPlan, err := lb.Plan(stmt)
	if err != nil {
		return nil, false, fmt.Errorf("logical planning error: %w", err)
	}

	scans, predicates := collectJoinOptimizerInputs(logicalPlan)
	if len(scans) < 2 {
		return nil, false, nil
	}

	tableRows := make([]float64, len(scans))
	for i, scan := range scans {
		tableRows[i] = 1
		if rowEstimator == nil {
			continue
		}
		if scan.TableRef == nil || scan.TableRef.table == nil {
			continue
		}
		rows, err := rowEstimator(scan.TableRef.table.Name)
		if err != nil {
			return nil, false, fmt.Errorf("row estimation failed for table '%s': %w", scan.TableRef.table.Name, err)
		}
		if rows > 0 {
			tableRows[i] = rows
		}
	}

	tableRefIDs := make([]uint64, len(scans))
	for i, scan := range scans {
		if scan != nil && scan.TableRef != nil {
			tableRefIDs[i] = scan.TableRef.refID
		}
	}

	return &JoinOptimizer{
		numTables:        len(scans),
		TableRows:        tableRows,
		Predicates:       predicates,
		TableRefIDs:      tableRefIDs,
		AvailableBuffers: availableBuffers,
		IndexMeta:        newCatalogJoinIndexMeta(scans),
	}, true, nil
}

func collectJoinOptimizerInputs(root LogicalPlanNode) ([]*LogicalScanNode, []Expr) {
	scans := make([]*LogicalScanNode, 0)
	seenScanRef := make(map[uint64]struct{})
	predicates := make([]Expr, 0)

	var walk func(node LogicalPlanNode)
	walk = func(node LogicalPlanNode) {
		if node == nil {
			return
		}

		switch n := node.(type) {
		case *LogicalScanNode:
			if n.TableRef == nil {
				return
			}
			if _, seen := seenScanRef[n.TableRef.refID]; seen {
				return
			}
			seenScanRef[n.TableRef.refID] = struct{}{}
			scans = append(scans, n)
			return
		case *LogicalJoinNode:
			for _, pred := range n.joinOn {
				predicates = append(predicates, collectJoinPredicateAtoms(pred)...)
			}
		case *LogicalFilterNode:
			predicates = append(predicates, collectJoinPredicateAtoms(n.Predicate)...)
		}

		for _, child := range node.Children() {
			walk(child)
		}
	}

	walk(root)
	return scans, predicates
}

func collectJoinPredicateAtoms(expr Expr) []Expr {
	if expr == nil {
		return nil
	}

	if logic, ok := expr.(*BinaryLogicExpression); ok && logic.logicType == And {
		left := collectJoinPredicateAtoms(logic.left)
		right := collectJoinPredicateAtoms(logic.right)
		return append(left, right...)
	}

	if isJoinPredicate(expr) {
		return []Expr{expr}
	}
	return nil
}

func isJoinPredicate(expr Expr) bool {
	if expr == nil {
		return false
	}
	refs := expr.GetReferencedColumns()
	origins := make(map[uint64]struct{})
	for _, col := range refs {
		if col == nil || col.origin == nil {
			continue
		}
		origins[col.origin.refID] = struct{}{}
		if len(origins) > 1 {
			return true
		}
	}
	return false
}

type catalogJoinIndexMeta struct {
	tableByIdx []*catalog.Table
}

func newCatalogJoinIndexMeta(scans []*LogicalScanNode) JoinIndexMetadata {
	tables := make([]*catalog.Table, len(scans))
	for i, scan := range scans {
		if scan != nil && scan.TableRef != nil {
			tables[i] = scan.TableRef.table
		}
	}
	return &catalogJoinIndexMeta{tableByIdx: tables}
}

func (m *catalogJoinIndexMeta) InnerHasJoinIndex(innerTableIdx int, predicates []Expr) bool {
	if innerTableIdx < 0 || innerTableIdx >= len(m.tableByIdx) {
		return false
	}
	tbl := m.tableByIdx[innerTableIdx]
	if tbl == nil {
		return false
	}

	indexedCols := make(map[string]struct{})
	for _, idx := range tbl.Indexes {
		for _, c := range idx.KeySchema {
			indexedCols[c] = struct{}{}
		}
	}
	if len(indexedCols) == 0 {
		return false
	}

	for _, pred := range predicates {
		if predicateUsesIndexedColumn(pred, tbl, indexedCols) {
			return true
		}
	}
	return false
}

func predicateUsesIndexedColumn(expr Expr, target *catalog.Table, indexedCols map[string]struct{}) bool {
	switch e := expr.(type) {
	case *ComparisonExpression:
		if e.compType != Equal {
			return false
		}
		leftCol, leftOK := e.left.(*LogicalColumn)
		rightCol, rightOK := e.right.(*LogicalColumn)
		if !leftOK || !rightOK || leftCol.origin == nil || rightCol.origin == nil {
			return false
		}

		if leftCol.origin.Equals(rightCol.origin) {
			return false
		}

		if leftCol.origin.table == target {
			_, ok := indexedCols[leftCol.cname]
			return ok
		}
		if rightCol.origin.table == target {
			_, ok := indexedCols[rightCol.cname]
			return ok
		}
	case *BinaryLogicExpression:
		return predicateUsesIndexedColumn(e.left, target, indexedCols) ||
			predicateUsesIndexedColumn(e.right, target, indexedCols)
	}
	return false
}
