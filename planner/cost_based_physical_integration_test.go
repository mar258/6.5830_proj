package planner

import (
	"testing"
)

func TestCBOReorderDisabledUsesRulePlanner(t *testing.T) {
	cat := newEvalNTableChainCatalogTB(t, 2)
	logicalRoot := newEvalNTableChainLogicalJoinTB(t, cat, 2)

	builder := NewPhysicalPlanBuilder(cat, physicalRulesJoinEval())

	p, err := builder.Build(logicalRoot)
	if err != nil {
		t.Fatalf("Build: %v", err)
	}

	ops := collectPhysicalJoinOperators(p)
	if len(ops) != 1 {
		t.Fatalf("expected 1 join, got %d: %v", len(ops), ops)
	}

	t.Logf("rule planner joins: %v", ops)
}

func TestCBOReorderBuildsPhysicalPlan(t *testing.T) {
	cat := newEvalNTableChainCatalogTB(t, 3)
	logicalRoot := newEvalNTableChainLogicalJoinTB(t, cat, 3)

	builder := NewPhysicalPlanBuilder(cat, physicalRulesJoinEval())
	builder.EnableCBOJoinReorder()

	p, err := builder.Build(logicalRoot)
	if err != nil {
		t.Fatalf("Build with CBO reorder: %v", err)
	}

	ops := collectPhysicalJoinOperators(p)
	if len(ops) != 2 {
		t.Fatalf("expected 2 joins for 3 tables, got %d: %v", len(ops), ops)
	}

	t.Logf("CBO physical joins: %v", ops)
	t.Logf("CBO physical root: %T", p)
}

func TestCBOReorderChangesJoinOrderForSkewedTables(t *testing.T) {
	const n = 5

	cat := newEvalNTableChainCatalogTB(t, n)
	logicalRoot := newEvalNTableChainLogicalJoinTB(t, cat, n)

	builder := NewPhysicalPlanBuilder(cat, physicalRulesJoinEval())
	builder.EnableCBOJoinReorder()

	p, err := builder.Build(logicalRoot)
	if err != nil {
		t.Fatalf("Build with CBO reorder: %v", err)
	}

	ops := collectPhysicalJoinOperators(p)
	if len(ops) != n-1 {
		t.Fatalf("expected %d joins, got %d: %v", n-1, len(ops), ops)
	}

	t.Logf("CBO reordered physical joins: %v", ops)
	t.Logf("CBO reordered physical root: %T", p)
}
