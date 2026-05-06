package planner

import (
	"testing"

	"github.com/xwb1989/sqlparser"
)

// Verifies SQLPlanner wires PrepareCostBasedJoin + PhysicalPlanBuilder.WithCostBasedJoin
// for multi-table queries without failing physical translation.
func TestSQLPlannerCostBasedJoinIntegration(t *testing.T) {
	cat := getBasicTestCatalog()
	_, _ = cat.AddIndex("idx_orders_uid", "orders", "btree", []string{"user_id"})
	_, _ = cat.AddIndex("idx_users_pk", "users", "btree", []string{"id"})

	planner := NewSQLPlanner(cat, []LogicalRule{
		&PredicatePushDownRule{},
	}, []PhysicalConversionRule{
		&SeqScanRule{},
		&IndexScanRule{},
		&IndexLookupRule{},
		&IndexNestedLoopJoinRule{},
		&SortMergeJoinRule{},
		&HashJoinRule{},
		&BlockNestedLoopJoinRule{},
		&ProjectionRule{},
		&FilterRule{},
		&SortRule{},
		&LimitRule{},
		&AggregationRule{},
		&InsertRule{},
		&DeleteRule{},
		&UpdateRule{},
		&ValuesRule{},
	})

	stmt, err := sqlparser.Parse("SELECT * FROM users u, orders o WHERE u.id = o.user_id")
	if err != nil {
		t.Fatal(err)
	}

	lb := NewLogicalPlanBuilder(cat)
	logicalPlan, err := lb.Plan(stmt)
	if err != nil {
		t.Fatal(err)
	}
	optPlan := planner.opt.Optimize(logicalPlan)

	opt, best, scans, ok, prepErr := PrepareCostBasedJoin(optPlan, 100, nil)
	if prepErr != nil {
		t.Fatal(prepErr)
	}
	if !ok || best == nil || len(scans) < 2 {
		t.Fatalf("expected CBO preparation ok for join query (ok=%v best=%v scans=%d)", ok, best != nil, len(scans))
	}

	pb := NewPhysicalPlanBuilder(cat, planner.physicalRules).WithCostBasedJoin(opt, best, scans)
	if _, err := pb.Build(optPlan); err != nil {
		t.Fatalf("physical build with CBO: %v", err)
	}

	if _, err := planner.Plan("SELECT * FROM users u, orders o WHERE u.id = o.user_id", true); err != nil {
		t.Fatalf("SQLPlanner.Plan: %v", err)
	}
}
