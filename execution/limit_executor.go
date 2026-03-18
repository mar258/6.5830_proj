package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// LimitExecutor limits the number of tuples returned by the child executor.
type LimitExecutor struct {
	plan *planner.LimitNode
	child Executor
	limit int
	currAmt int
}

func NewLimitExecutor(plan *planner.LimitNode, child Executor) *LimitExecutor {
	return &LimitExecutor{plan: plan, child: child, limit: plan.Limit, currAmt: 0}
}

func (e *LimitExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *LimitExecutor) Init(ctx *ExecutorContext) error {
	e.currAmt = 0
	return e.child.Init(ctx)
}

func (e *LimitExecutor) Next() bool {
	if e.currAmt >= e.limit{
		return false
	}
	e.currAmt++
	return e.child.Next()
}

func (e *LimitExecutor) Current() storage.Tuple {
	return e.child.Current()
}

func (e *LimitExecutor) Error() error {
	return e.child.Error()
}

func (e *LimitExecutor) Close() error {
	return e.child.Close()
}
