package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// ProjectionExecutor evaluates a list of expressions on the input tuples
// and produces a new tuple containing the results of those expressions.
type ProjectionExecutor struct {
	plan  *planner.ProjectionNode
	child Executor
	tuple storage.Tuple
}

// NewProjectionExecutor creates a new ProjectionExecutor.
func NewProjectionExecutor(plan *planner.ProjectionNode, child Executor) *ProjectionExecutor {
	return &ProjectionExecutor{plan: plan, child: child}
}

func (e *ProjectionExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *ProjectionExecutor) Init(ctx *ExecutorContext) error {
	return e.child.Init(ctx)
}

func (e *ProjectionExecutor) Next() bool {
	if !e.child.Next() {
		return false
	}
	input := e.child.Current()
	vals := make([]common.Value, 0, len(e.plan.Expressions))
	for _, expr := range e.plan.Expressions {
		v := expr.Eval(input)
		vals = append(vals, v.Copy())
	}
	e.tuple = storage.FromValues(vals...)
	return true
}

func (e *ProjectionExecutor) Current() storage.Tuple {
	return e.tuple
}

func (e *ProjectionExecutor) Error() error {
	return e.child.Error()
}

func (e *ProjectionExecutor) Close() error {
	return e.child.Close()
}
