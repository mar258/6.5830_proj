package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"sort"
)

// SortExecutor sorts the input tuples based on the provided ordering expressions.
// It is a blocking operator but uses lazy evaluation (sorts on first Next).
type SortExecutor struct {
	plan *planner.SortNode
	child Executor
	buf []storage.Tuple
	idx int
	done bool
	err error
}

func NewSortExecutor(plan *planner.SortNode, child Executor) *SortExecutor {
	return &SortExecutor{plan:plan, child:child}
}

func (e *SortExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *SortExecutor) Init(ctx *ExecutorContext) error {
	err := e.child.Init(ctx)
	if err != nil{
		return err
	}

	tupleDesc := storage.NewRawTupleDesc(e.plan.OutputSchema())
	e.buf = make([]storage.Tuple, 0)
	for e.child.Next(){
		tup := e.child.Current()
		e.buf = append(e.buf, tup.DeepCopy(tupleDesc))
	}

	orderBy := e.plan.OrderBy
	sort.SliceStable(e.buf, func(i, j int) bool {
		li := e.buf[i]
		lj := e.buf[j]

		for _, ob := range orderBy {
			vi := ob.Expr.Eval(li)
			vj := ob.Expr.Eval(lj)

			cmp := vi.Compare(vj) 
			if cmp == 0 {
				continue
			}

			if ob.Direction == planner.SortOrderAscending {
				return cmp < 0
			}
			// Descending: larger values come first.
			return cmp > 0
		}
		// Fully equal on all keys
		return false
	})
	e.idx = -1

	return nil
}

func (e *SortExecutor) Next() bool {
	if e.done || e.err != nil {
		return false
	}
	e.idx++
	if e.idx >= len(e.buf) {
		e.done = true
		return false
	}
	return true
}

func (e *SortExecutor) Current() storage.Tuple {
	if e.idx < 0 || e.idx >= len(e.buf) {
		return storage.EmptyTuple
	}
	return e.buf[e.idx]
}

func (e *SortExecutor) Error() error {
	return e.err
}

func (e *SortExecutor) Close() error {
	return e.child.Close()
}
