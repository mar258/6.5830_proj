package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// SortMergeJoinExecutor implements the sort-merge join algorithm.
// The planner guarantees that both children are already sorted on their join keys.
// You only need to support Equi-Joins
type SortMergeJoinExecutor struct {
	plan  *planner.SortMergeJoinNode
	left  Executor
	right Executor
	err   error

	leftTuple  storage.Tuple
	rightTuple storage.Tuple
	leftDone   bool
	rightDone  bool

	results []storage.Tuple
	idx     int

	joinedDesc *storage.RawTupleDesc
	joinBuf    []byte
}

func NewSortMergeJoinExecutor(plan *planner.SortMergeJoinNode, left Executor, right Executor) *SortMergeJoinExecutor {
	return &SortMergeJoinExecutor{plan: plan, left: left, right: right}
}

func (e *SortMergeJoinExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *SortMergeJoinExecutor) Init(ctx *ExecutorContext) error {
	e.err = nil
	e.idx = -1
	e.results = nil
	e.leftTuple = storage.Tuple{}
	e.rightTuple = storage.Tuple{}
	e.leftDone = false
	e.rightDone = false

	if err := e.left.Init(ctx); err != nil {
		e.err = err
		return err
	}
	if err := e.right.Init(ctx); err != nil {
		e.err = err
		return err
	}

	e.joinedDesc = storage.NewRawTupleDesc(e.plan.OutputSchema())
	e.joinBuf = make([]byte, e.joinedDesc.BytesPerTuple())

	advanceLeft := func() bool {
		if !e.left.Next() {
			e.leftDone = true
			return false
		}
		e.leftTuple = e.left.Current()
		return true
	}

	advanceRight := func() bool {
		if !e.right.Next() {
			e.rightDone = true
			return false
		}
		e.rightTuple = e.right.Current()
		return true
	}

	keyVals := func(t storage.Tuple, exprs []planner.Expr) []common.Value {
		vals := make([]common.Value, len(exprs))
		for i, ex := range exprs {
			vals[i] = ex.Eval(t)
		}
		return vals
	}

	compareKeys := func(lVals, rVals []common.Value) (cmp int, equal bool) {
		// equal requires no NULLs and all fields equal
		equal = true
		for i := 0; i < len(lVals); i++ {
			lv := lVals[i]
			rv := rVals[i]
			if lv.IsNull() || rv.IsNull() {
				equal = false
			}
			c := lv.Compare(rv)
			if c != 0 {
				return c, false
			}
		}
		return 0, equal
	}

	// done if hit end of left or right 
	if !advanceLeft() || !advanceRight() {
		return nil
	}

	for !e.leftDone && !e.rightDone {
		lKey := keyVals(e.leftTuple, e.plan.LeftKeys)
		rKey := keyVals(e.rightTuple, e.plan.RightKeys)
		cmp, eq := compareKeys(lKey, rKey)

		if eq {
			// Collect duplicate run on left for this key.
			leftRun := []storage.Tuple{e.leftTuple.DeepCopy(storage.NewRawTupleDesc(e.plan.Left.OutputSchema()))}
			for {
				if !advanceLeft() {
					break
				}
				nk := keyVals(e.leftTuple, e.plan.LeftKeys)
				ncmp, neq := compareKeys(nk, lKey)
				if ncmp == 0 && neq {
					leftRun = append(leftRun, e.leftTuple.DeepCopy(storage.NewRawTupleDesc(e.plan.Left.OutputSchema())))
					continue
				}
				break
			}

			// Collect duplicate run on right for this key.
			rightRun := []storage.Tuple{e.rightTuple.DeepCopy(storage.NewRawTupleDesc(e.plan.Right.OutputSchema()))}
			for {
				if !advanceRight() {
					break
				}
				nk := keyVals(e.rightTuple, e.plan.RightKeys)
				ncmp, neq := compareKeys(rKey, nk)
				if ncmp == 0 && neq {
					rightRun = append(rightRun, e.rightTuple.DeepCopy(storage.NewRawTupleDesc(e.plan.Right.OutputSchema())))
					continue
				}
				break
			}

			// Cross product between duplicate runs.
			for _, lt := range leftRun {
				for _, rt := range rightRun {
					joined := storage.MergeTuples(e.joinBuf, e.joinedDesc, lt, rt)
					e.results = append(e.results, joined.DeepCopy(e.joinedDesc))
				}
			}
			continue
		}

		// Not equal: advance the smaller key. If cmp==0 due to NULLs, advance both.
		if cmp < 0 {
			advanceLeft()
		} else if cmp > 0 {
			advanceRight()
		} else {
			advanceLeft()
			advanceRight()
		}
	}

	if err := e.left.Error(); err != nil {
		e.err = err
		return err
	}
	if err := e.right.Error(); err != nil {
		e.err = err
		return err
	}
	return nil
}

func (e *SortMergeJoinExecutor) Next() bool {
	if e.err != nil {
		return false
	}
	e.idx++
	return e.idx >= 0 && e.idx < len(e.results)
}

func (e *SortMergeJoinExecutor) Current() storage.Tuple {
	if e.idx < 0 || e.idx >= len(e.results) {
		return storage.EmptyTuple
	}
	return e.results[e.idx]
}

func (e *SortMergeJoinExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	if err := e.left.Error(); err != nil {
		return err
	}
	if err := e.right.Error(); err != nil {
		return err
	}
	return nil
}

func (e *SortMergeJoinExecutor) Close() error {
	var firstErr error
	if err := e.left.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := e.right.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
