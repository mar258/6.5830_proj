package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
)

// IndexNestedLoopJoinExecutor implements an index nested loop join.
//
// It scans the left child. For each left tuple, it evaluates the left-side
// join key expressions, probes the right table's index, fetches matching right
// tuples by RID, and returns left || right.
type IndexNestedLoopJoinExecutor struct {
	plan           *planner.IndexNestedLoopJoinNode
	left           Executor
	rightIndex     indexing.Index
	rightTableHeap *TableHeap

	ctx *ExecutorContext
	err error

	leftTuple storage.Tuple
	current   storage.Tuple

	// Matching right-side RIDs for the current left tuple.
	matches []common.RecordID
	matchIx int

	// Reusable buffers.
	rightBuf []byte
	outBuf   []byte
	outDesc  *storage.RawTupleDesc
}

// NewIndexJoinExecutor creates a new IndexNestedLoopJoinExecutor.
// It assumes the left table is accessed via the provided rightIndex and rightTableHeap.
func NewIndexJoinExecutor(plan *planner.IndexNestedLoopJoinNode, left Executor, rightIndex indexing.Index, rightTableHeap *TableHeap) *IndexNestedLoopJoinExecutor {
	return &IndexNestedLoopJoinExecutor{
		plan:           plan,
		left:           left,
		rightIndex:     rightIndex,
		rightTableHeap: rightTableHeap,
		matches:        make([]common.RecordID, 0),
	}
}

func (e *IndexNestedLoopJoinExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *IndexNestedLoopJoinExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	e.err = nil
	e.leftTuple = storage.Tuple{}
	e.current = storage.Tuple{}
	e.matches = e.matches[:0]
	e.matchIx = 0

	if e.rightTableHeap != nil {
		e.rightBuf = make([]byte, e.rightTableHeap.StorageSchema().BytesPerTuple())
	}

	e.outDesc = storage.NewRawTupleDesc(e.plan.OutputSchema())
	e.outBuf = make([]byte, e.outDesc.BytesPerTuple())

	return e.left.Init(ctx)
}

func (e *IndexNestedLoopJoinExecutor) Next() bool {
	for {
		// First, consume any right-side matches for the current left tuple.
		for e.matchIx < len(e.matches) {
			rid := e.matches[e.matchIx]
			e.matchIx++

			err := e.rightTableHeap.ReadTuple(e.txn(), rid, e.rightBuf, e.plan.ForUpdate)
			if err != nil {
				// With lazy index deletes, an index may still point to a deleted heap tuple.
				// Skip stale entries instead of failing the join.
				if err == ErrTupleDeleted {
					continue
				}
				e.err = err
				return false
			}

			rightTuple := storage.FromRawTuple(
				storage.RawTuple(e.rightBuf),
				e.rightTableHeap.StorageSchema(),
				rid,
			)

			e.current = storage.MergeTuples(e.outBuf, e.outDesc, e.leftTuple, rightTuple)
			return true
		}

		// No pending matches. Advance the left child.
		if !e.left.Next() {
			e.err = e.left.Error()
			return false
		}

		e.leftTuple = e.left.Current()

		key, skip, err := e.makeProbeKey(e.leftTuple)
		if err != nil {
			e.err = err
			return false
		}
		if skip {
			continue
		}

		e.matches = e.matches[:0]
		e.matches, err = e.rightIndex.ScanKey(key, e.matches, e.txn())
		if err != nil {
			e.err = err
			return false
		}
		e.matchIx = 0
	}
}

func (e *IndexNestedLoopJoinExecutor) Current() storage.Tuple {
	return e.current
}

func (e *IndexNestedLoopJoinExecutor) Error() error {
	return e.err
}

func (e *IndexNestedLoopJoinExecutor) Close() error {
	if e.left != nil {
		return e.left.Close()
	}
	return nil
}

func (e *IndexNestedLoopJoinExecutor) txn() *transaction.TransactionContext {
	if e.ctx == nil {
		return nil
	}
	return e.ctx.txn
}

func (e *IndexNestedLoopJoinExecutor) makeProbeKey(leftTuple storage.Tuple) (indexing.Key, bool, error) {
	md := e.rightIndex.Metadata()
	keyRaw := make(storage.RawTuple, md.KeySize())

	for i, expr := range e.plan.LeftKeys {
		value := expr.Eval(leftTuple)
		if value.IsNull() {
			return indexing.Key{}, true, nil
		}
		md.KeySchema.SetValue(keyRaw, i, value)
	}

	return md.AsKey(keyRaw), false, nil
}
