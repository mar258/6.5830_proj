package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// The size of block, in bytes, that the join operator is allowed to buffer
const blockSize = 1 << 15

// materializeTuple converts a tuple into a purely virtual tuple (backed by owned Values).
// This prevents buffered tuples from being corrupted when upstream iterators reuse their internal buffers.
func materializeTuple(t storage.Tuple) storage.Tuple {
	n := t.NumColumns()
	vals := make([]common.Value, 0, n)
	for i := 0; i < n; i++ {
		vals = append(vals, t.GetValue(i).Copy())
	}
	return storage.FromValues(vals...)
}

// BlockNestedLoopJoinExecutor implements the block nested loop join algorithm.
// It loads a block of tuples from the left child into memory and then scans the right child
// to find matches. This reduces the number of times the right child is sequentially scanned.
type BlockNestedLoopJoinExecutor struct {
	plan  *planner.NestedLoopJoinNode
	left  Executor
	right Executor

	ctx *ExecutorContext

	// Current left block and join cursors.
	leftBlock     []storage.Tuple
	leftBlockIdx  int
	rightTuple    storage.Tuple
	haveRightTuple bool

	// Last produced joined tuple.
	currJoined storage.Tuple

	err error
	maxLeftTuples int
	leftDesc *storage.RawTupleDesc

	joinedDesc *storage.RawTupleDesc
	joinBuf    []byte
}

// NewBlockNestedLoopJoinExecutor creates a new BlockNestedLoopJoinExecutor.
func NewBlockNestedLoopJoinExecutor(plan *planner.NestedLoopJoinNode, left Executor, right Executor) *BlockNestedLoopJoinExecutor {
	return &BlockNestedLoopJoinExecutor{plan: plan, left:left, right:right}
}

func (e *BlockNestedLoopJoinExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *BlockNestedLoopJoinExecutor) Init(ctx *ExecutorContext) error {
	e.ctx = ctx
	e.err = nil
	e.leftBlock = nil
	e.leftBlockIdx = 0
	e.rightTuple = storage.Tuple{}
	e.haveRightTuple = false
	e.currJoined = storage.Tuple{}

	if err := e.left.Init(ctx); err != nil {
		e.err = err
		return err
	}
	if err := e.right.Init(ctx); err != nil {
		e.err = err
		return err
	}

	// Precompute block capacity (in tuples) based on left tuple schema size.
	leftSchema := e.plan.Left.OutputSchema()
	e.leftDesc = storage.NewRawTupleDesc(leftSchema)
	tupleBytes := e.leftDesc.BytesPerTuple()
	e.maxLeftTuples = blockSize / tupleBytes

	// Output tuple descriptor/buffer for joined tuples.
	e.joinedDesc = storage.NewRawTupleDesc(e.plan.OutputSchema())
	e.joinBuf = make([]byte, e.joinedDesc.BytesPerTuple())

	return nil
}

func (e *BlockNestedLoopJoinExecutor) Next() bool {
	if e.err != nil {
		return false
	}

	for {
		// If we don't currently have a left block, build one from the left child.
		if len(e.leftBlock) == 0 {
			e.leftBlock = make([]storage.Tuple, 0, e.maxLeftTuples)
			e.leftBlockIdx = 0

			for len(e.leftBlock) < e.maxLeftTuples {
				if !e.left.Next() {
					break
				}
				e.leftBlock = append(e.leftBlock, e.left.Current().DeepCopy(e.leftDesc))
			}

			if err := e.left.Error(); err != nil {
				e.err = err
				return false
			}

			// If left is empty, the whole join is exhausted.
			if len(e.leftBlock) == 0 {
				return false
			}

			// For this block, we scan the right side exactly once.
			e.haveRightTuple = false
			e.rightTuple = storage.Tuple{}
			if err := e.right.Init(e.ctx); err != nil {
				e.err = err
				return false
			}
		}

		// Ensure we have a current right tuple to probe the whole buffered left block.
		if !e.haveRightTuple {
			if !e.right.Next() {
				if err := e.right.Error(); err != nil {
					e.err = err
					return false
				}
				// Right exhausted for this block -> move on to the next left block.
				e.leftBlock = nil
				continue
			}

			e.rightTuple = e.right.Current()
			e.leftBlockIdx = 0
			e.haveRightTuple = true
		}

		// Probe the buffered left tuples against the current right tuple.
		for e.leftBlockIdx < len(e.leftBlock) {
			leftT := e.leftBlock[e.leftBlockIdx]
			e.leftBlockIdx++

			joined := storage.MergeTuples(e.joinBuf, e.joinedDesc, leftT, e.rightTuple)
			if planner.ExprIsTrue(e.plan.Predicate.Eval(joined)) {
				e.currJoined = joined
				return true
			}
		}

		// Finished scanning the left block for this right tuple, advance to the next right tuple.
		e.haveRightTuple = false
	}
}

func (e *BlockNestedLoopJoinExecutor) Current() storage.Tuple {
	return e.currJoined
}

func (e *BlockNestedLoopJoinExecutor) Error() error {
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

func (e *BlockNestedLoopJoinExecutor) Close() error {
	var firstErr error
	if err := e.left.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := e.right.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}
