package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
)

// HashJoinExecutor implements the hash join algorithm.
// It builds a hash table from the left child and probes it with the right child.
// It only supports Equi-Joins.
type HashJoinExecutor struct {
	plan  *planner.HashJoinNode
	left  Executor
	right Executor
	err   error

	hashTable *ExecutionHashTable[[]storage.Tuple]

	joinedDesc *storage.RawTupleDesc
	joinBuf    []byte

	results []storage.Tuple
	idx     int
}

// NewHashJoinExecutor creates a new HashJoinExecutor.
func NewHashJoinExecutor(plan *planner.HashJoinNode, left Executor, right Executor) *HashJoinExecutor {
	return &HashJoinExecutor{plan: plan, left: left, right: right}
}

func (e *HashJoinExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *HashJoinExecutor) Init(ctx *ExecutorContext) error {
	e.err = nil
	e.idx = -1
	e.results = nil

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

	leftDesc := storage.NewRawTupleDesc(e.plan.Left.OutputSchema())
	keySchema := storage.NewRawTupleDesc(typesFromExprs(e.plan.LeftKeys))
	e.hashTable = NewExecutionHashTable[[]storage.Tuple](keySchema)

	keyVals := func(t storage.Tuple, exprs []planner.Expr) ([]common.Value, bool) {
		vals := make([]common.Value, len(exprs))
		for i, ex := range exprs {
			v := ex.Eval(t)
			if v.IsNull() {
				return nil, false
			}
			vals[i] = v.Copy()
		}
		return vals, true
	}

	// create hash table
	for e.left.Next() {
		leftTuple := e.left.Current()
		lVals, ok := keyVals(leftTuple, e.plan.LeftKeys)
		if !ok {
			continue
		}
		lKey := storage.FromValues(lVals...)
		bucket, exists := e.hashTable.Get(lKey)
		leftCopy := leftTuple.DeepCopy(leftDesc)
		if exists {
			bucket = append(bucket, leftCopy)
		} else {
			bucket = []storage.Tuple{leftCopy}
		}
		e.hashTable.Insert(lKey, bucket)
	}
	if err := e.left.Error(); err != nil {
		e.err = err
		return err
	}

	// iterate right and probe
	for e.right.Next() {
		rightTuple := e.right.Current()
		rVals, ok := keyVals(rightTuple, e.plan.RightKeys)
		if !ok {
			continue
		}
		rKey := storage.FromValues(rVals...)
		bucket, exists := e.hashTable.Get(rKey)
		if !exists {
			continue
		}
		for _, leftTuple := range bucket {
			joined := storage.MergeTuples(e.joinBuf, e.joinedDesc, leftTuple, rightTuple)
			e.results = append(e.results, joined.DeepCopy(e.joinedDesc))
		}
	}
	if err := e.right.Error(); err != nil {
		e.err = err
		return err
	}

	return nil
}

func (e *HashJoinExecutor) Next() bool {
	if e.err != nil {
		return false
	}
	e.idx++
	return e.idx >= 0 && e.idx < len(e.results)
}

func (e *HashJoinExecutor) Current() storage.Tuple {
	if e.idx < 0 || e.idx >= len(e.results) {
		return storage.EmptyTuple
	}
	return e.results[e.idx]
}

func (e *HashJoinExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	if err := e.left.Error(); err != nil {
		return err
	}
	if err := e.right.Error(); err != nil {
		return err
	}
	return e.err
}

func (e *HashJoinExecutor) Close() error {
	var firstErr error
	if err := e.left.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := e.right.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func typesFromExprs(exprs []planner.Expr) []common.Type {
	types := make([]common.Type, len(exprs))
	for i, ex := range exprs {
		types[i] = ex.OutputType()
	}
	return types
}
