package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/common"
)

// AggregateExecutor implements hash-based aggregation.
type AggregateExecutor struct {
	plan *planner.AggregateNode
	child Executor
	err error
	hashTable *ExecutionHashTable[*aggState]
	results   []storage.Tuple
	idx int
}

type aggState struct {
    counts []int64
    sums   []int64
    mins   []common.Value
    maxs   []common.Value
    seen   []bool // for SUM/MIN/MAX to decide NULL output
}

func NewAggregateExecutor(plan *planner.AggregateNode, child Executor) *AggregateExecutor {
	return &AggregateExecutor{plan: plan, child: child}
}

func (e *AggregateExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *AggregateExecutor) Init(ctx *ExecutorContext) error {
    e.err = nil
    e.results = nil
    e.idx = -1

    if err := e.child.Init(ctx); err != nil {
        e.err = err
        return err
    }

    // Build key schema for GROUP BY (may be empty).
    groupTypes := make([]common.Type, len(e.plan.GroupByClause))
    for i, expr := range e.plan.GroupByClause {
        groupTypes[i] = expr.OutputType()
    }
    keySchema := storage.NewRawTupleDesc(groupTypes)
    e.hashTable = NewExecutionHashTable[*aggState](keySchema)

    numAgg := len(e.plan.AggClauses)
    initialized := false

    // Consume all input tuples and update aggregate state.
    for e.child.Next() {
        initialized = true
        tup := e.child.Current()

        // Compute group key tuple.
        keyVals := make([]common.Value, len(e.plan.GroupByClause))
        for i, expr := range e.plan.GroupByClause {
            keyVals[i] = expr.Eval(tup).Copy()
        }
        keyTuple := storage.FromValues(keyVals...)

        state, ok := e.hashTable.Get(keyTuple)
        if !ok {
            state = &aggState{
                counts: make([]int64, numAgg),
                sums:   make([]int64, numAgg),
                mins:   make([]common.Value, numAgg),
                maxs:   make([]common.Value, numAgg),
                seen:   make([]bool, numAgg),
            }
            e.hashTable.Insert(keyTuple, state)
        }

        for i, clause := range e.plan.AggClauses {
            v := clause.Expr.Eval(tup)

            switch clause.Type {
            case planner.AggCount:
                // COUNT(expr): ignore NULLs. COUNT(*) is represented as COUNT(1) by planner/tests.
                if !v.IsNull() {
                    state.counts[i]++
                }
            case planner.AggSum:
                // SUM ignores NULLs; if all NULLs, output NULL.
                if !v.IsNull() {
                    state.seen[i] = true
                    state.sums[i] += v.IntValue()
                }
            case planner.AggMin:
                if !v.IsNull() {
                    if !state.seen[i] || v.Compare(state.mins[i]) < 0 {
                        state.mins[i] = v.Copy()
                        state.seen[i] = true
                    }
                }
            case planner.AggMax:
                if !v.IsNull() {
                    if !state.seen[i] || v.Compare(state.maxs[i]) > 0 {
                        state.maxs[i] = v.Copy()
                        state.seen[i] = true
                    }
                }
            }
        }
    }

    if childErr := e.child.Error(); childErr != nil {
        e.err = childErr
        return childErr
    }

    // if there were no input tuples at all, return 0 output rows
    if !initialized {
        e.results = nil
        return nil
    }

    // Materialize output rows from the hash table.
    e.results = make([]storage.Tuple, 0)
    e.hashTable.Iterate(func(key storage.Tuple, state *aggState) {
        outVals := make([]common.Value, 0, len(e.plan.GroupByClause)+len(e.plan.AggClauses))

        // Group By fields first (if any).
        for i := 0; i < len(e.plan.GroupByClause); i++ {
            outVals = append(outVals, key.GetValue(i).Copy())
        }

        // Aggregates.
        for i, clause := range e.plan.AggClauses {
            switch clause.Type {
            case planner.AggCount:
                outVals = append(outVals, common.NewIntValue(state.counts[i]))
            case planner.AggSum:
                if state.seen[i] {
                    outVals = append(outVals, common.NewIntValue(state.sums[i]))
                } else {
                    outVals = append(outVals, common.NewNullInt())
                }
            case planner.AggMin:
                if state.seen[i] {
                    outVals = append(outVals, state.mins[i].Copy())
                } else if clause.Expr.OutputType() == common.StringType {
                    outVals = append(outVals, common.NewNullString())
                } else {
                    outVals = append(outVals, common.NewNullInt())
                }
            case planner.AggMax:
                if state.seen[i] {
                    outVals = append(outVals, state.maxs[i].Copy())
                } else if clause.Expr.OutputType() == common.StringType {
                    outVals = append(outVals, common.NewNullString())
                } else {
                    outVals = append(outVals, common.NewNullInt())
                }
            }
        }

        e.results = append(e.results, storage.FromValues(outVals...))
    })

    return nil
}

func (e *AggregateExecutor) Next() bool {
	if e.err != nil{
		return false
	}
	e.idx++
	return e.idx >= 0 && e.idx < len(e.results)
}

func (e *AggregateExecutor) Current() storage.Tuple {
	if e.idx < 0 || e.idx >= len(e.results){
		return storage.EmptyTuple
	}
	return e.results[e.idx]
}

func (e *AggregateExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	if err := e.child.Error(); err != nil {
		return err
	}
	return e.err
}

func (e *AggregateExecutor) Close() error {
	return e.child.Close()
}
