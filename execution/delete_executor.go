package execution

import (
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
	"mit.edu/dsg/godb/common"
)

// DeleteExecutor executes a DELETE query.
// It iterates over the child (which produces the tuples to be deleted with all rows read),
// removes them from the TableHeap, and cleans up all associated Index entries.
type DeleteExecutor struct {
	plan *planner.DeleteNode
	child Executor
	tableHeap *TableHeap
	indexes []indexing.Index
	txn *transaction.TransactionContext
	err error
	deleted int64
	done bool
}

func NewDeleteExecutor(plan *planner.DeleteNode, child Executor, tableHeap *TableHeap, indexes []indexing.Index) *DeleteExecutor {
	return &DeleteExecutor{plan: plan, child: child, tableHeap: tableHeap, indexes: indexes}
}

func (e *DeleteExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *DeleteExecutor) Init(ctx *ExecutorContext) error {
	if ctx != nil{
		e.txn = ctx.GetTransaction()
	}else{
		e.txn = nil
	}

	return e.child.Init(ctx)
}

func (e *DeleteExecutor) Next() bool {
	if e.done || e.err != nil{
		return false
	}

	// desc := e.tableHeap.StorageSchema()

	for e.child.Next(){
		tup := e.child.Current()
		rid := tup.RID()

		err := e.tableHeap.DeleteTuple(e.txn, rid)
		if err != nil{
			e.err = err
			return false
		}

		for _, idx := range e.indexes {
			md := idx.Metadata()

			oldBuf := make([]byte, md.KeySchema.BytesPerTuple())
			oldKey := md.AsKey(storage.RawTuple(oldBuf))
			err = idx.DeleteEntry(oldKey, rid, e.txn)
			if err != nil{
				e.err = err
				return false
			}
		}
		e.deleted++
	}

	e.done = true
	return true
}

func (e *DeleteExecutor) Current() storage.Tuple {
	return storage.FromValues(common.NewIntValue((e.deleted)))
}

func (e *DeleteExecutor) Close() error {
	return e.child.Close()
}

func (e *DeleteExecutor) Error() error {
	return e.err
}
