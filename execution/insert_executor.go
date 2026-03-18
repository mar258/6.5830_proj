package execution

import (
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
	"mit.edu/dsg/godb/common"
)

// InsertExecutor executes an INSERT query.
// It consumes tuples from its child (which could be a ValuesExecutor or a SELECT query),
// inserts them into the TableHeap, and updates all associated indexes.
//
// For this course, you may assume that the child does not read from the table you are inserting into
type InsertExecutor struct {
	plan *planner.InsertNode
	child Executor
	tableHeap *TableHeap
	indexes []indexing.Index
	txn *transaction.TransactionContext
	done bool
	err error
	insertedCount int64
}

func NewInsertExecutor(plan *planner.InsertNode, child Executor, tableHeap *TableHeap, indexes []indexing.Index) *InsertExecutor {
	return &InsertExecutor{plan: plan, child: child, tableHeap: tableHeap, indexes: indexes}
}

func (e *InsertExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *InsertExecutor) Init(ctx *ExecutorContext) error {
	e.insertedCount = 0
	e.done = false
	e.err = nil

	if ctx != nil{
		e.txn = ctx.GetTransaction()
	}else{
		e.txn = nil
	}

	return e.child.Init(ctx)
}

func (e *InsertExecutor) Next() bool {
	if e.done || e.err != nil{
		return false
	}
	var count int64
	desc := e.tableHeap.StorageSchema()
	for e.child.Next(){
		tup := e.child.Current()

		row := make([]byte, desc.BytesPerTuple())
		tup.WriteToBuffer(row, desc)
		rid, err := e.tableHeap.InsertTuple(e.txn, storage.RawTuple(row))
		if err != nil{
			e.err = err
			return false
		}

		for _, idx := range e.indexes {
			md := idx.Metadata()

			keyVals := make([]common.Value, len(md.ProjectionList))
			for i, colIdx := range md.ProjectionList {
				keyVals[i] = tup.GetValue(colIdx)
			}

			keyTuple := storage.FromValues(keyVals...)
			keyBuf := make([]byte, md.KeySchema.BytesPerTuple())
			keyTuple.WriteToBuffer(keyBuf, md.KeySchema)
			key := md.AsKey(storage.RawTuple(keyBuf))

			if err := idx.InsertEntry(key, rid, e.txn); err != nil {
				e.err = err
				return false
			}
		}

		count++
	}

	e.insertedCount = count
	e.done = true
	return true
}

func (e *InsertExecutor) Current() storage.Tuple {
	return storage.FromValues(common.NewIntValue((e.insertedCount)))
}

func (e *InsertExecutor) Close() error {
	return e.child.Close()
}

func (e *InsertExecutor) Error() error {
	return e.err
}
