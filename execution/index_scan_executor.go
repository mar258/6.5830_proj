package execution

import (
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
)

// IndexScanExecutor executes a range scan over an index.
// It iterates through the B+Tree (or other index type) starting from a specific key
// and traversing in a specific direction (Forward or Backward).
type IndexScanExecutor struct {
	plan      *planner.IndexScanNode
	index     indexing.Index
	tableHeap *TableHeap
	iter      indexing.ScanIterator
	readBuf   []byte
	txn       *transaction.TransactionContext
	err       error
}

func NewIndexScanExecutor(plan *planner.IndexScanNode, index indexing.Index, tableHeap *TableHeap) *IndexScanExecutor {
	return &IndexScanExecutor{plan: plan, index: index, tableHeap: tableHeap}
}

func (e *IndexScanExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *IndexScanExecutor) Init(ctx *ExecutorContext) error {
	var txn *transaction.TransactionContext
	if ctx != nil {
		txn = ctx.GetTransaction()
	}
	e.txn = txn
	e.readBuf = make([]byte, e.tableHeap.StorageSchema().BytesPerTuple())
	iter, err := e.index.Scan(e.plan.StartKey, e.plan.Direction, txn)
	if err != nil {
		return err
	}
	e.iter = iter
	return nil
}

func (e *IndexScanExecutor) Next() bool {
	return e.iter.Next()
}

func (e *IndexScanExecutor) Current() storage.Tuple {
	rid := e.iter.Value()
	desc := e.tableHeap.StorageSchema()
	e.err = e.tableHeap.ReadTuple(e.txn, rid, e.readBuf, e.plan.ForUpdate)
	if e.err != nil {
		return storage.Tuple{}
	}
	return storage.FromRawTuple(storage.RawTuple(e.readBuf), desc, rid)
}

func (e *IndexScanExecutor) Close() error {
	return e.iter.Close()
}

func (e *IndexScanExecutor) Error() error {
	if e.err != nil {
		return e.err
	}
	return e.iter.Error()
}
