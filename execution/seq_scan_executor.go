package execution

import (
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
)

// SeqScanExecutor implements a sequential scan over a table.
type SeqScanExecutor struct {
	tableHeap *TableHeap
	plan      *planner.SeqScanNode
	iter      TableHeapIterator
	iterBuf   []byte
}

// NewSeqScanExecutor creates a new SeqScanExecutor.
func NewSeqScanExecutor(plan *planner.SeqScanNode, tableHeap *TableHeap) *SeqScanExecutor {
	return &SeqScanExecutor{tableHeap: tableHeap, plan: plan}
}

func (e *SeqScanExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *SeqScanExecutor) Init(ctx *ExecutorContext) error {
	var txn *transaction.TransactionContext
	if ctx != nil {
		txn = ctx.GetTransaction()
	}
	e.iterBuf = make([]byte, e.tableHeap.StorageSchema().BytesPerTuple())
	iter, err := e.tableHeap.Iterator(txn, e.plan.Mode, e.iterBuf)
	if err != nil {
		return err
	}
	e.iter = iter
	return nil
}

func (e *SeqScanExecutor) Next() bool {
	return e.iter.Next()
}

func (e *SeqScanExecutor) Current() storage.Tuple {
	desc := e.tableHeap.StorageSchema()
	return storage.FromRawTuple(e.iter.CurrentTuple(), desc, e.iter.CurrentRID())
}

func (e *SeqScanExecutor) Error() error {
	return e.iter.Error()
}

func (e *SeqScanExecutor) Close() error {
	return e.iter.Close()
}
