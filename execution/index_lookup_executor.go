package execution

import (
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/indexing"
	"mit.edu/dsg/godb/planner"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
)

// IndexLookupExecutor implements a Point Lookup using an index. Unlike a full Index Scan, which iterates over a
// range of keys, this executor efficiently retrieves only the tuples that match a specific equality key
// (e.g., "SELECT * FROM users WHERE id = 5").
type IndexLookupExecutor struct {
	plan      *planner.IndexLookupNode
	index     indexing.Index
	tableHeap *TableHeap
	txn       *transaction.TransactionContext
	rids      []common.RecordID
	readBuf   []byte
	idx       int
	err       error
}

func NewIndexLookupExecutor(plan *planner.IndexLookupNode, index indexing.Index, tableHeap *TableHeap) *IndexLookupExecutor {
	return &IndexLookupExecutor{plan: plan, index: index, tableHeap: tableHeap}
}

func (e *IndexLookupExecutor) PlanNode() planner.PlanNode {
	return e.plan
}

func (e *IndexLookupExecutor) Init(ctx *ExecutorContext) error {
	e.err = nil
	var txn *transaction.TransactionContext
	if ctx != nil {
		txn = ctx.GetTransaction()
	}
	e.txn = txn
	e.readBuf = make([]byte, e.tableHeap.StorageSchema().BytesPerTuple())
	e.rids = nil
	e.idx = -1

	rids, err := e.index.ScanKey(e.plan.EqualityKey, nil, txn)
	if err != nil {
		e.err = err
		return err
	}
	e.rids = rids
	return nil
}

func (e *IndexLookupExecutor) Next() bool {
	if e.err != nil {
		return false
	}
	e.idx++
	if e.idx >= len(e.rids) {
		return false
	}
	return true
}

func (e *IndexLookupExecutor) Current() storage.Tuple {
	if e.err != nil || e.idx < 0 || e.idx >= len(e.rids) {
		return storage.EmptyTuple
	}
	rid := e.rids[e.idx]
	desc := e.tableHeap.StorageSchema()
	e.err = e.tableHeap.ReadTuple(e.txn, rid, e.readBuf, e.plan.ForUpdate)
	if e.err != nil {
		return storage.Tuple{}
	}
	return storage.FromRawTuple(storage.RawTuple(e.readBuf), desc, rid)
}

func (e *IndexLookupExecutor) Close() error {
	return nil
}

func (e *IndexLookupExecutor) Error() error {
	return e.err
}
