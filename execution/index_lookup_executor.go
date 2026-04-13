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
	md := e.index.Metadata()
	tableDesc := e.tableHeap.StorageSchema()

	for{
		e.idx++
		if e.idx >= len(e.rids) {
			return false
		}
		rid := e.rids[e.idx]
	
		if e.txn != nil {
			tableTag := transaction.NewTableLockTag(e.tableHeap.oid)
			tupleTag := transaction.NewTupleLockTag(rid)
			if e.plan.ForUpdate {
				if err := e.txn.AcquireLock(tableTag, transaction.LockModeIX); err != nil {
					e.err = err
					return false
				}
				if err := e.txn.AcquireLock(tupleTag, transaction.LockModeX); err != nil {
					e.err = err
					return false
				}
			} else {
				if err := e.txn.AcquireLock(tableTag, transaction.LockModeIS); err != nil {
					e.err = err
					return false
				}
				if err := e.txn.AcquireLock(tupleTag, transaction.LockModeS); err != nil {
					e.err = err
					return false
				}
			}
		}
	
		e.err = e.tableHeap.ReadTuple(nil, rid, e.readBuf, e.plan.ForUpdate)
		if e.err != nil {
			//stale read
			if e.err == ErrTupleDeleted{
				continue
			}
			return false
		}

		// check for key mismatch
		rowKey := keyFromRow(md, tableDesc, e.readBuf)
		if !e.plan.EqualityKey.Equals(rowKey) {
			continue
		}
		
		return true
	}
}

func (e *IndexLookupExecutor) Current() storage.Tuple {
	if e.err != nil || e.idx < 0 || e.idx >= len(e.rids) {
		return storage.EmptyTuple
	}
	rid := e.rids[e.idx]
	desc := e.tableHeap.StorageSchema()

	return storage.FromRawTuple(storage.RawTuple(e.readBuf), desc, rid)
}

func (e *IndexLookupExecutor) Close() error {
	return nil
}

func (e *IndexLookupExecutor) Error() error {
	return e.err
}
