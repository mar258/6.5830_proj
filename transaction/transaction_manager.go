package transaction

import (
	"sync"
	"sync/atomic"

	"github.com/puzpuzpuz/xsync/v3"
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/storage"
)

// activeTxnEntry tracks a running transaction and its starting point in the log.
type activeTxnEntry struct {
	txn      *TransactionContext
	startLsn storage.LSN
}

// TransactionManager is the central component managing the lifecycle of transactions.
// It coordinates with the LockManager for concurrency control and the LogManager for
// Write-Ahead Logging (WAL) and recovery.
type TransactionManager struct {
	// activeTxns maps TransactionIDs to their runtime context and metadata
	activeTxns *xsync.MapOf[common.TransactionID, activeTxnEntry]

	logManager  storage.LogManager
	bufferPool  *storage.BufferPool
	lockManager *LockManager

	nextTxnID atomic.Uint64
	// Pool to recycle transaction contexts
	txnPool sync.Pool
}

// NewTransactionManager initializes the transaction manager.
func NewTransactionManager(logManager storage.LogManager, bufferPool *storage.BufferPool, lockManager *LockManager) *TransactionManager {
	return &TransactionManager{
		activeTxns:  xsync.NewMapOf[common.TransactionID, activeTxnEntry](),
		logManager:  logManager,
		bufferPool:  bufferPool,
		lockManager: lockManager,
		txnPool: sync.Pool{
			New: func() any {
				return &TransactionContext{
					lm: lockManager,
					logRecords: newLogRecordBuffer(),
					heldLocks:  make(map[DBLockTag]DBLockMode),
				}
			},
		},
	}
}

func (tm *TransactionManager) getTxnFromPool() *TransactionContext {
	if v := tm.txnPool.Get(); v != nil {
		return v.(*TransactionContext)
	}
	return &TransactionContext{
		logRecords: newLogRecordBuffer(),
		heldLocks:  make(map[DBLockTag]DBLockMode),
	}
}

// putTxnToPool clears pooled state and returns the context to the pool.
// Call only after the transaction is fully finished (no longer in activeTxns).
func (tm *TransactionManager) putTxnToPool(txn *TransactionContext) {
	if txn == nil {
		return
	}
	// tm.activeTxns.Delete(txn.ID())
	// txn.id = 0
	// txn.lm = nil
	// txn.logRecords.reset()
	// txn.heldLocks = make(map[DBLockTag]DBLockMode)
	// txn.abortActions = txn.abortActions[:0]
	// txn.commitActions = txn.commitActions[:0]
	tm.txnPool.Put(txn)
}

// Begin starts a new transaction and returns the initialized context.
func (tm *TransactionManager) Begin() (*TransactionContext, error) {
	tid := common.TransactionID(tm.nextTxnID.Add(1))
	txn := tm.txnPool.Get().(*TransactionContext)
	txn.id = tid
	clear(txn.heldLocks)
	txn.abortActions = txn.abortActions[:0]
	txn.commitActions = txn.commitActions[:0]

	lsn, err := tm.logManager.Append(txn.NewBeginTransactionRecord())
	if err != nil {
		txn.logRecords.reset()
		tm.txnPool.Put(txn)
		return nil, err
	}
	tm.activeTxns.Store(tid, activeTxnEntry{txn: txn, startLsn: lsn})

	return txn, nil
}

// Commit completes a transaction and makes its effects durable and visible.
func (tm *TransactionManager) Commit(txn *TransactionContext) error {
	lsn, err := tm.logManager.Append(txn.NewCommitRecord())
	if err != nil{
		return err
	}
	err = tm.logManager.WaitUntilFlushed(lsn)
	if err != nil{
		return err
	}

	// Execute In-Memory changes (Indexes) after flushed. Think about how this should interleave with the commit logic.
	for _, task := range txn.commitActions {
		task.Target.Invoke(task.Type, task.Key, task.RID)
	}

	txn.ReleaseAllLocks()
	tm.activeTxns.Delete(txn.id)
	txn.Reset(txn.id)
	tm.putTxnToPool(txn)
	return nil
}

// Abort stops a transaction and ensures its effects are rolled back
func (tm *TransactionManager) Abort(txn *TransactionContext) error {
	// Rollback In-Memory changes (Indexes)
	// YOU SHOULD NOT NEED TO MODIFY THIS LOGIC
	for i := len(txn.abortActions) - 1; i >= 0; i-- {
		cleanupTask := txn.abortActions[i]
		cleanupTask.Target.Invoke(cleanupTask.Type, cleanupTask.Key, cleanupTask.RID)
	}

	// Add your implementation here
	for i := txn.logRecords.len() -1; i > 0; i--{
		rec := txn.logRecords.get(i)
		if rec.RecordType() == storage.LogInsert{
			lsn, err := tm.logManager.Append(txn.NewInsertCLR(rec))
			if err != nil{
				return err
			}
			pageId:= rec.RID().PageID
			frame, err := tm.bufferPool.GetPage(pageId)
			frame.PageLatch.Lock()
			hp := frame.AsHeapPage()
			hp.MarkDeleted(rec.RID(), true)
			frame.MonotonicallyUpdateLSN(lsn)
			frame.PageLatch.Unlock()
			tm.bufferPool.UnpinPage(frame, true)

		}else if rec.RecordType() == storage.LogDelete{
			lsn, err := tm.logManager.Append(txn.NewDeleteCLR(rec))
			if err != nil{
				return err
			}
			pageId:= rec.RID().PageID
			frame, err := tm.bufferPool.GetPage(pageId)
			frame.PageLatch.Lock()
			hp := frame.AsHeapPage()
			hp.MarkDeleted(rec.RID(), false)
			frame.MonotonicallyUpdateLSN(lsn)
			frame.PageLatch.Unlock()
			tm.bufferPool.UnpinPage(frame, true)

		}else if rec.RecordType() == storage.LogUpdate{
			lsn, err := tm.logManager.Append(txn.NewUpdateCLR(rec))
			if err != nil{
				return err
			}

			pageId:= rec.RID().PageID
			frame, err := tm.bufferPool.GetPage(pageId)
			frame.PageLatch.Lock()
			hp := frame.AsHeapPage()
			copy(hp.AccessTuple(rec.RID()), rec.BeforeImage())
			frame.MonotonicallyUpdateLSN(lsn)
			frame.PageLatch.Unlock()
			tm.bufferPool.UnpinPage(frame, true)
		}
	}

	_, err := tm.logManager.Append(txn.NewAbortRecord())
	if err != nil{
		return err
	}
	
	txn.ReleaseAllLocks()
	tm.activeTxns.Delete(txn.id)
	txn.Reset(txn.id)
	tm.putTxnToPool(txn)

	return nil
}

// RestartTransactionForRecovery is used during database recovery (ARIES Redo phase).
// It reconstructs a TransactionContext for a transaction that was active at the time of the crash.
//
// Hint: You do not need to worry about this function until lab 4
func (tm *TransactionManager) RestartTransactionForRecovery(txnId common.TransactionID) *TransactionContext {
	panic("unimplemented")
}

// ATTEntry represents a snapshot of an active transaction for the Active Transaction Table (ATT).
type ATTEntry struct {
	ID       common.TransactionID
	StartLSN storage.LSN
}

// GetActiveTransactionsSnapshot returns a snapshot of currently active transaction IDs and their start LSNs.
//
// Hint: You do not need to worry about this function until lab 4
func (tm *TransactionManager) GetActiveTransactionsSnapshot() []ATTEntry {
	panic("unimplemented")
}
