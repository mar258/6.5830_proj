package transaction

import (
	"fmt"
	"sync"
	"mit.edu/dsg/godb/common"
)

// DBLockTag identifies a unique resource (Table or Tuple). It represents Tuple if it has a full RecordID, and
// represents a table if only the oid is set and the rest are set to -1
type DBLockTag struct {
	common.RecordID
}

// NewTableLockTag creates a DBLockTag representing a whole table.
func NewTableLockTag(oid common.ObjectID) DBLockTag {
	return DBLockTag{
		RecordID: common.RecordID{
			PageID: common.PageID{
				Oid:     oid,
				PageNum: -1,
			},
			Slot: -1,
		},
	}
}

// NewTupleLockTag creates a DBLockTag representing a specific tuple (row).
func NewTupleLockTag(rid common.RecordID) DBLockTag {
	return DBLockTag{
		RecordID: rid,
	}
}

func (t DBLockTag) String() string {
	if t.PageNum == -1 {
		return fmt.Sprintf("Table(%d)", t.Oid)
	}
	return fmt.Sprintf("Tuple(%d, %d, %d)", t.Oid, t.PageNum, t.Slot)
}

// DBLockMode represents the type of access a transaction is requesting.
// GoDB supports a standard Multi-Granularity Locking hierarchy.
type DBLockMode int

const (
	// LockModeS (Shared) allows reading a resource. Multiple transactions can hold S locks simultaneously.
	LockModeS DBLockMode = iota
	// LockModeX (Exclusive) allows modification. It is incompatible with all other modes.
	LockModeX
	// LockModeIS (Intent Shared) indicates the intention to read resources at a lower level (e.g., locking a table IS to read tuples).
	LockModeIS
	// LockModeIX (Intent Exclusive) indicates the intention to modify resources at a lower level (e.g., locking a table IX to modify tuples).
	LockModeIX
	// LockModeSIX (Shared Intent Exclusive) allows reading the resource (like S) AND the intention to modify lower-level resources (like IX).
	LockModeSIX
)

func (m DBLockMode) String() string {
	switch m {
	case LockModeS:
		return "LockModeS"
	case LockModeX:
		return "LockModeX"
	case LockModeIS:
		return "LockModeIS"
	case LockModeIX:
		return "LockModeIX"
	case LockModeSIX:
		return "LockModeSIX"
	}
	return "Unknown lock mode"
}

type LockRequest struct {
	TID     common.TransactionID
	Tag     DBLockTag
	Mode    DBLockMode
	Granted bool
}

type Lock struct {
	mu sync.Mutex
	cond      *sync.Cond
	waitQueue []LockRequest
	holders map[common.TransactionID]DBLockMode
}

// LockManager manages the granting, releasing, and waiting of locks on database resources.
type LockManager struct {
	mu sync.Mutex
	lockTable map[DBLockTag]*Lock
	txnLocks map[common.TransactionID]map[DBLockTag]DBLockMode
	lockReqPool sync.Pool
}

// NewLockManager initializes a new LockManager.
func NewLockManager() *LockManager {
	return &LockManager{
		lockTable: make(map[DBLockTag]*Lock),
		txnLocks:  make(map[common.TransactionID]map[DBLockTag]DBLockMode),
		lockReqPool: sync.Pool{
			New: func() any { return &Lock{} },
		},
	}
}

func (lm *LockManager) acquireLock() *Lock {
	r := lm.lockReqPool.Get().(*Lock)
	return r
}

func (lm *LockManager) releaseLock(r *Lock) {
	if r == nil {
		return
	}
	r.mu = sync.Mutex{}
	r.cond = nil
	r.waitQueue = nil
	r.holders = nil
	lm.lockReqPool.Put(r)
}

// Lock acquires a lock on a specific resource (Table or Tuple) with the requested mode. If the lock cannot be acquired
// immediately, the transaction blocks until it is granted or aborted. It returns nil if the lock is successfully
// acquired, or GoDBError(DeadlockError) in case of a (potential or detected) deadlock.
func (lm *LockManager) Lock(tid common.TransactionID, tag DBLockTag, mode DBLockMode) error {
	lm.mu.Lock()
	res, exists := lm.lockTable[tag]
	if !exists{
		// get from pool or allocate new
		res = lm.acquireLock()
		res.holders = make(map[common.TransactionID]DBLockMode)
		res.waitQueue = make([]LockRequest, 0)
		res.cond = sync.NewCond(&res.mu)
		lm.lockTable[tag] = res
	}

	lm.mu.Unlock()

	res.mu.Lock()
	defer res.mu.Unlock()

	currentMode, held := res.holders[tid]
	// txn alrdy holds lock on resource
	if held{
		if mode == currentMode{
			return nil
		}
		// want weaker lock
		if weakerLock(currentMode, mode){
			return nil
		}

		//upgrade 

		// no waiters
		if len(res.waitQueue) == 0 && compatibleWHolders(mode, tid, res.holders) {
			res.holders[tid] = mode
			lm.setTxnLock(tid, tag, mode)
			return nil
		}

		if shouldAbortWait(tid, mode, res.holders, res.waitQueue) {
			return common.GoDBError{Code: common.DeadlockError, ErrString: "deadlock"}
		}
	
		req := LockRequest{TID: tid, Tag: tag, Mode:mode}
		res.waitQueue = append(res.waitQueue, req)

		for{
			res.cond.Wait()
			if curr_hold, isHeld := res.holders[tid]; isHeld && curr_hold == mode{
				return nil
			}
		}

	}

	// new lock req
	if len(res.waitQueue) == 0 && compatibleWHolders(mode, tid, res.holders){
		res.holders[tid] = mode
		lm.setTxnLock(tid, tag, mode)
		return nil
	}

	if shouldAbortWait(tid, mode, res.holders, res.waitQueue) {
		return common.GoDBError{Code: common.DeadlockError, ErrString: "deadlock"}
	}

	req := LockRequest{TID: tid, Tag: tag, Mode:mode}
	res.waitQueue = append(res.waitQueue, req)

	for{
		res.cond.Wait()
		if curr_hold, isHeld := res.holders[tid]; isHeld && curr_hold == mode{
			return nil
		}
	}
}

// Unlock releases the lock held by the transaction on the specified resource. If the requesting transaction does not
// hold the specified lock, it should return GoDBError(LockNotFoundError)
func (lm *LockManager) Unlock(tid common.TransactionID, tag DBLockTag) error {
	lm.mu.Lock()
	lock, ok := lm.lockTable[tag]
	lm.mu.Unlock()
	if !ok {
		return common.GoDBError{Code: common.LockNotFoundError, ErrString: "lock not found"}
	}

	lock.mu.Lock()
	defer lock.mu.Unlock()
	_, held := lock.holders[tid]
	if !held{
		return common.GoDBError{Code: common.LockNotFoundError, ErrString: "transaction does not hold lock"}
	}

	delete(lock.holders, tid)
	lm.clearTxnLock(tid, tag)

	for len(lock.waitQueue) > 0{
		req := lock.waitQueue[0]
		if !compatibleWHolders(req.Mode, req.TID, lock.holders){
			break
		}
		lock.holders[req.TID] = req.Mode
		lm.setTxnLock(req.TID, req.Tag, req.Mode)
		lock.waitQueue = lock.waitQueue[1:]
	}

	lock.cond.Broadcast()
	return nil

}

// LockHeld checks if any transaction currently holds a lock on the given resource.
func (lm *LockManager) LockHeld(tag DBLockTag) bool {
	lm.mu.Lock()
	res, exists := lm.lockTable[tag]
	lm.mu.Unlock()
	if !exists {
		return false
	}
	res.mu.Lock()
	defer res.mu.Unlock()
	return len(res.holders) > 0
}

func (lm *LockManager) setTxnLock(tid common.TransactionID, tag DBLockTag, mode DBLockMode){
	lm.mu.Lock()
	defer lm.mu.Unlock()
	m, ok := lm.txnLocks[tid]
	if !ok{
		m = make(map[DBLockTag]DBLockMode)
		lm.txnLocks[tid] = m
	}
	m[tag] = mode
}

func (lm *LockManager) clearTxnLock(tid common.TransactionID, tag DBLockTag) {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	if m, ok := lm.txnLocks[tid]; ok {
		delete(m, tag)
		if len(m) == 0 {
			delete(lm.txnLocks, tid)
		}
	}
}

func shouldAbortWait(tid common.TransactionID, reqMode DBLockMode, holders map[common.TransactionID]DBLockMode, waitQueue []LockRequest) bool {
	for holderTID, holderMode := range holders{
		if holderTID == tid{
			continue
		}
		if !compatibleLock(holderMode, reqMode) && tid > holderTID{
			return true
		}
	}
	for _, req := range waitQueue{
		if req.TID == tid{
			continue
		}
		if !compatibleLock(req.Mode, reqMode) && tid > req.TID{
			return true
		}
	}
	return false
}


func compatibleWHolders(mode DBLockMode, tid common.TransactionID, holders map[common.TransactionID]DBLockMode) bool{
	for k, v := range holders{
		if tid == k{
			continue
		}
		if !compatibleLock(mode, v){
			return false
		}
	}
	return true
}

func compatibleLock(a DBLockMode, b DBLockMode) bool{
	switch a {
    case LockModeS:
        return b == LockModeS || b == LockModeIS
    case LockModeX:
        return false
    case LockModeIS:
        return b == LockModeIS || b == LockModeIX || b == LockModeS || b == LockModeSIX
    case LockModeIX:
        return b == LockModeIS || b == LockModeIX
    case LockModeSIX:
        return b == LockModeIS
    default:
        return false
    }
}

func weakerLock(held DBLockMode, req DBLockMode) bool{
	if held == LockModeSIX{
		return req == LockModeS || req == LockModeIS || req == LockModeIX
	}else if (held == LockModeIX){
		return req == LockModeIS
	}else if (held == LockModeX){
		return true
	}else if (held == LockModeS){
		return req == LockModeIS
	}
	return false
}