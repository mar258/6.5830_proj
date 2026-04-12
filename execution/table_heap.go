package execution

import (
	"errors"

	"mit.edu/dsg/godb/catalog"
	"mit.edu/dsg/godb/common"
	"mit.edu/dsg/godb/storage"
	"mit.edu/dsg/godb/transaction"
)

// TableHeap represents a physical table stored as a heap file on disk.
// It handles the insertion, update, deletion, and reading of tuples, managing
// interactions with the BufferPool, LockManager, and LogManager.
type TableHeap struct {
	oid         common.ObjectID
	desc        *storage.RawTupleDesc
	bufferPool  *storage.BufferPool
	logManager  storage.LogManager
	lockManager *transaction.LockManager
}

// NewTableHeap creates a TableHeap and performs a metadata scan to initialize stats.
func NewTableHeap(table *catalog.Table, bufferPool *storage.BufferPool, logManager storage.LogManager, lockManager *transaction.LockManager) (*TableHeap, error) {
	// Build a physical tuple descriptor from the catalog table's column types.
	fieldTypes := make([]common.Type, len(table.Columns))
	for i, col := range table.Columns {
		fieldTypes[i] = col.Type
	}

	return &TableHeap{
		oid:         table.Oid,
		desc:        storage.NewRawTupleDesc(fieldTypes),
		bufferPool:  bufferPool,
		logManager:  logManager,
		lockManager: lockManager,
	}, nil
}

// StorageSchema returns the physical byte-layout descriptor of the tuples in this table.
func (tableHeap *TableHeap) StorageSchema() *storage.RawTupleDesc {
	return tableHeap.desc
}

// InsertTuple inserts a tuple into the TableHeap. It should find a free space, allocating if needed, and return the found slot.
func (tableHeap *TableHeap) InsertTuple(txn *transaction.TransactionContext, row storage.RawTuple) (common.RecordID, error) {
	tableTag := transaction.NewTableLockTag(tableHeap.oid)
	if txn != nil{
		err := txn.AcquireLock(tableTag, transaction.LockModeIX)
		if err != nil{
			return common.RecordID{}, err
		}
	}

	storageManager := tableHeap.bufferPool.StorageManager()
	file, err := storageManager.GetDBFile(tableHeap.oid)
	if err != nil {
		return common.RecordID{}, err
	}
	numPages, err := file.NumPages()
	if err != nil {
		return common.RecordID{}, err
	}

	// Find space on existing heap pages 
	for numPage := 0; numPage < numPages; numPage++ {	
		pageID := common.PageID{Oid: tableHeap.oid, PageNum: int32(numPage)}	
		frame, err := tableHeap.bufferPool.GetPage(pageID)
		if err != nil {
			continue
		}

		frame.PageLatch.Lock()
		heapPage := frame.AsHeapPage()
		freeSlot := heapPage.FindFreeSlot()
		if freeSlot != -1 {
			rid := common.RecordID{PageID: pageID, Slot: int32(freeSlot)}		
			tupleTag := transaction.NewTupleLockTag(rid)

			if txn != nil{
				err = txn.AcquireLock(tupleTag, transaction.LockModeX)
				if err != nil{
					return common.RecordID{}, err
				}	
				rec := txn.NewInsertRecord(rid, row)
				lsn, err := tableHeap.logManager.Append(rec)
				if err != nil {
					return common.RecordID{}, err
				}
				frame.MonotonicallyUpdateLSN(lsn)	
			}
			tuple := heapPage.AccessTuple(rid)
			copy(tuple, row)
			heapPage.MarkAllocated(rid, true)
			frame.PageLatch.Unlock()
			tableHeap.bufferPool.UnpinPage(frame, true)
			return rid, nil
		}
		frame.PageLatch.Unlock()
		tableHeap.bufferPool.UnpinPage(frame, false)
	}

	// No space found on existing pages: allocate a new heap page.
	numPage, err := file.AllocatePage(1)
	if err != nil {
		return common.RecordID{}, err
	}
	pageID := common.PageID{Oid: tableHeap.oid, PageNum: int32(numPage)}
	frame, err := tableHeap.bufferPool.GetPage(pageID)
	if err != nil {
		return common.RecordID{}, err
	}

	frame.PageLatch.Lock()
	storage.InitializeHeapPage(tableHeap.desc, frame)
	heapPage := frame.AsHeapPage()
	freeSlot := heapPage.FindFreeSlot()
	if freeSlot == -1 {
		frame.PageLatch.Unlock()
		tableHeap.bufferPool.UnpinPage(frame, false)
		return common.RecordID{}, errors.New("no free slot on newly allocated page")
	}

	rid := common.RecordID{PageID: pageID, Slot: int32(freeSlot)}
	tupleTag := transaction.NewTupleLockTag(rid)

	if txn != nil{
		err = txn.AcquireLock(tupleTag, transaction.LockModeX)
		if err != nil{
			return common.RecordID{}, err
		}	
		rec := txn.NewInsertRecord(rid, row)
		lsn, err := tableHeap.logManager.Append(rec)
		if err != nil {
			return common.RecordID{}, err
		}
		frame.MonotonicallyUpdateLSN(lsn)	
	}

	tuple := heapPage.AccessTuple(rid)
	copy(tuple, row)
	heapPage.MarkAllocated(rid, true)
	frame.PageLatch.Unlock()

	tableHeap.bufferPool.UnpinPage(frame, true)

	return rid, nil
}

var ErrTupleDeleted = errors.New("tuple has been deleted")

// DeleteTuple marks a tuple as deleted in the TableHeap. If the tuple has been deleted, return ErrTupleDeleted
func (tableHeap *TableHeap) DeleteTuple(txn *transaction.TransactionContext, rid common.RecordID) error {
	tableTag := transaction.NewTableLockTag(tableHeap.oid)
	tupleTag := transaction.NewTupleLockTag(rid)

	if txn != nil{
		err := txn.AcquireLock(tupleTag, transaction.LockModeX)
		if err != nil{
			return err
		}
		err = txn.AcquireLock(tableTag, transaction.LockModeIX)
		if err != nil{
			return err
		}
	}

	frame, err := tableHeap.bufferPool.GetPage(rid.PageID)
	if err != nil{
		return err
	}
	defer tableHeap.bufferPool.UnpinPage(frame, true)

	frame.PageLatch.Lock()
	defer frame.PageLatch.Unlock()

	page := frame.AsHeapPage()

	if !page.IsAllocated(rid) {
		return ErrTupleDeleted
	}

	if page.IsDeleted(rid) {
		return ErrTupleDeleted
	}

	if txn != nil{
		rec := txn.NewDeleteRecord(rid)
		lsn, err := tableHeap.logManager.Append(rec)
		if err != nil {
			return err
		}
		frame.MonotonicallyUpdateLSN(lsn)
	}
	page.MarkDeleted(rid, true)
	return nil
}

// ReadTuple reads the physical bytes of a tuple into the provided buffer. If forUpdate is true, read should acquire
// exclusive lock instead of shared. If the tuple has been deleted, return ErrTupleDeleted
func (tableHeap *TableHeap) ReadTuple(txn *transaction.TransactionContext, rid common.RecordID, buffer []byte, forUpdate bool) error {
	tupleTag := transaction.NewTupleLockTag(rid)
	tableTag := transaction.NewTableLockTag(tableHeap.oid)
	if forUpdate{
		err := txn.AcquireLock(tupleTag, transaction.LockModeX)
		if err != nil{
			return  err
		}
		err = txn.AcquireLock(tableTag, transaction.LockModeIX)
		if err != nil{
			return  err
		}

	
	}else{
		err := txn.AcquireLock(tupleTag, transaction.LockModeS)
		if err != nil{
			return err
		}
		err = txn.AcquireLock(tableTag, transaction.LockModeIS)
		if err != nil{
			return  err
		}

	}

	frame, err := tableHeap.bufferPool.GetPage(rid.PageID)
	if err != nil {
		return err
	}
	defer tableHeap.bufferPool.UnpinPage(frame, false)

	if forUpdate {
		frame.PageLatch.Lock()
		defer frame.PageLatch.Unlock()
	} else {
		frame.PageLatch.RLock()
		defer frame.PageLatch.RUnlock()
	}

	hp := frame.AsHeapPage()
	if !hp.IsAllocated(rid) {
		return ErrTupleDeleted
	}

	if hp.IsDeleted(rid) {
		return ErrTupleDeleted
	}

	copy(buffer[:hp.RowSize()], hp.AccessTuple(rid))
	return nil
}

// UpdateTuple updates a tuple in-place with new binary data. If the tuple has been deleted, return ErrTupleDeleted.
func (tableHeap *TableHeap) UpdateTuple(txn *transaction.TransactionContext, rid common.RecordID, updatedTuple storage.RawTuple) error {
	tupleTag := transaction.NewTupleLockTag(rid)
	tableTag := transaction.NewTableLockTag(tableHeap.oid)
	err := txn.AcquireLock(tupleTag, transaction.LockModeX)
	if err != nil{
		return err
	}
	err = txn.AcquireLock(tableTag, transaction.LockModeIX)
	if err != nil{
		return err
	}


	frame, err := tableHeap.bufferPool.GetPage(rid.PageID)
	if err != nil {
		return err
	}

	defer tableHeap.bufferPool.UnpinPage(frame,true)

	frame.PageLatch.Lock()
	defer frame.PageLatch.Unlock()

	hp := frame.AsHeapPage()
	if !hp.IsAllocated(rid) {
		return ErrTupleDeleted
	}

	if hp.IsDeleted(rid) {
		return ErrTupleDeleted
	}
	dest := hp.AccessTuple(rid)

	if txn != nil{
		rec := txn.NewUpdateRecord(rid, dest, updatedTuple)
		lsn, err := tableHeap.logManager.Append(rec)
		if err != nil {
			return err
		}
		frame.MonotonicallyUpdateLSN(lsn)

	}

	copy(dest, updatedTuple)

	return nil
}

// VacuumPage attempts to clean up deleted slots on a specific page.
// If slots are deleted AND no transaction holds a lock on them, they are marked as free.
// This is used to reclaim space in the background.
func (tableHeap *TableHeap) VacuumPage(pageID common.PageID) error {
	frame, err := tableHeap.bufferPool.GetPage(pageID)
	if err != nil {
		return err
	}

	frame.PageLatch.Lock()
	defer frame.PageLatch.Unlock()

	hp := frame.AsHeapPage()
	numSlots := hp.NumSlots()
	for i:= 0; i<numSlots; i++{
		rid := common.RecordID{PageID: pageID, Slot: int32(i)}

		if hp.IsAllocated(rid) && hp.IsDeleted(rid){
			tupleTag := transaction.NewTupleLockTag(rid)
			held :=  tableHeap.lockManager.LockHeld(tupleTag)
			if held{
				continue
			}
			hp.MarkAllocated(rid, false)
		}

	}
	tableHeap.bufferPool.UnpinPage(frame, true)
	return nil
}

// Iterator creates a new TableHeapIterator to scan the table. It acquires the supplied lock on the table (S, X, or SIX),
// and uses the supplied byte slice to fetch tuples in the returned iterator (for zero-allocation scanning).
func (tableHeap *TableHeap) Iterator(txn *transaction.TransactionContext, mode transaction.DBLockMode, buffer []byte) (TableHeapIterator, error) {
	if txn != nil{
		tableTag := transaction.NewTableLockTag(tableHeap.oid)
		err := txn.AcquireLock(tableTag, mode)
		if err != nil{
			return TableHeapIterator{}, err
		}	
	}
	dbFile, err := tableHeap.bufferPool.StorageManager().GetDBFile(tableHeap.oid)
	if err != nil {
		return TableHeapIterator{}, err
	}
	numPages, err := dbFile.NumPages()
	if err != nil {
		return TableHeapIterator{}, err
	}

	return TableHeapIterator{
		tableHeap: tableHeap,
		buffer: buffer[:tableHeap.desc.BytesPerTuple()],
		numPages: numPages,
		txn: txn,
	}, nil
}

// TableHeapIterator iterates over all valid (allocated and non-deleted) tuples in the heap.
type TableHeapIterator struct {
	tableHeap *TableHeap
	buffer    []byte
	numPages int
	txn *transaction.TransactionContext

	currPageNum int
	currSlot int
	currFrame *storage.PageFrame
	currPID common.PageID
	currRID common.RecordID
	err error 
}

// IsNil returns true if the TableHeapIterator is the default, uninitialized value
func (it *TableHeapIterator) IsNil() bool {
	return it == nil || it.tableHeap == nil
}

// Next advances the iterator to the next valid tuple.
// It manages page pins automatically (unpinning the old page when moving to a new one).
func (it *TableHeapIterator) Next() bool {
	if it.IsNil() {
		return false
	}

	if it.err != nil {
		return false
	}

	bp := it.tableHeap.bufferPool

	for {
		// fetch and pin the next heap page when we run out of slots
		// on the current page (or at the very beginning of iteration).
		if it.currFrame == nil {
			if it.currPageNum >= it.numPages {
				return false
			}

			pid := common.PageID{
				Oid:     it.tableHeap.oid,
				PageNum: int32(it.currPageNum),
			}
			frame, err := bp.GetPage(pid)
			if err != nil {
				it.err = err
				return false
			}

			it.currFrame = frame
			it.currPID = pid
			it.currSlot = -1
		}

		// Scan slots on the current page
		it.currFrame.PageLatch.RLock()
		hp := it.currFrame.AsHeapPage()
		numSlots := hp.NumSlots()

		for {
			it.currSlot++
			if it.currSlot >= numSlots {
				// Finished this page: release latch and unpin before advancing to the next page
				it.currFrame.PageLatch.RUnlock()
				bp.UnpinPage(it.currFrame, false)
				it.currFrame = nil
				it.currPageNum++
				break
			}

			rid := common.RecordID{
				PageID: it.currPID,
				Slot:   int32(it.currSlot),
			}

			if hp.IsAllocated(rid) && !hp.IsDeleted(rid) {
				copy(it.buffer, hp.AccessTuple(rid))
				it.currRID = rid
				it.currFrame.PageLatch.RUnlock()
				return true
			}
		}
	}
}

// CurrentTuple returns the raw bytes of the tuple at the current cursor position.
// The bytes are valid only until Next() is called again.
func (it *TableHeapIterator) CurrentTuple() storage.RawTuple {
	if it.IsNil(){
		return nil
	}

	return storage.RawTuple(it.buffer)
}

// CurrentRID returns the RecordID of the current tuple.
func (it *TableHeapIterator) CurrentRID() common.RecordID {
	return it.currRID
}

// CurrentRID returns the first error encountered during iteration, if any.
func (it *TableHeapIterator) Error() error {
	return it.err
}

// Close releases any resources associated with the TableHeapIterator
func (it *TableHeapIterator) Close() error {
	if it.currFrame != nil && it.tableHeap != nil && it.tableHeap.bufferPool != nil {
		it.tableHeap.bufferPool.UnpinPage(it.currFrame, false)
		it.currFrame = nil
	}
	it.tableHeap = nil
	it.txn = nil
	it.buffer = nil
	return nil
}
