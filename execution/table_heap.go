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
	return &TableHeap{
		oid: table.Oid,
		desc: table.Desc,
		bufferPool: bufferPool,
		logManager: logManager,
		lockManager: lockManager,
	}, nil
}

// StorageSchema returns the physical byte-layout descriptor of the tuples in this table.
func (tableHeap *TableHeap) StorageSchema() *storage.RawTupleDesc {
	return tableHeap.desc
}

// InsertTuple inserts a tuple into the TableHeap. It should find a free space, allocating if needed, and return the found slot.
func (tableHeap *TableHeap) InsertTuple(txn *transaction.TransactionContext, row storage.RawTuple) (common.RecordID, error) {
	storageManager := tableHeap.bufferPool.StorageManager()
	file, err := storageManager.GetDBFile(tableHeap.Oid)
	if err != nil{
		return common.RecordID{}, err
	}
	numPages, err := storageManager.NumPages()
	if err != nil{
		return common.RecordID{}, err
	}

	for numPage := 0; numPage < numPages; numPage++{
		pageId = common.PageID{Oid: tableHeap.Oid, PageNum: numPage}
		frame, err := tableHeap.GetPage(pageId)
		if err != nil{
			continue
		}
		heapPage = frame.AsHeapPage()
		freeSlot := heapPage.FindFreeSlot()
		if freeSlot > 0{
			rid:= common.RecordId{PageID; pageId, Slot:freeSlot}
			frame.PageLatch.Lock()
			tuple := heapPage.AccessTuple(rid)
			copy(tuple, row)
			heapPage.MarkAllocated(rid, true)
			frame.PageLatch.Unlock()
			tableHeap.bufferPool.Unpin(frame, true)
			return rid, nil
		}
		tableHeap.bufferPool.Unpin(frame, false)
	}

	// allocate 
	numPage, err := storageManager.AllocatePage(1)
	if err != nil {
		return common.RecordID{}, err
	}
	pageId = common.PageID{Oid: tableHeap.Oid, PageNum: numPage}
	frame, err := tableHeap.GetPage(pageId)
	if err != nil {
		return common.RecordID{}, err
	}
	frame.PageLatch.Lock()
	frame.InitializeHeapPage(tableHeap.desc, frame)
	heapPage := frame.AsHeapPage()
	freeSlot := heapPage.FindFreeSlot()
	if freeSlot > 0{
		return common.RecordID{}, err
	}
	rid:= common.RecordId{PageID; pageId, Slot:freeSlot}
	tuple := heapPage.AccessTuple(rid)
	copy(tuple, row)
	heapPage.MarkAllocated(rid, true)
	frame.PageLatch.Unlock()

	tableHeap.bufferPool.Unpin(frame, true)

	return rid, nil

}

var ErrTupleDeleted = errors.New("tuple has been deleted")

// DeleteTuple marks a tuple as deleted in the TableHeap. If the tuple has been deleted, return ErrTupleDeleted
func (tableHeap *TableHeap) DeleteTuple(txn *transaction.TransactionContext, rid common.RecordID) error {
	panic("unimplemented")
}

// ReadTuple reads the physical bytes of a tuple into the provided buffer. If forUpdate is true, read should acquire
// exclusive lock instead of shared. If the tuple has been deleted, return ErrTupleDeleted
func (tableHeap *TableHeap) ReadTuple(txn *transaction.TransactionContext, rid common.RecordID, buffer []byte, forUpdate bool) error {
	panic("unimplemented")
}

// UpdateTuple updates a tuple in-place with new binary data. If the tuple has been deleted, return ErrTupleDeleted.
func (tableHeap *TableHeap) UpdateTuple(txn *transaction.TransactionContext, rid common.RecordID, updatedTuple storage.RawTuple) error {
	panic("unimplemented")
}

// VacuumPage attempts to clean up deleted slots on a specific page.
// If slots are deleted AND no transaction holds a lock on them, they are marked as free.
// This is used to reclaim space in the background.
func (tableHeap *TableHeap) VacuumPage(pageID common.PageID) error {
	panic("unimplemented")
}

// Iterator creates a new TableHeapIterator to scan the table. It acquires the supplied lock on the table (S, X, or SIX),
// and uses the supplied byte slice to fetch tuples in the returned iterator (for zero-allocation scanning).
func (tableHeap *TableHeap) Iterator(txn *transaction.TransactionContext, mode transaction.DBLockMode, buffer []byte) (TableHeapIterator, error) {
	panic("unimplemented")
}

// TableHeapIterator iterates over all valid (allocated and non-deleted) tuples in the heap.
type TableHeapIterator struct {
	// Fill me in!
}

// IsNil returns true if the TableHeapIterator is the default, uninitialized value
func (it *TableHeapIterator) IsNil() bool {
	panic("unimplemented")
}

// Next advances the iterator to the next valid tuple.
// It manages page pins automatically (unpinning the old page when moving to a new one).
func (it *TableHeapIterator) Next() bool {
	panic("unimplemented")
}

// CurrentTuple returns the raw bytes of the tuple at the current cursor position.
// The bytes are valid only until Next() is called again.
func (it *TableHeapIterator) CurrentTuple() storage.RawTuple {
	panic("unimplemented")
}

// CurrentRID returns the RecordID of the current tuple.
func (it *TableHeapIterator) CurrentRID() common.RecordID {
	panic("unimplemented")
}

// CurrentRID returns the first error encountered during iteration, if any.
func (it *TableHeapIterator) Error() error {
	panic("unimplemented")
}

// Close releases any resources associated with the TableHeapIterator
func (it *TableHeapIterator) Close() error {
	panic("unimplemented")
}
