package storage

import (
	"mit.edu/dsg/godb/common"
	"github.com/puzpuzpuz/xsync/v3"
	"sync"
)

// BufferPool manages the reading and writing of database pages between the DiskFileManager and memory.
// It acts as a central cache to keep "hot" pages in memory with fixed capacity and selectively evicts
// pages to disk when the pool becomes full. Users will need to coordinate concurrent access to pages
// using page-level latches and metadata (which you should define in page.go). All methods
// must be thread-safe, as multiple threads will request the same or different pages concurrently.
// To get full credit, you likely need to do better than coarse-grained latching (i.e., a global latch for the entire
// BufferPool instance).
type Slot struct{
	pageId common.PageID
	ref bool
	frame *PageFrame
	lock           sync.RWMutex
}

type BufferPool struct {
	// add more fields here...
	lock           sync.RWMutex
	storageManager DBFileManager
	buffer_cache   *xsync.MapOf[common.PageID, *PageFrame]
	frames         []Slot
	hand           int
}

// NewBufferPool creates a new BufferPool with a fixed capacity defined by numPages. It requires a
// storageManager to handle the underlying disk I/O operations.
//
// Hint: You will need to worry about logManager until Lab 3
func NewBufferPool(numPages int, storageManager DBFileManager, logManager LogManager) *BufferPool {
	frames := make([]Slot, numPages)
	for i:= range frames {
		frames[i].frame = &PageFrame{}
		frames[i].ref = false
	}
	return &BufferPool{
		storageManager: storageManager,
		buffer_cache:   xsync.NewMapOf[common.PageID, *PageFrame](),
		frames:         frames,
		hand:           0,
		lock:           sync.RWMutex{},
	}
}

// StorageManager returns the underlying disk manager.
func (bp *BufferPool) StorageManager() DBFileManager {
	return bp.storageManager
}

// clock eviction
func (bp *BufferPool) evict() int{
	var freedIdx int
	for i:= 0; i < len(bp.frames) * 2; i++{
		idx := bp.hand%len(bp.frames)
		slot := &bp.frames[idx]
		id, ref, frame := slot.pageId, slot.ref, slot.frame
		if ref == false{
			if frame.getPins() > 0{
				bp.hand = (bp.hand + 1) % len(bp.frames)
				continue
			}

			if frame.getDirty(){
				file, _ := bp.StorageManager().GetDBFile(id.Oid)
				_ = file.WritePage(int(id.PageNum), frame.Bytes[:])
			}

			bp.buffer_cache.Delete(id)
			slot.ref = false
			freedIdx = idx
			bp.hand = (bp.hand + 1) % len(bp.frames)
			break
		}else{
			slot.ref = false
			bp.hand = (bp.hand + 1) % len(bp.frames)
		}
	}
	return freedIdx
}

// GetPage retrieves a page from the buffer pool, ensuring it is pinned (i.e. prevented from eviction until
// unpinned) and ready for use. If the page is already in the pool, the cached bytes are returned. If the page is not
// present, the method must first make space by selecting a victim frame to evict
// (potentially writing it to disk if dirty), and then read the requested page from disk into that frame.
func (bp *BufferPool) GetPage(pageID common.PageID) (*PageFrame, error) {
	var frame *PageFrame
	bp.lock.Lock()
	defer bp.lock.Unlock()
	frame, ok := bp.buffer_cache.Load(pageID)
	if ok {
		for i := 0; i < len(bp.frames); i++ {
			if bp.frames[i].pageId == pageID{
				bp.frames[i].ref = true
				break
			}
		}
	}else{
		freeSlot := bp.evict()
		slot := &bp.frames[freeSlot]
		frame = slot.frame
		frame.PageLatch.Lock()
		defer frame.PageLatch.Unlock()
		frame.setDirty(false)
		file, err := bp.StorageManager().GetDBFile(pageID.Oid)
		if err != nil{
			return nil, err
		}
		err = file.ReadPage(int(pageID.PageNum), frame.Bytes[:])
		if err != nil{
			return nil, err
		}

		slot.pageId = pageID
		slot.ref = true
		bp.buffer_cache.Store(pageID, frame)
	}

	frame.setPins(true)
	return frame, nil
}

// UnpinPage indicates that the caller is done using a page. It unpins the page, making the page potentially evictable
// if no other thread is accessing it. If the setDirty flag is true, the page is marked as modified, ensuring
// it will be written back to disk before eviction.
func (bp *BufferPool) UnpinPage(frame *PageFrame, setDirty bool) {
	frame.setPins(false)
	if setDirty{
		frame.setDirty(true)
	}
}

// FlushAllPages flushes all dirty pages to disk that have an LSN less than `flushedUntil`, regardless of pins.
// This is typically called during a checkpoint or Shutdown to ensure durability, but also useful for tests
func (bp *BufferPool) FlushAllPages() error {
	var flushErr error
	bp.buffer_cache.Range(func(id common.PageID, frame *PageFrame) bool {
		frame.PageLatch.Lock()
		defer frame.PageLatch.Unlock()
		if frame.getDirty() {
			file, err := bp.StorageManager().GetDBFile(id.Oid)
			if err != nil {
				flushErr = err
				return false
			}
			err = file.WritePage(int(id.PageNum), frame.Bytes[:])
			if err != nil {
				flushErr = err
				return false
			}
			frame.setDirty(false)
		}
		return true
	})
	return flushErr
}

// GetDirtyPageTableSnapshot returns a map of all currently dirty pages and their RecoveryLSN.
// This is called during checkpoint to snapshot the current DPT into the log.
//
// Hint: You do not need to worry about this function until lab 4
func (bp *BufferPool) GetDirtyPageTableSnapshot() map[common.PageID]LSN {
	// You will not need to implement this until lab4
	panic("unimplemented")
}
