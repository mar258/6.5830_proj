package storage

import (
	"mit.edu/dsg/godb/common"
	"github.com/puzpuzpuz/xsync/v4"
	"sync/atomic"
)

// BufferPool manages the reading and writing of database pages between the DiskFileManager and memory.
// It acts as a central cache to keep "hot" pages in memory with fixed capacity and selectively evicts
// pages to disk when the pool becomes full. Users will need to coordinate concurrent access to pages
// using page-level latches and metadata (which you should define in page.go). All methods
// must be thread-safe, as multiple threads will request the same or different pages concurrently.
// To get full credit, you likely need to do better than coarse-grained latching (i.e., a global latch for the entire
// BufferPool instance).
type BufferPool struct {
	// add more fields here...
	numPages       uint64
	storageManager DBFileManager
	buffer_cache   *xsync.MapOf[common.PageID, *PageFrame]
	frames []*PageFrame
	hand uint64
}

// NewBufferPool creates a new BufferPool with a fixed capacity defined by numPages. It requires a
// storageManager to handle the underlying disk I/O operations.
//
// Hint: You will need to worry about logManager until Lab 3
func NewBufferPool(numPages int, storageManager DBFileManager, logManager LogManager) *BufferPool {
	frames := make([]*PageFrame, numPages)
	for i := 0; i < numPages; i++ {
		frames[i] = &PageFrame{}
		frames[i].setRef(false)
	}
	return &BufferPool{
		numPages: uint64(numPages),
		storageManager: storageManager,
		buffer_cache:   xsync.NewMapOf[common.PageID, *PageFrame](),
		frames: frames,
	}
}

// StorageManager returns the underlying disk manager.
func (bp *BufferPool) StorageManager() DBFileManager {
	return bp.storageManager
}

// GetPage retrieves a page from the buffer pool, ensuring it is pinned (i.e. prevented from eviction until
// unpinned) and ready for use. If the page is already in the pool, the cached bytes are returned. If the page is not
// present, the method must first make space by selecting a victim frame to evict
// (potentially writing it to disk if dirty), and then read the requested page from disk into that frame.
func (bp *BufferPool) GetPage(pageID common.PageID) (*PageFrame, error) {
	var err error
	// hit
	frame, ok := bp.buffer_cache.Load(pageID)
	if ok {
		frame.lock.Lock()
		if frame.getEvicting() == false{
			frame.setPins(1)
			frame.setRef(true)
			frame.lock.Unlock()
			return frame, nil
		}
		frame.lock.Unlock()
	}

	// miss
	file, err := bp.StorageManager().GetDBFile(pageID.Oid)
	if err != nil{
		return nil, err
	}

	for{
		idx := int((atomic.AddUint64(&bp.hand, 1) - 1) % bp.numPages)
		frame = bp.frames[idx]
		frame.lock.Lock()
		id := frame.getPageID()
		if frame.getEvicting() == true{
			frame.lock.Unlock()
			continue
		}

		if frame.getRef() == true{
			frame.setRef(false)
			frame.lock.Unlock()
			continue
		}

		if frame.getPins() > 0{
			frame.lock.Unlock()
			continue
		}
		
		actual, loaded := bp.buffer_cache.LoadOrStore(pageID, frame)

		if loaded{
			frame.lock.Unlock()
			actual.lock.Lock()
			actual.setPins(1)
			actual.setRef(true)
			actual.lock.Unlock()
			return actual, nil
		}


		frame.setPins(1)
		frame.setEvicting(true)
		frame.setPageID(pageID)
		frame.setEvicting(true)

		frame.PageLatch.Lock()
		
		if frame.getDirty(){
			frame.lock.Unlock()
			file, _ := bp.StorageManager().GetDBFile(id.Oid)
			_ = file.WritePage(int(id.PageNum), frame.Bytes[:])
			frame.lock.Lock()
			frame.setDirty(false)
		}
		frame.lock.Unlock()
		
		bp.buffer_cache.Delete(id)
		err = file.ReadPage(int(pageID.PageNum), frame.Bytes[:])
		frame.PageLatch.Unlock()

		frame.lock.Lock()
		frame.setEvicting(false)
		frame.lock.Unlock()

		if err != nil{
			return nil, err
		}

		return frame, nil
	}
		

	return frame, nil
}

// UnpinPage indicates that the caller is done using a page. It unpins the page, making the page potentially evictable
// if no other thread is accessing it. If the setDirty flag is true, the page is marked as modified, ensuring
// it will be written back to disk before eviction.
func (bp *BufferPool) UnpinPage(frame *PageFrame, setDirty bool) {
	frame.lock.Lock()
	if setDirty{
		frame.setDirty(true)
	}
	frame.setPins(-1)
	frame.lock.Unlock()
}

// FlushAllPages flushes all dirty pages to disk that have an LSN less than `flushedUntil`, regardless of pins.
// This is typically called during a checkpoint or Shutdown to ensure durability, but also useful for tests
func (bp *BufferPool) FlushAllPages() error {
	var flushErr error
	bp.buffer_cache.Range(func(id common.PageID, frame *PageFrame) bool {
		frame.lock.Lock()
		if frame.getDirty() {
			frame.lock.Unlock()
			file, err := bp.StorageManager().GetDBFile(id.Oid)
			if err != nil {
				flushErr = err
				return false
			}
			frame.PageLatch.Lock()
			err = file.WritePage(int(id.PageNum), frame.Bytes[:])
			frame.PageLatch.Unlock()
			if err != nil {
				flushErr = err
				return false
			}
			frame.setDirty(false)
		}else{
			frame.lock.Unlock()
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
