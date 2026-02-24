package storage

import (
	"mit.edu/dsg/godb/common"
	"github.com/puzpuzpuz/xsync/v4"
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
	numPages       int
	storageManager DBFileManager
	buffer_cache   *xsync.MapOf[common.PageID, *PageFrame]
}

// NewBufferPool creates a new BufferPool with a fixed capacity defined by numPages. It requires a
// storageManager to handle the underlying disk I/O operations.
//
// Hint: You will need to worry about logManager until Lab 3
func NewBufferPool(numPages int, storageManager DBFileManager, logManager LogManager) *BufferPool {
	return &BufferPool{
		numPages: numPages,
		storageManager: storageManager,
		buffer_cache:   xsync.NewMapOf[common.PageID, *PageFrame](),
	}
	// for i := 0; i < numPages; i++ {
    //     frame := &PageFrame{}
	// 	frame.setPins(false)
	// 	frame.setRef(false)
	// 	frame.setDirty(false)
    //     pool.buffer_cache.Store(common.PageID{Oid: 0, PageNum: int32(i)}, frame) 
    // }
	// return pool
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
	var resultFrame *PageFrame
	var err error
	for{
		// hit
		frame, ok := bp.buffer_cache.Load(pageID)
		if ok {
			frame.setPins(true)
			frame.setRef(true)
			return frame, nil
			
		}

		// miss

		// space in cache
		if bp.buffer_cache.Size() < bp.numPages {
			newFrame := &PageFrame{}
			
			file, err := bp.StorageManager().GetDBFile(pageID.Oid)
			if err != nil {
				return nil, err
			}
			err = file.ReadPage(int(pageID.PageNum), newFrame.Bytes[:])
			if err != nil {
				return nil, err
			}
			
			newFrame.setPins(true)
			newFrame.setRef(false)
			bp.buffer_cache.Store(pageID, newFrame)
			return newFrame, nil
		}

		// evict 
		for i:=0; i<2; i++ {
			bp.buffer_cache.RangeRelaxed(func(id common.PageID, frame *PageFrame) bool {
				
				if frame.getRef() == true{
					frame.setRef(false)
					return true
				}

				if frame.getPins() > 0{
					return true
				}
				if !frame.PageLatch.TryLock() {
					return true
				}
				frame.setPins(true)

				if frame.getDirty(){
					file, _ := bp.StorageManager().GetDBFile(id.Oid)
					_ = file.WritePage(int(id.PageNum), frame.Bytes[:])
					frame.setDirty(false)
				}

				bp.buffer_cache.Delete(id)
				file, err := bp.StorageManager().GetDBFile(pageID.Oid)
				if err != nil{
					frame.PageLatch.Unlock()
					return false
				}
				err = file.ReadPage(int(pageID.PageNum), frame.Bytes[:])
				if err != nil{
					frame.PageLatch.Unlock()
					return false
				}
				bp.buffer_cache.LoadOrStore(pageID, frame)
				frame.PageLatch.Unlock()
				frame.setRef(true)
				resultFrame = frame
				return false
		
			})
			if resultFrame != nil{
				return resultFrame, nil
			}
		}
	}

	if resultFrame == nil{
		return nil, err
	}

	return resultFrame, nil
}

// UnpinPage indicates that the caller is done using a page. It unpins the page, making the page potentially evictable
// if no other thread is accessing it. If the setDirty flag is true, the page is marked as modified, ensuring
// it will be written back to disk before eviction.
func (bp *BufferPool) UnpinPage(frame *PageFrame, setDirty bool) {
	if setDirty{
		frame.setDirty(true)
	}
	frame.setPins(false)
}

// FlushAllPages flushes all dirty pages to disk that have an LSN less than `flushedUntil`, regardless of pins.
// This is typically called during a checkpoint or Shutdown to ensure durability, but also useful for tests
func (bp *BufferPool) FlushAllPages() error {
	var flushErr error
	bp.buffer_cache.Range(func(id common.PageID, frame *PageFrame) bool {
		if frame.getDirty() {
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
