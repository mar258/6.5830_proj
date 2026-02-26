package storage

import (
	"math/rand"
	"mit.edu/dsg/godb/common"
	"github.com/puzpuzpuz/xsync/v4"
	"sync"
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
	numPages       int
	storageManager DBFileManager
	buffer_cache   *xsync.MapOf[common.PageID, *PageFrame]
	frames         []*PageFrame
	hand           uint64
	pageLocks      *xsync.MapOf[common.PageID, *sync.Mutex]
}

// NewBufferPool creates a new BufferPool with a fixed capacity defined by numPages. It requires a
// storageManager to handle the underlying disk I/O operations.
//
// Hint: You will need to worry about logManager until Lab 3
func NewBufferPool(numPages int, storageManager DBFileManager, logManager LogManager) *BufferPool {
	frames := make([]*PageFrame, numPages)
	for i := 0; i < numPages; i++ {
		frames[i] = &PageFrame{}
	}
	return &BufferPool{
		numPages:       numPages,
		storageManager: storageManager,
		buffer_cache:   xsync.NewMapOf[common.PageID, *PageFrame](),
		frames:         frames,
		pageLocks:      xsync.NewMapOf[common.PageID, *sync.Mutex](),
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
    // hit
    frame, ok := bp.buffer_cache.Load(pageID)
    if ok {
        frame.lock.Lock()
        if frame.getEvicting() == false {
            frame.setPins(1)
            frame.setRef(true)
            frame.lock.Unlock()
            return frame, nil
        }
        frame.lock.Unlock()
    }

    // miss
    for i := 0; i < 100; i++ {
        idx := int((atomic.AddUint64(&bp.hand, 1) - 1)) % bp.numPages
        frame = bp.frames[idx]

        frame.lock.Lock()

        if frame.getPins() > 0 {
            frame.lock.Unlock()
            continue
        }

        if frame.getRef() == true {
            frame.setRef(false)
            frame.lock.Unlock()
            continue
        }

        if frame.getEvicting() == true {
            frame.lock.Unlock()
            continue
        }
        frame.setEvicting(true)
        
        id := frame.getPageID()
        
        // ORDERING FIX: Delete from cache BEFORE dropping the lock
        
        
        if frame.getDirty() {
            // ORDERING FIX: Drop frame.lock BEFORE acquiring PageLatch
            frame.lock.Unlock() 
            
            file, _ := bp.StorageManager().GetDBFile(id.Oid)
            frame.PageLatch.Lock()
            err := file.WritePage(int(id.PageNum), frame.Bytes[:])
            frame.PageLatch.Unlock()
            
            if err != nil {
                // ORDERING FIX: Clean up state before returning error
                frame.lock.Lock()
                frame.setEvicting(false)
                frame.lock.Unlock()
                return nil, err
            }
            
            // ORDERING FIX: Re-acquire frame.lock to update metadata
            frame.lock.Lock() 
            frame.setDirty(false) 
        }
        bp.buffer_cache.Delete(id)
        // ORDERING FIX: Drop frame.lock entirely before LoadOrStore and ReadPage
        frame.lock.Unlock() 
        
        actual, loaded := bp.buffer_cache.LoadOrStore(pageID, frame)

        if loaded {
            frame.lock.Lock()
            frame.setEvicting(false)
            frame.lock.Unlock()
            
            actual.lock.Lock()
            actual.setPins(1)
            actual.setRef(true)
            actual.lock.Unlock()
            return actual, nil
        }

        currFile, err := bp.StorageManager().GetDBFile(pageID.Oid)
        if err != nil {
            frame.lock.Lock()
            frame.setEvicting(false)
            frame.lock.Unlock()
            return nil, err
        }
        
        frame.PageLatch.Lock()
        err = currFile.ReadPage(int(pageID.PageNum), frame.Bytes[:])
        frame.PageLatch.Unlock()
        
        if err != nil {
            frame.lock.Lock()
            frame.setEvicting(false)
            frame.lock.Unlock()
            return nil, err
        }
        
        frame.lock.Lock()
        frame.setPins(1)
        frame.setPageID(pageID)
        frame.setEvicting(false)
        frame.lock.Unlock()
        
        return frame, nil
    }
        
    // fallback: evict a random page
    frame = bp.frames[rand.Intn(bp.numPages)]
    
    frame.lock.Lock()
    frame.setEvicting(true)
        
    id := frame.getPageID()
    
    if frame.getDirty() {
        // ORDERING FIX: Drop frame.lock BEFORE acquiring PageLatch
        frame.lock.Unlock()
        
        file, _ := bp.StorageManager().GetDBFile(id.Oid)
        frame.PageLatch.Lock()
        err := file.WritePage(int(id.PageNum), frame.Bytes[:])
        frame.PageLatch.Unlock()
        
        if err != nil {
            frame.lock.Lock()
            frame.setEvicting(false)
            frame.lock.Unlock()
            return nil, err
        }
        
        frame.lock.Lock()
        frame.setDirty(false)
    }
	bp.buffer_cache.Delete(id)
    
    frame.lock.Unlock()
    
    actual, loaded := bp.buffer_cache.LoadOrStore(pageID, frame)

    if loaded {
        frame.lock.Lock()
        frame.setEvicting(false)
        frame.lock.Unlock()
        
        actual.lock.Lock()
        actual.setPins(1)
        actual.setRef(true)
        actual.lock.Unlock()
        return actual, nil
    }

    currFile, err := bp.StorageManager().GetDBFile(pageID.Oid)
    if err != nil {
        frame.lock.Lock()
        frame.setEvicting(false)
        frame.lock.Unlock()
        return nil, err
    }
    
    frame.PageLatch.Lock()
    err = currFile.ReadPage(int(pageID.PageNum), frame.Bytes[:])
    frame.PageLatch.Unlock()
    
    if err != nil {
        frame.lock.Lock()
        frame.setEvicting(false)
        frame.lock.Unlock()
        return nil, err
    }

    frame.lock.Lock()
    frame.setPins(1)
    frame.setPageID(pageID)
    frame.setEvicting(false)
    frame.lock.Unlock()

    return frame, nil
}

// UnpinPage indicates that the caller is done using a page. It unpins the page, making the page potentially evictable
// if no other thread is accessing it. If the setDirty flag is true, the page is marked as modified, ensuring
// it will be written back to disk before eviction.
func (bp *BufferPool) UnpinPage(frame *PageFrame, setDirty bool) {
	// lock, ok := bp.pageLocks.Load(pageID)
	// fmt.Printf("Lock line 191\n")
	frame.lock.Lock()
	if setDirty{
		frame.setDirty(true)
	}
	frame.setPins(-1)
	// fmt.Printf("Unlock line 204\n")
	frame.lock.Unlock()
}

// FlushAllPages flushes all dirty pages to disk that have an LSN less than `flushedUntil`, regardless of pins.
// This is typically called during a checkpoint or Shutdown to ensure durability, but also useful for tests
func (bp *BufferPool) FlushAllPages() error {
	var flushErr error
	bp.buffer_cache.Range(func(id common.PageID, frame *PageFrame) bool {
		// fmt.Printf("Lock line 204\n")
		frame.lock.Lock()
		if frame.getDirty(){
			// fmt.Printf("Unlock line 215\n")
			frame.lock.Unlock()
			file, err := bp.StorageManager().GetDBFile(id.Oid)
			if err != nil {
				flushErr = err
				return false
			}
			// fmt.Printf("Lock line 213\n")
			frame.PageLatch.Lock()
			err = file.WritePage(int(id.PageNum), frame.Bytes[:])
			// fmt.Printf("Unlock line 224\n")
			frame.PageLatch.Unlock()
			if err != nil {
				flushErr = err
				return false
			}
			// fmt.Printf("Lock line 220\n")
			frame.lock.Lock()
			frame.setDirty(false)
		}
		// fmt.Printf("Unlock line 233\n")
		frame.lock.Unlock()
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
