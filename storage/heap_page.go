package storage

import (
	"mit.edu/dsg/godb/common"
)

// HeapPage Layout:
// LSN (8) | RowSize (2) | NumSlots (2) |  NumUsed (2) | Padding (2) | allocation Bitmap | deleted Bitmap | rows
type HeapPage struct {
	*PageFrame 
}

func (hp HeapPage) NumUsed() int {
	result := int(hp.PageFrame.Bytes[12]) + int(hp.PageFrame.Bytes[13]) << 8
	return result
}

func (hp HeapPage) setNumUsed(numUsed int) {
	// little endian
	hp.PageFrame.Bytes[12] = byte(numUsed); // low
	hp.PageFrame.Bytes[13] = byte(numUsed >> 8); // high

}

func (hp HeapPage) NumSlots() int {
	result := int(hp.PageFrame.Bytes[10]) + int(hp.PageFrame.Bytes[11]) << 8
	return result
}

func (hp HeapPage) RowSize() int {
	result := int(hp.PageFrame.Bytes[8]) + int(hp.PageFrame.Bytes[9]) << 8
	return result
}

func InitializeHeapPage(desc *RawTupleDesc, frame *PageFrame) {
	rowSize := desc.BytesPerTuple()
	// lsn
	for i := 0; i< 8; i++{
		frame.Bytes[i] = 0
	}
	// tuple = row
	// rowsize = bytesPerTuple
	frame.Bytes[8] = byte(rowSize)
	frame.Bytes[9] = byte(rowSize >> 8)
	// num used
	frame.Bytes[12] = 0
	frame.Bytes[13] = 0

	headerSize := 16
	numSlots := 0
	for i := 1; ; i++{
		wordsBerBitmap := (i + 63) / 64
		bytesPerBitmap := wordsBerBitmap * 8
		if headerSize + (2*bytesPerBitmap) + i*rowSize > common.PageSize{
			break
		}
		numSlots = i
	}

	// num slots
	frame.Bytes[10] = byte(numSlots)
	frame.Bytes[11] = byte(numSlots >> 8)

	wordsBerBitmap := (numSlots + 63) / 64
	bytesPerBitmap := wordsBerBitmap * 8
	// set bitmaps to 0
	for i := 0; i < 2*bytesPerBitmap; i++{
		frame.Bytes[headerSize + i] = 0
	}
}

func (frame *PageFrame) AsHeapPage() HeapPage {
	return HeapPage{PageFrame: frame}
}

func (hp HeapPage) FindFreeSlot() int {
	wordsBerBitmap := (hp.NumSlots() + 63) / 64
	bytesPerBitmap := wordsBerBitmap * 8
	allocationBitmap := AsBitmap(hp.Bytes[16: 16 + bytesPerBitmap], hp.NumSlots())
	return allocationBitmap.FindFirstZero(0)	
}

// IsAllocated checks the allocation bitmap to see if a slot is valid.
func (hp HeapPage) IsAllocated(rid common.RecordID) bool {
	wordsBerBitmap := (hp.NumSlots() + 63) / 64
	bytesPerBitmap := wordsBerBitmap * 8
	allocationBitmap := AsBitmap(hp.Bytes[16: 16 + bytesPerBitmap], hp.NumSlots())
	return allocationBitmap.LoadBit(int(rid.Slot))
}

func (hp HeapPage) MarkAllocated(rid common.RecordID, allocated bool) {
	wordsBerBitmap := (hp.NumSlots() + 63) / 64
	bytesPerBitmap := wordsBerBitmap * 8
	allocationBitmap := AsBitmap(hp.Bytes[16: 16 + bytesPerBitmap], hp.NumSlots())
	prev := allocationBitmap.SetBit(int(rid.Slot), allocated)
	if allocated && !prev{
		hp.setNumUsed(hp.NumUsed() + 1)
	} else if !allocated && prev {
		hp.setNumUsed(hp.NumUsed() - 1)
        hp.MarkDeleted(rid, false)
	}
}

func (hp HeapPage) IsDeleted(rid common.RecordID) bool {
	wordsBerBitmap := (hp.NumSlots() + 63) / 64
	bytesPerBitmap := wordsBerBitmap * 8
	deletedIndex := 16 + bytesPerBitmap
	deletedBitmap := AsBitmap(hp.Bytes[deletedIndex: deletedIndex + bytesPerBitmap], hp.NumSlots())
	return deletedBitmap.LoadBit(int(rid.Slot))
}

func (hp HeapPage) MarkDeleted(rid common.RecordID, deleted bool) {
	wordsBerBitmap := (hp.NumSlots() + 63) / 64
	bytesPerBitmap := wordsBerBitmap * 8
	deletedIndex := 16 + bytesPerBitmap
	deletedBitmap := AsBitmap(hp.Bytes[deletedIndex: deletedIndex + bytesPerBitmap], hp.NumSlots())
	deletedBitmap.SetBit(int(rid.Slot), deleted)
}

func (hp HeapPage) AccessTuple(rid common.RecordID) RawTuple {
	wordsBerBitmap := (hp.NumSlots() + 63) / 64
	bytesPerBitmap := wordsBerBitmap * 8
	tuplesIndex := 16 + (bytesPerBitmap * 2)
	slotIndex := int(rid.Slot) * hp.RowSize()
	return hp.Bytes[tuplesIndex + slotIndex: tuplesIndex + slotIndex + hp.RowSize()]
}
