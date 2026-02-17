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
	result := 0
	for i:= 12; i < 14; i++{
		result = result << 8;
		result += int(hp.PageFrame.Bytes[i])
	}
	return result
}

func (hp HeapPage) setNumUsed(numUsed int) {
	hp.PageFrame.Bytes[12] = byte(numUsed >> 0);
	hp.PageFrame.Bytes[13] = byte(numUsed >> 8);

}

func (hp HeapPage) NumSlots() int {
	result := 0
	for i:= 10; i < 12; i++{
		result = result << 8;
		result += int(hp.PageFrame.Bytes[i])
	}
	return result
}

func (hp HeapPage) RowSize() int {
	result := 0
	for i:= 8; i < 10; i++{
		result = result << 8;
		result += int(hp.PageFrame.Bytes[i])
	}
	return result
}

func InitializeHeapPage(desc *RawTupleDesc, frame *PageFrame) {
	panic("unimplemented")
	newPage := HeapPage{
		*PageFrame: frame
	}
}

func (frame *PageFrame) AsHeapPage() HeapPage {
	panic("unimplemented")
}

func (hp HeapPage) FindFreeSlot() int {
	panic("unimplemented")
}

// IsAllocated checks the allocation bitmap to see if a slot is valid.
func (hp HeapPage) IsAllocated(rid common.RecordID) bool {
	panic("unimplemented")
}

func (hp HeapPage) MarkAllocated(rid common.RecordID, allocated bool) {
	panic("unimplemented")
}

func (hp HeapPage) IsDeleted(rid common.RecordID) bool {
	panic("unimplemented")
}

func (hp HeapPage) MarkDeleted(rid common.RecordID, deleted bool) {
	panic("unimplemented")
}

func (hp HeapPage) AccessTuple(rid common.RecordID) RawTuple {
	panic("unimplemented")
}
