package storage

import (
	"unsafe"

	"mit.edu/dsg/godb/common"
)

// Bitmap provides a convenient interface for manipulating bits in a byte slice.
// It does not own the underlying bytes; instead, it provides a structured view over
// an existing buffer (e.g., a database page).
//
// The implementation should be optimized for performance by performing word-level (uint64)
// operations during scans to skip full blocks of set bits.
type Bitmap struct {
	words   []uint64
	numBits int
}

// AsBitmap creates a Bitmap view over the provided byte slice.
//
// Constraints:
// 1. data must be aligned to 8 bytes to allow safe casting to uint64.
// 2. data must be large enough to contain numBits (rounded up to the nearest 8-byte word).
func AsBitmap(data []byte, numBits int) Bitmap {
	numWords := (numBits + 63) / 64

	// Edge case: an empty bitmap (no bits to track) should not attempt to index data[0].
	if numWords == 0 {
		return Bitmap{
			words:   nil,
			numBits: numBits,
		}
	}

	common.Assert(common.AlignedTo8(len(data)), "Bitmap bytes length must be aligned to 8")
	common.Assert(len(data) >= numWords*8, "bitmap buffer too small")

	ptr := unsafe.Pointer(&data[0])
	// Slice reference cast to uint64
	words := unsafe.Slice((*uint64)(ptr), numWords)

	return Bitmap{
		words:   words,
		numBits: numBits,
	}
}

// SetBit sets the bit at index i to the given value.
// Returns the previous value of the bit.
func (b *Bitmap) SetBit(i int, on bool) (originalValue bool) {
	wordIndex := i / 64
	bitIndex := uint64(i % 64)
	mask := uint64(1) << bitIndex
	orig := b.words[wordIndex] & mask != 0

	if on {
		b.words[wordIndex] |= mask
	} else {
		b.words[wordIndex] &^= mask
	}
	
	return orig
}

// LoadBit returns the value of the bit at index i.
func (b *Bitmap) LoadBit(i int) bool {
	wordIndex := i / 64
	bitIndex := uint64(i % 64)

	orig := b.words[wordIndex] & ((uint64(1)) << bitIndex) != 0;
	return orig;
}

// FindFirstZero searches for the first bit set to 0 (false) in the bitmap.
// It begins the search at startHint and scans to the end of the bitmap.
// If no zero bit is found, it wraps around and scans from the beginning (index 0)
// up to startHint.
//
// Returns the index of the first zero bit found, or -1 if the bitmap is entirely full.
func (b *Bitmap) FindFirstZero(startHint int) int {
	wordIndex := startHint / 64
	bitIndex := startHint % 64

	idx := -1
	found := false

	for i := wordIndex; i < len(b.words); i++ {
		startBit := 0
		if i == wordIndex {
			startBit = bitIndex
		}

		endBit := 64
		if i == len(b.words)-1 {
			remaining := b.numBits - i*64
			if remaining < endBit {
				endBit = remaining
			}
		}

		for j := startBit; j < endBit; j++ {
			mask := uint64(1) << uint(j)
			if (b.words[i] & mask) == 0 { 
				idx = (i * 64) + j
				found = true
				break
			}
		}
		if found {
			break
		}
	}

	if !found {
		for i := 0; i <= wordIndex && i < len(b.words); i++ {
			endBit := 64
			if i == wordIndex {
				endBit = bitIndex
			}

			if i == len(b.words)-1 {
				remaining := b.numBits - i*64
				if remaining < endBit {
					endBit = remaining
				}
			}

			for j := 0; j < endBit; j++ {
				mask := uint64(1) << uint(j)
				if (b.words[i] & mask) == 0 { 
					idx = (i * 64) + j
					found = true
					break
				}
			}
			if found {
				break
			}
		}
	}

	return idx
}
