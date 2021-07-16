package freemap

import (
	"encoding/binary"
	"sync"

	"github.com/cespare/xxhash"
)

type FreeMap struct {
	buckets [1024]bucket
}

const (
	chunkSize uint32 = 1 << 16
	flagMask  uint32 = 1 << 31
)

type bucket struct {
	chunks  [][]byte
	curSize uint32
	l       sync.RWMutex
	m       map[uint64]uint32
}

func hash(k []byte) uint64 {
	return xxhash.Sum64(k)
}

func (b *bucket) insert(k []byte) {
	b.l.Lock()
	defer b.l.Unlock()
	kLen := uint32(len(k))
	if kLen >= 1<<16 { // 2 bytes to store length
		return
	}
	h := hash(k)
	offset, found := b.m[h]
	if !found {
		b.m[h] = b.pushLastChunk(k)
	} else {
		for {
			chunkIdx := offset >> 16
			chunkOffset := offset & ((1 << 16) - 1)
			curChunk := b.chunks[chunkIdx]
			sLen := binary.LittleEndian.Uint16(curChunk[chunkOffset : chunkOffset+2])
			meta := binary.LittleEndian.Uint32(curChunk[chunkOffset+2 : chunkOffset+6])
			flag := (meta & flagMask) == flagMask
			if cmp(k, curChunk[chunkOffset+6:chunkOffset+6+uint32(sLen)]) {
				if !flag {
					meta |= flagMask
					binary.LittleEndian.PutUint32(curChunk[chunkOffset+2:chunkOffset+6], meta)
				}
				return
			}
			pos := meta & (flagMask - 1)
			if pos == 0 {
				pos = b.pushLastChunk(k)
				if flag {
					pos |= flagMask
				}
				binary.LittleEndian.PutUint32(curChunk[chunkOffset+2:chunkOffset+6], pos)
				return
			}
			offset = pos
		}
	}
}

func (b *bucket) pushLastChunk(k []byte) uint32 {
	kLen := uint32(len(k))
	lastChunk := b.chunks[len(b.chunks)-1]
	if chunkSize-b.curSize < kLen+6 {
		lastChunk = makeChunk()
		b.chunks = append(b.chunks, lastChunk)
		b.curSize = 0
	}
	pos := b.curSize + uint32((len(b.chunks)-1))<<16
	binary.LittleEndian.PutUint16(lastChunk[b.curSize:b.curSize+2], uint16(len(k)))
	b.curSize += 2
	binary.LittleEndian.PutUint32(lastChunk[b.curSize:b.curSize+4], 1<<31)
	b.curSize += 4
	copy(lastChunk[b.curSize:b.curSize+kLen], k)
	b.curSize += kLen
	return pos
}

func makeChunk() []byte {
	return make([]byte, chunkSize)
}

func cmp(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
