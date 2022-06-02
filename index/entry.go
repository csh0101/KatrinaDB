package index

import "encoding/binary"

type ValueStruct struct {
	Value     []byte
	ExpiresAt uint64
}

//EncodeSize is to Arena allocate
func (e *ValueStruct) EncodeSize() uint32 {
	sz := len(e.Value)
	enc := sizeVarint(e.ExpiresAt)
	return uint32(sz + enc)
}

//EncodeValue is to EncodeValues
func (e *ValueStruct) EncodeValue(buf []byte) uint32 {
	off := binary.PutUvarint(buf[:], e.ExpiresAt)
	n := copy(buf[off:], e.Value)
	return uint32(off + n)
}

//DecodeValue is DecodeValue
func (e *ValueStruct) DecodeValue(buf []byte) {
	var sz int
	e.ExpiresAt, sz = binary.Uvarint(buf)
	e.Value = buf[sz:]
}

func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return
}

//Entry K,V Pair
type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64
}

func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}
