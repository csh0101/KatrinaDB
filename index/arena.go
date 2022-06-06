package index

import (
	"log"
	"sync/atomic"
	"unsafe"

	"github.com/pkg/errors"
)

const (
	// DefaultNodeSize is the Size of SikipList
	DefaultNodeSize = int(unsafe.Sizeof(Node{}))
	// AlignSize
	AlignSize = int(unsafe.Sizeof(uint64(0))) - 1
)

//Arena a pre-allocate buffer
type Arena struct {
	used uint32
	buf  []byte
}

func NewArena(n int) *Arena {
	return &Arena{
		used: 1,
		buf:  make([]byte, n),
	}
}

//allocate size
func (a *Arena) alloc(size uint32) uint32 {
	used := atomic.AddUint32(&a.used, size)

	cap := len(a.buf) - int(used)

	if cap < DefaultNodeSize {
		grow := uint32(len(a.buf))

		if grow > 1<<30 {
			grow = 1 << 30
		}
		if grow < size {
			grow = size
		}
		temp := make([]byte, len(a.buf)+int(grow))
		if !(len(a.buf) == copy(temp, a.buf)) {
			log.Fatal("alloc failed")
		}
		a.buf = temp
	}
	//return the offset of after-put data
	return used - size
}

func (a *Arena) putNodeV3(height int) uint32 {
	nodeSize := uint32(unsafe.Sizeof(Node{}))
	offsetSize := uint32(unsafe.Sizeof(uint32(0)))
	unusedSize := (MaxLevels - height) * int(offsetSize)
	//nodeSize - uint32(unusedSize)
	l := uint32(nodeSize - uint32(unusedSize) + uint32(AlignSize))
	//n
	n := a.alloc(l)
	//补齐内存对齐 64 -- 8个字节
	//8 16 24 32
	m := (n + uint32(AlignSize)) & ^(uint32(AlignSize))
	return m
}

func (a *Arena) getNodeV3(offset uint32) *Node {
	if offset == 0 {
		return nil
	}
	return (*Node)(unsafe.Pointer(&a.buf[offset]))
}

func (a *Arena) getNodeOffset(node *Node) uint32 {
	if node == nil {
		return 0
	}
	return uint32(uintptr(unsafe.Pointer(node)) - uintptr(unsafe.Pointer(&a.buf[0])))
}

//putKey put key datastruct into memory pool
func (a *Arena) putKey(key []byte) uint32 {
	sz := uint32(len(key))
	offset := a.alloc(sz)
	buf := a.buf[offset : offset+sz]
	if !(len(key) == copy(buf, key)) {
		log.Fatal("copy failed")
	}
	return offset
}

func (a *Arena) getKey(off uint32, sz uint16) []byte {
	return a.buf[off : off+uint32(sz)]
}

func (a *Arena) putVal(v ValueStruct) uint32 {
	sz := v.EncodeSize()
	offset := a.alloc(sz)
	buf := a.buf[offset : offset+sz]
	if !(v.EncodeValue(buf) == sz) {
		log.Fatal("unexcepted length")
	}
	return offset
}

func (a *Arena) getVal(offset uint32, size uint32) (ret ValueStruct) {
	ret.DecodeValue(a.buf[offset : offset+size])
	return
}

func encodeValue(valOffset uint32, valSize uint32) uint64 {
	return uint64(valOffset) | uint64(valSize)<<32
}

func decodeValue(value uint64) (valOffset uint32, valSize uint32) {
	valOffset = uint32(value)
	valSize = uint32(value >> 32)
	return
}
func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
