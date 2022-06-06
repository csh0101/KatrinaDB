package index

import (
	"bytes"
	"log"
	"math"
	"sync"
	"sync/atomic"
	_ "unsafe"

	"github.com/pkg/errors"
)

const (
	MaxLevels      int = 48
	levelInscrease     = math.MaxUint32 / 3
)

type SkipList struct {
	level   int32
	headOff uint32
	arena   *Arena
	mux     *sync.Mutex
}
type Node struct {
	//val offset and value size in a uint64
	value uint64
	//keyOffset
	keyOffset uint32 //immutable
	//keySize
	keySize uint16 //immutable
	//level cur level
	level uint16
	//uint32 is the Node offset in Arena
	next [MaxLevels]uint32
}

//NewSkiplist
func NewSkiplist(arenaSize int) *SkipList {
	arena := NewArena(arenaSize)
	head := NewNode(arena, nil, ValueStruct{}, MaxLevels)
	ho := arena.getNodeOffset(head)
	return &SkipList{
		level:   1,
		headOff: ho,
		arena:   arena,
		mux:     &sync.Mutex{},
	}
}

//NewNode
//NewNode存在空间浪费的隐患
func NewNode(arena *Arena, key []byte, v ValueStruct, level int) *Node {
	/**
	1.先创建一个Node,得到Node在内存中的offset
	2.再放入一个Key,得到Key在内存中的offset
	3.取出node，然后初始化值
	*/
	nodeOffset := arena.putNodeV3(int(level))
	//offset in node.keyOffset
	keyOffset := arena.putKey(key)
	//val in node.val
	val := encodeValue(arena.putVal(v), v.EncodeSize())

	node := arena.getNodeV3(nodeOffset)

	node.keyOffset = keyOffset
	node.keySize = uint16(len(key))
	node.value = val
	node.level = uint16(level)
	return node
}

//getValueOffset
func (n *Node) getValueOffset() (uint32, uint32) {
	value := atomic.LoadUint64(&n.value)
	return decodeValue(value)
}

//setValue 当前的setValue起不到setValue的作用
func (n *Node) setValue(arena *Arena, val uint64) {
	atomic.StoreUint64(&n.value, val)
}

func (n *Node) getKey(arena *Arena) []byte {
	return arena.getKey(n.keyOffset, n.keySize)
}

//getNextOffset the index
func (n *Node) getNextOffset(level int) uint32 {
	//offset
	return atomic.LoadUint32(&n.next[level])
}

//casNextOffset
func (n *Node) casNextOffset(level int, old, val uint32) bool {
	return atomic.CompareAndSwapUint32(&n.next[level], old, val)
}

//Insert
//跳跃表的性质:高层的节点在底层一定是存在的
func (l *SkipList) Insert(e *Entry) {
	// l.mux.Lock()
	// defer l.mux.Unlock()
	//插入的流程
	//找到 每个level 需要插入位置的前后节点，更新它们的next
	key, val := e.Key, ValueStruct{
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
	}
	curLevel := l.getHeight()
	var (
		prev [MaxLevels + 1]uint32
		next [MaxLevels + 1]uint32
	)
	//curlLevel + 1 actually is head node offset
	prev[curLevel] = l.headOff

	//我的锁必须要放在这里
	// l.mux.Lock()
	// defer l.mux.Unlock()
	for i := int(curLevel) - 1; i >= 0; i-- {
		//获得应该插入位置的前一个节点和后一个节点
		prev[i], next[i] = l.findPNForLevel(key, prev[i+1], i)
		//分类讨论

		//这种情况在这种测试中不存在，因为key都是不同的
		if prev[i] == next[i] {
			//找到了一个等于这个key的情况
			valOffset := l.arena.putVal(val)
			//val encode
			encValue := encodeValue(valOffset, val.EncodeSize())
			// get the node need update
			prevNode := l.arena.getNodeV3(prev[i])
			prevNode.setValue(l.arena, encValue)
			return
		}

	}
	//如果说锁放在这里会出错的话,假设有两个协程AB，同时操作这里prev[i]和next[i]
	//corekv main分支可以把锁加在这里放在这里，我加在这里就直接gg，暂时没想到什么好的调试多线程的方式
	//gei level
	l.mux.Lock()
	defer l.mux.Unlock()
	newLevel := l.randomLevel()
	newNode := NewNode(l.arena, key, val, newLevel)
	curLevel = l.getHeight()
	//cas to update curLevel
	for newLevel > int(curLevel) {
		//update newLevel
		if atomic.CompareAndSwapInt32(&l.level, curLevel, int32(newLevel)) {
			break
		}
		curLevel = l.getHeight()
	}
	for i := 0; i < newLevel; i++ {
		//this block also have the cas feat
		for {
			//condtion: newLevel > old skiplist maxLevel
			if l.arena.getNodeV3(prev[i]) == nil {
				//0 is must have
				AssertTrue(i > 1)
				prev[i], next[i] = l.findPNForLevel(key, l.headOff, i)
				// becuase it is nil so dont contain the equal case
				AssertTrue(prev[i] != next[i])
			}
			// whatever next[i] =nil
			newNode.next[i] = next[i]
			pnode := l.arena.getNodeV3(prev[i])
			if pnode.casNextOffset(i, next[i], l.arena.getNodeOffset(newNode)) {
				break
			}
			prev[i], next[i] = l.findPNForLevel(key, prev[i], i)
			if prev[i] == next[i] {
				AssertTruef(i == 0, "Equality can happen only on base level")
				valueOffset := l.arena.putVal(val)
				encValue := encodeValue(valueOffset, val.EncodeSize())
				//获得那个刚刚被其他协程插入的结点
				prevNode := l.arena.getNodeV3(prev[i])
				prevNode.setValue(l.arena, encValue)
				// this is a key point so keep the same key for a groutine only occur once
				return
			}

		}
	}
}

//Query a Query method for skipList
func (l *SkipList) Query(key []byte) ValueStruct {
	//查询的流程
	n, _ := l.findNear(key, false, true)
	if n == nil {
		return ValueStruct{}
	}
	//getKey
	nextKey := l.arena.getKey(n.keyOffset, n.keySize)
	if !SameKey(key, nextKey) {
		return ValueStruct{}
	}
	valOffset, valSize := n.getValueOffset()
	vs := l.arena.getVal(valOffset, valSize)
	return vs
}

//SameKey
func SameKey(src, dst []byte) bool {
	if len(src) != len(dst) {
		return false
	}
	return bytes.Equal(src, dst)
}

//findPNForLevel fidn Prev and Next
func (l *SkipList) findPNForLevel(key []byte, before uint32, level int) (uint32, uint32) {
	// return --
	//get the Node in memory pool by Uint32 address
	for {
		beforeNode := l.arena.getNodeV3(before)
		next := beforeNode.getNextOffset(level)
		nextNode := l.arena.getNodeV3(next)
		//如果下一个nextNode 是空
		if nextNode == nil {
			return before, next
		}
		//下一个key
		nextKey := nextNode.getKey(l.arena)
		cmp := CompareKey_Native(key, nextKey)
		if cmp == 0 {
			//equality case
			return next, next
		}
		if cmp < 0 {
			return before, next
		}
		before = next
	}
}

//findNear less true find the node which  node.key < key
// less false find the node which node.key > key
func (l *SkipList) findNear(key []byte, less bool, allowEqual bool) (*Node, bool) {
	head := l.getHead()
	level := int(l.getHeight() - 1)
	for {
		//当前head的下一个
		next := l.getNext(head, level)

		//cur level next is nil
		if next == nil {
			//either allowEqual is set true or false  inner block
			if level > 0 {
				level--
				continue
			}
			// if level == 0
			if level == 0 {
				return nil, false
			}
			//如果不是小于
			if !less {
				return nil, false
			}
			if head == l.getHead() {
				return nil, false
			}
			return head, false

		}
		//如果next不为空就要比较
		nextKey := next.getKey(l.arena)
		cmp := CompareKey_Native(key, nextKey)
		if cmp > 0 {
			//key > next
			head = next
			continue
		}
		if cmp == 0 {
			if allowEqual {
				return next, true
			}
			if !less {
				return l.getNext(next, 0), false
			}
			if level > 0 {
				level--
				continue
			}
			//only two node and last node is equal with the key so not container the node key less than key
			if head == l.getHead() {
				return nil, false
			}
			return head, false
		}
		//cmp < 0  head.key <key < nextKey
		if level > 0 {
			level--
			continue
		}
		if !less {
			return next, false
		}
		// if less
		if head == l.getHead() {
			return nil, false
		}
		return head, false
	}
}
func (l *SkipList) Length() int {
	length := 0
	head := l.getHead()
	x := head
	for x != nil {
		next := x.next[0]
		length += 1
		nextNode := l.arena.getNodeV3(next)
		//todo to ignore for test only
		// if nextNode != nil {
		// 	off, size := nextNode.getValueOffset()
		// 	vs := l.arena.getVal(off, size)
		// 	fmt.Println(string(vs.Value))
		// 	valCnt++
		// }
		x = nextNode
	}
	return length

}
func (l *SkipList) getHead() *Node {
	//通过头结点偏移量 获得头结点
	return l.arena.getNodeV3(l.headOff)
}

//getHeight cas get the list maximum level,provide concurrent safe from multip threads process Insert or Search
func (l *SkipList) getHeight() int32 {
	return atomic.LoadInt32(&l.level)
}

func (l *SkipList) getNext(node *Node, level int) *Node {
	//获取下一个结点的offset
	offset := node.getNextOffset(level)
	//
	return l.arena.getNodeV3(offset)
}
func (l *SkipList) randomLevel() int {
	h := 1
	for h < MaxLevels && FastRand() < levelInscrease {
		h++
	}
	return h
}

func CompareKey_Native(key1, key2 []byte) int {
	return bytes.Compare(key1, key2)
}

//go:linkname FastRand runtime.fastrand
func FastRand() uint32

//AssertTruef
func AssertTruef(b bool, format string, args ...interface{}) {
	if !b {
		log.Fatalf("%+v", errors.Errorf(format, args...))
	}
}

func (l *SkipList) Add(e *Entry) {
	key, v := e.Key, ValueStruct{
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
	}

	listHeight := l.getHeight()
	var prev [MaxLevels + 1]uint32
	var next [MaxLevels + 1]uint32
	prev[listHeight] = l.headOff

	for i := int(listHeight) - 1; i >= 0; i-- {
		prev[i], next[i] = l.findPNForLevel(key, prev[i+1], i)
		if prev[i] == next[i] {
			vo := l.arena.putVal(v)
			encValue := encodeValue(vo, v.EncodeSize())
			prevNode := l.arena.getNodeV3(prev[i])
			prevNode.setValue(l.arena, encValue)
			return
		}
	}
	height := l.randomLevel()
	x := NewNode(l.arena, key, v, height)

	// Try to increase s.height via CAS.
	listHeight = l.getHeight()
	for height > int(listHeight) {
		if atomic.CompareAndSwapInt32(&l.level, listHeight, int32(height)) {
			// Successfully increased skiplist.height.
			break
		}
		listHeight = l.getHeight()
	}

	// We always insert from the base level and up. After you add a node in base level, we cannot
	// create a node in the level above because it would have discovered the node in the base level.
	for i := 0; i < height; i++ {
		for {
			//todo
			if l.arena.getNodeV3(prev[i]) == nil {
				AssertTrue(i > 1) // This cannot happen in base level.
				// We haven't computed prev, next for this level because height exceeds old listHeight.
				// For these levels, we expect the lists to be sparse, so we can just search from head.
				prev[i], next[i] = l.findPNForLevel(key, l.headOff, i)
				// Someone adds the exact same key before we are able to do so. This can only happen on
				// the base level. But we know we are not on the base level.
				AssertTrue(prev[i] != next[i])
			}
			x.next[i] = next[i]
			pnode := l.arena.getNodeV3(prev[i])
			if pnode.casNextOffset(i, next[i], l.arena.getNodeOffset(x)) {
				// Managed to insert x between prev[i] and next[i]. Go to the next level.
				break
			}
			// CAS failed. We need to recompute prev and next.
			// It is unlikely to be helpful to try to use a different level as we redo the search,
			// because it is unlikely that lots of nodes are inserted between prev[i] and next[i].
			prev[i], next[i] = l.findPNForLevel(key, prev[i], i)
			if prev[i] == next[i] {
				AssertTruef(i == 0, "Equality can happen only on base level: %d", i)
				vo := l.arena.putVal(v)
				encValue := encodeValue(vo, v.EncodeSize())
				prevNode := l.arena.getNodeV3(prev[i])
				prevNode.setValue(l.arena, encValue)
				return
			}
		}
	}
}
