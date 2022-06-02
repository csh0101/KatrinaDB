package index_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/csh0101/katrinadb/index"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func RandString(len int) string {
	bytes := make([]byte, len)

	for i := 0; i < len; i++ {
		b := index.R.Intn(26) + 65
		bytes[i] = byte(b)
	}
	return string(bytes)
}

func TestSkipListInsertAndSearchBase(t *testing.T) {
	l := index.NewSkiplist(1000)

	entry1 := index.NewEntry([]byte(RandString(10)), []byte("Val1"))

	//skiplist add entry1
	l.Insert(entry1)

	vs := l.Query(entry1.Key)

	//excepted equal
	assert.Equal(t, entry1.Value, vs.Value)

	//key point this step we should
	key2 := RandString(10)
	entry2 := index.NewEntry([]byte(key2), []byte("Val2"))

	l.Insert(entry2)

	vs2 := l.Query(entry2.Key)

	//excepted equal
	assert.Equal(t, vs2.Value, []byte("Val2"))
	assert.Equal(t, entry2.Value, vs2.Value)

	//Update a entry

	entry3 := index.NewEntry([]byte(key2), []byte("Val3"))

	l.Insert(entry3)

	vs3 := l.Query(entry3.Key)

	assert.Equal(t, entry3.Key, []byte(key2))
	assert.Equal(t, vs3.Value, []byte("Val3"))
}

func TestConcurrentInsertAndQuery(t *testing.T) {
	const n = 10000
	l := index.NewSkiplist(1)

	var wg = &sync.WaitGroup{}
	_ = wg
	generateKey := func(i int) []byte {
		return []byte(fmt.Sprintf("Keykeykey%05d", i))
	}
	wg.Add(n)
	for i := 0; i < n; i++ {
		i := i
		go func(i int) {
			defer wg.Done()
			l.Insert(index.NewEntry(generateKey(i), generateKey(i)))
		}(i)
		// l.Insert(index.NewEntry(generateKey(i), generateKey(i)))
	}
	wg.Wait()
	require.EqualValues(t, 10001, l.Length())
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int, wg *sync.WaitGroup) {
			defer wg.Done()
			vs := l.Query(generateKey(i))
			require.EqualValues(t, generateKey(i), vs.Value)
			return
		}(i, wg)
	}
	wg.Wait()
}
