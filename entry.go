package extsort

import "github.com/graxinc/bytepool"

var entryPool = bytepool.NewDynamic()

type entry struct {
	data   *bytepool.Bytes
	keyLen int
}

func fetchEntry() *entry {
	return &entry{entryPool.Get(), 0}
}

func (e entry) Key() []byte {
	return e.data.B[:e.keyLen]
}

func (e entry) Val() []byte {
	return e.data.B[e.keyLen:]
}

func (e entry) ValLen() int {
	return len(e.data.B) - e.keyLen
}

func (e *entry) Release() {
	entryPool.Put(e.data)
}
