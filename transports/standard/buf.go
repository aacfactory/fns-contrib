package standard

import (
	"sync"
)

var (
	bufPool = sync.Pool{New: func() any {
		return make([]byte, 4096)
	}}
)

func acquireBuf() []byte {
	x := bufPool.Get()
	if x == nil {
		return make([]byte, 4096)
	}
	return x.([]byte)
}

func releaseBuf(buf []byte) {
	bufPool.Put(buf)
}
