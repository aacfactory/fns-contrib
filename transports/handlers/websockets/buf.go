package websockets

import "sync"

var (
	bufPool = sync.Pool{}
)

func acquireBuf() []byte {
	x := bufPool.Get()
	if x == nil {
		return make([]byte, 1024)
	}
	return x.([]byte)
}

func releaseBuf(buf []byte) {
	bufPool.Put(buf)
}
