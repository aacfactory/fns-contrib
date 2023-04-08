package http3

import (
	"bufio"
	"io"
	"sync"
)

var (
	bufPool = sync.Pool{New: func() any {
		return make([]byte, 4096)
	}}
	bufioReaderPool sync.Pool
	bufioWriterPool sync.Pool
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

func newBufioReader(r io.Reader) *bufio.Reader {
	if v := bufioReaderPool.Get(); v != nil {
		br := v.(*bufio.Reader)
		br.Reset(r)
		return br
	}
	return bufio.NewReader(r)
}

func putBufioReader(br *bufio.Reader) {
	br.Reset(nil)
	bufioReaderPool.Put(br)
}

func newBufioWriter(w io.Writer) *bufio.Writer {
	if v := bufioWriterPool.Get(); v != nil {
		bw := v.(*bufio.Writer)
		bw.Reset(w)
		return bw
	}
	return bufio.NewWriter(w)
}

func putBufioWriter(bw *bufio.Writer) {
	bw.Reset(nil)
	bufioWriterPool.Put(bw)
}
