package websocket

import (
	"errors"
	"io"
	"strings"
	"sync"

	"github.com/klauspost/compress/flate"
)

const (
	minCompressionLevel     = -2
	maxCompressionLevel     = flate.BestCompression
	defaultCompressionLevel = 1
)

var (
	flateWriterPools [maxCompressionLevel - minCompressionLevel + 1]sync.Pool
	flateReaderPool  = sync.Pool{New: func() interface{} {
		return flate.NewReader(nil)
	}}
)

func decompressNoContextTakeover(r io.Reader) io.ReadCloser {
	const tail = "\x00\x00\xff\xff" + "\x01\x00\x00\xff\xff"
	fr, _ := flateReaderPool.Get().(io.ReadCloser)
	fr.(flate.Resetter).Reset(io.MultiReader(r, strings.NewReader(tail)), nil)
	return &flateReadWrapper{fr}
}

func isValidCompressionLevel(level int) bool {
	return minCompressionLevel <= level && level <= maxCompressionLevel
}

func compressNoContextTakeover(w io.WriteCloser, level int) io.WriteCloser {
	p := &flateWriterPools[level-minCompressionLevel]
	tw := &truncWriter{w: w}
	fw, _ := p.Get().(*flate.Writer)
	if fw == nil {
		fw, _ = flate.NewWriter(tw, level)
	} else {
		fw.Reset(tw)
	}
	return &flateWriteWrapper{fw: fw, tw: tw, p: p}
}

type truncWriter struct {
	w io.WriteCloser
	n int
	p [4]byte
}

func (w *truncWriter) Write(p []byte) (int, error) {
	n := 0

	if w.n < len(w.p) {
		n = copy(w.p[w.n:], p)
		p = p[n:]
		w.n += n
		if len(p) == 0 {
			return n, nil
		}
	}

	m := len(p)
	if m > len(w.p) {
		m = len(w.p)
	}

	if nn, err := w.w.Write(w.p[:m]); err != nil {
		return n + nn, err
	}

	copy(w.p[:], w.p[m:])
	copy(w.p[len(w.p)-m:], p[len(p)-m:])
	nn, err := w.w.Write(p[:len(p)-m])
	return n + nn, err
}

type flateWriteWrapper struct {
	fw *flate.Writer
	tw *truncWriter
	p  *sync.Pool
}

func (w *flateWriteWrapper) Write(p []byte) (int, error) {
	if w.fw == nil {
		return 0, errWriteClosed
	}
	return w.fw.Write(p)
}

func (w *flateWriteWrapper) Close() error {
	if w.fw == nil {
		return errWriteClosed
	}
	err1 := w.fw.Flush()
	w.p.Put(w.fw)
	w.fw = nil
	if w.tw.p != [4]byte{0, 0, 0xff, 0xff} {
		return errors.New("websocket: internal error, unexpected bytes at end of flate stream")
	}
	err2 := w.tw.w.Close()
	if err1 != nil {
		return err1
	}
	return err2
}

type flateReadWrapper struct {
	fr io.ReadCloser
}

func (r *flateReadWrapper) Read(p []byte) (int, error) {
	if r.fr == nil {
		return 0, io.ErrClosedPipe
	}
	n, err := r.fr.Read(p)
	if err == io.EOF {
		_ = r.Close()
	}
	return n, err
}

func (r *flateReadWrapper) Close() error {
	if r.fr == nil {
		return io.ErrClosedPipe
	}
	err := r.fr.Close()
	flateReaderPool.Put(r.fr)
	r.fr = nil
	return err
}
