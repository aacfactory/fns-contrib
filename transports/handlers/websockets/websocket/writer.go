package websocket

import (
	"encoding/binary"
	"github.com/aacfactory/errors"
	"io"
)

type writePoolData struct{ buf []byte }

type messageWriter struct {
	c         *Conn
	compress  bool
	pos       int
	frameType int
	err       error
}

func (w *messageWriter) endMessage(err error) error {
	if w.err != nil {
		return err
	}
	c := w.c
	w.err = err
	c.writer = nil
	if c.writePool != nil {
		c.writePool.Put(writePoolData{buf: c.writeBuf})
		c.writeBuf = nil
	}
	return err
}

func (w *messageWriter) flushFrame(final bool, extra []byte) error {
	c := w.c
	if c == nil {
		return ErrNilConn
	}
	length := w.pos - maxFrameHeaderSize + len(extra)

	if isControl(w.frameType) &&
		(!final || length > maxControlFramePayloadSize) {
		return w.endMessage(errInvalidControlFrame)
	}

	b0 := byte(w.frameType)
	if final {
		b0 |= finalBit
	}
	if w.compress {
		b0 |= rsv1Bit
	}
	w.compress = false

	b1 := byte(0)
	if !c.isServer {
		b1 |= maskBit
	}

	framePos := 0
	if c.isServer {
		framePos = 4
	}

	switch {
	case length >= 65536:
		c.writeBuf[framePos] = b0
		c.writeBuf[framePos+1] = b1 | 127
		binary.BigEndian.PutUint64(c.writeBuf[framePos+2:], uint64(length))
	case length > 125:
		framePos += 6
		c.writeBuf[framePos] = b0
		c.writeBuf[framePos+1] = b1 | 126
		binary.BigEndian.PutUint16(c.writeBuf[framePos+2:], uint16(length))
	default:
		framePos += 8
		c.writeBuf[framePos] = b0
		c.writeBuf[framePos+1] = b1 | byte(length)
	}

	if !c.isServer {
		key := newMaskKey()
		copy(c.writeBuf[maxFrameHeaderSize-4:], key[:])
		maskBytes(key, 0, c.writeBuf[maxFrameHeaderSize:w.pos])
		if len(extra) > 0 {
			return w.endMessage(c.writeFatal(errors.Warning("websocket: internal error, extra used in client mode")))
		}
	}

	if c.isWriting {
		panic("concurrent write to websocket connection")
	}
	c.isWriting = true

	err := c.write(w.frameType, c.writeDeadline, c.writeBuf[framePos:w.pos], extra)

	if !c.isWriting {
		panic("concurrent write to websocket connection")
	}
	c.isWriting = false

	if err != nil {
		return w.endMessage(err)
	}

	if final {
		_ = w.endMessage(errWriteClosed)
		return nil
	}

	w.pos = maxFrameHeaderSize
	w.frameType = continuationFrame
	return nil
}

func (w *messageWriter) ncopy(max int) (int, error) {
	n := len(w.c.writeBuf) - w.pos
	if n <= 0 {
		if err := w.flushFrame(false, nil); err != nil {
			return 0, err
		}
		n = len(w.c.writeBuf) - w.pos
	}
	if n > max {
		n = max
	}
	return n, nil
}

func (w *messageWriter) Write(p []byte) (int, error) {
	if w.err != nil {
		return 0, w.err
	}

	if len(p) > 2*len(w.c.writeBuf) && w.c.isServer {
		// Don't buffer large messages.
		err := w.flushFrame(false, p)
		if err != nil {
			return 0, err
		}
		return len(p), nil
	}

	nn := len(p)
	for len(p) > 0 {
		n, err := w.ncopy(len(p))
		if err != nil {
			return 0, err
		}
		copy(w.c.writeBuf[w.pos:], p[:n])
		w.pos += n
		p = p[n:]
	}
	return nn, nil
}

func (w *messageWriter) WriteString(p string) (int, error) {
	if w.err != nil {
		return 0, w.err
	}

	nn := len(p)
	for len(p) > 0 {
		n, err := w.ncopy(len(p))
		if err != nil {
			return 0, err
		}
		copy(w.c.writeBuf[w.pos:], p[:n])
		w.pos += n
		p = p[n:]
	}
	return nn, nil
}

func (w *messageWriter) ReadFrom(r io.Reader) (nn int64, err error) {
	if w.err != nil {
		return 0, w.err
	}
	for {
		if w.pos == len(w.c.writeBuf) {
			err = w.flushFrame(false, nil)
			if err != nil {
				break
			}
		}
		var n int
		n, err = r.Read(w.c.writeBuf[w.pos:])
		w.pos += n
		nn += int64(n)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
			break
		}
	}
	return nn, err
}

func (w *messageWriter) Close() error {
	if w.err != nil {
		return w.err
	}
	return w.flushFrame(true, nil)
}
