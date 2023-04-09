package websocket

import (
	"github.com/aacfactory/errors"
	"io"
)

type messageReader struct{ c *Conn }

func (r *messageReader) Read(b []byte) (int, error) {
	c := r.c
	if c == nil {
		return 0, ErrNilConn
	}
	if c.messageReader != r {
		return 0, io.EOF
	}

	for c.readErr == nil {

		if c.readRemaining > 0 {
			if int64(len(b)) > c.readRemaining {
				b = b[:c.readRemaining]
			}
			n, err := c.br.Read(b)
			c.readErr = hideTempErr(err)
			if c.isServer {
				c.readMaskPos = maskBytes(c.readMaskKey, c.readMaskPos, b[:n])
			}
			rem := c.readRemaining
			rem -= int64(n)
			_ = c.setReadRemaining(rem)
			if c.readRemaining > 0 && c.readErr == io.EOF {
				c.readErr = errUnexpectedEOF
			}
			return n, c.readErr
		}

		if c.readFinal {
			c.messageReader = nil
			return 0, io.EOF
		}

		frameType, err := c.advanceFrame()
		switch {
		case err != nil:
			c.readErr = hideTempErr(err)
		case frameType == TextMessage || frameType == BinaryMessage:
			c.readErr = errors.Warning("websocket: internal error, unexpected text or binary in Reader")
		}
	}

	err := c.readErr
	if err == io.EOF && c.messageReader == r {
		err = errUnexpectedEOF
	}
	return 0, err
}

func (r *messageReader) Close() error {
	return nil
}
