package websocket

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/uid"
	"github.com/aacfactory/json"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

func newConn(conn net.Conn, isServer bool, readBufferSize, writeBufferSize int, writeBufferPool BufferPool, br *bufio.Reader, writeBuf []byte) *Conn {
	if br == nil {
		if readBufferSize == 0 {
			readBufferSize = defaultReadBufferSize
		} else if readBufferSize < maxControlFramePayloadSize {
			readBufferSize = maxControlFramePayloadSize
		}
		br = bufio.NewReaderSize(conn, readBufferSize)
	}

	if writeBufferSize <= 0 {
		writeBufferSize = defaultWriteBufferSize
	}
	writeBufferSize += maxFrameHeaderSize

	if writeBuf == nil && writeBufferPool == nil {
		writeBuf = make([]byte, writeBufferSize)
	}

	mu := make(chan struct{}, 1)
	mu <- struct{}{}
	c := &Conn{
		id:                     uid.Bytes(),
		isServer:               isServer,
		br:                     br,
		conn:                   conn,
		mu:                     mu,
		readFinal:              true,
		writeBuf:               writeBuf,
		writePool:              writeBufferPool,
		writeBufSize:           writeBufferSize,
		enableWriteCompression: true,
		compressionLevel:       defaultCompressionLevel,
	}
	c.SetCloseHandler(nil)
	c.SetPingHandler(nil)
	c.SetPongHandler(nil)
	return c
}

type Conn struct {
	id                     []byte
	conn                   net.Conn
	isServer               bool
	subprotocol            string
	mu                     chan struct{}
	writeBuf               []byte
	writePool              BufferPool
	writeBufSize           int
	writeDeadline          time.Time
	writer                 io.WriteCloser
	isWriting              bool
	writeErrMu             sync.Mutex
	writeErr               error
	enableWriteCompression bool
	compressionLevel       int
	newCompressionWriter   func(io.WriteCloser, int) io.WriteCloser
	reader                 io.ReadCloser
	readErr                error
	br                     *bufio.Reader
	readRemaining          int64
	readFinal              bool
	readLength             int64
	readLimit              int64
	readMaskPos            int
	readMaskKey            [4]byte
	handlePong             func([]byte) error
	handlePing             func([]byte) error
	handleClose            func(int, string) error
	readErrCount           int
	messageReader          *messageReader
	readDecompress         bool
	newDecompressionReader func(io.Reader) io.ReadCloser
}

func (c *Conn) Id() []byte {
	return c.id
}

func (c *Conn) setReadRemaining(n int64) error {
	if n < 0 {
		return ErrReadLimit
	}
	c.readRemaining = n
	return nil
}

func (c *Conn) Subprotocol() string {
	if c == nil {
		return ""
	}
	return c.subprotocol
}

func (c *Conn) Close() error {
	if c == nil {
		return ErrNilConn
	}
	if c.conn == nil {
		return ErrNilNetConn
	}
	return c.conn.Close()
}

func (c *Conn) LocalAddr() net.Addr {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.LocalAddr()
}

func (c *Conn) RemoteAddr() net.Addr {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.RemoteAddr()
}

func (c *Conn) writeFatal(err error) error {
	if c == nil {
		return ErrNilConn
	}
	err = hideTempErr(err)
	c.writeErrMu.Lock()
	if c.writeErr == nil {
		c.writeErr = err
	}
	c.writeErrMu.Unlock()
	return err
}

func (c *Conn) read(n int) ([]byte, error) {
	if c == nil {
		return nil, ErrNilConn
	}
	p, err := c.br.Peek(n)
	if err == io.EOF {
		err = errUnexpectedEOF
	}
	_, _ = c.br.Discard(len(p))
	return p, err
}

func (c *Conn) write(frameType int, deadline time.Time, buf0, buf1 []byte) error {
	if c == nil {
		return ErrNilConn
	}
	<-c.mu
	defer func() { c.mu <- struct{}{} }()

	c.writeErrMu.Lock()
	err := c.writeErr
	c.writeErrMu.Unlock()
	if err != nil {
		return err
	}
	if c.conn == nil {
		return ErrNilNetConn
	}
	_ = c.conn.SetWriteDeadline(deadline)
	if len(buf1) == 0 {
		_, err = c.conn.Write(buf0)
	} else {
		err = c.writeBufs(buf0, buf1)
	}
	if err != nil {
		return c.writeFatal(err)
	}
	if frameType == CloseMessage {
		_ = c.writeFatal(ErrCloseSent)
	}
	return nil
}

func (c *Conn) writeBufs(bufs ...[]byte) error {
	b := net.Buffers(bufs)
	_, err := b.WriteTo(c.conn)
	return err
}

func (c *Conn) WriteControl(messageType int, data []byte, deadline time.Time) error {
	if c == nil {
		return ErrNilConn
	}
	if !isControl(messageType) {
		return errBadWriteOpCode
	}
	if len(data) > maxControlFramePayloadSize {
		return errInvalidControlFrame
	}

	b0 := byte(messageType) | finalBit
	b1 := byte(len(data))
	if !c.isServer {
		b1 |= maskBit
	}

	buf := make([]byte, 0, maxFrameHeaderSize+maxControlFramePayloadSize)
	buf = append(buf, b0, b1)

	if c.isServer {
		buf = append(buf, data...)
	} else {
		key := newMaskKey()
		buf = append(buf, key[:]...)
		buf = append(buf, data...)
		maskBytes(key, 0, buf[6:])
	}

	d := 1000 * time.Hour
	if !deadline.IsZero() {
		d = deadline.Sub(time.Now())
		if d < 0 {
			return errWriteTimeout
		}
	}

	timer := time.NewTimer(d)
	select {
	case <-c.mu:
		timer.Stop()
	case <-timer.C:
		return errWriteTimeout
	}
	defer func() { c.mu <- struct{}{} }()

	c.writeErrMu.Lock()
	err := c.writeErr
	c.writeErrMu.Unlock()
	if err != nil {
		return err
	}
	if c.conn == nil {
		return ErrNilNetConn
	}
	_ = c.conn.SetWriteDeadline(deadline)
	_, err = c.conn.Write(buf)
	if err != nil {
		return c.writeFatal(err)
	}
	if messageType == CloseMessage {
		_ = c.writeFatal(ErrCloseSent)
	}
	return err
}

func (c *Conn) beginMessage(mw *messageWriter, messageType int) error {
	if c == nil {
		return ErrNilConn
	}
	if c.writer != nil {
		_ = c.writer.Close()
		c.writer = nil
	}

	if !isControl(messageType) && !isData(messageType) {
		return errBadWriteOpCode
	}

	c.writeErrMu.Lock()
	err := c.writeErr
	c.writeErrMu.Unlock()
	if err != nil {
		return err
	}

	mw.c = c
	mw.frameType = messageType
	mw.pos = maxFrameHeaderSize

	if c.writeBuf == nil {
		wpd, ok := c.writePool.Get().(writePoolData)
		if ok {
			c.writeBuf = wpd.buf
		} else {
			c.writeBuf = make([]byte, c.writeBufSize)
		}
	}
	return nil
}

func (c *Conn) NextWriter(messageType int) (io.WriteCloser, error) {
	if c == nil {
		return nil, ErrNilConn
	}
	var mw messageWriter
	if err := c.beginMessage(&mw, messageType); err != nil {
		return nil, err
	}
	c.writer = &mw
	if c.newCompressionWriter != nil && c.enableWriteCompression && isData(messageType) {
		w := c.newCompressionWriter(c.writer, c.compressionLevel)
		mw.compress = true
		c.writer = w
	}
	return c.writer, nil
}

func (c *Conn) WritePreparedMessage(pm *PreparedMessage) error {
	if c == nil {
		return ErrNilConn
	}
	frameType, frameData, err := pm.frame(prepareKey{
		isServer:         c.isServer,
		compress:         c.newCompressionWriter != nil && c.enableWriteCompression && isData(pm.messageType),
		compressionLevel: c.compressionLevel,
	})
	if err != nil {
		return err
	}
	if c.isWriting {
		return fmt.Errorf("concurrent write to websocket connection")
	}
	c.isWriting = true
	err = c.write(frameType, c.writeDeadline, frameData, nil)
	if !c.isWriting {
		return fmt.Errorf("concurrent write to websocket connection")
	}
	c.isWriting = false
	return err
}

func (c *Conn) WriteMessage(messageType int, data []byte) error {
	if c == nil {
		return ErrNilConn
	}
	if c.isServer && (c.newCompressionWriter == nil || !c.enableWriteCompression) {
		var mw messageWriter
		if err := c.beginMessage(&mw, messageType); err != nil {
			return err
		}
		n := copy(c.writeBuf[mw.pos:], data)
		mw.pos += n
		data = data[n:]
		return mw.flushFrame(true, data)
	}

	w, err := c.NextWriter(messageType)
	if err != nil {
		return err
	}
	if _, err = w.Write(data); err != nil {
		return err
	}
	return w.Close()
}

func (c *Conn) SetWriteDeadline(t time.Time) error {
	if c == nil {
		return ErrNilConn
	}
	c.writeDeadline = t
	return nil
}

func (c *Conn) advanceFrame() (int, error) {
	if c.readRemaining > 0 {
		if _, err := io.CopyN(io.Discard, c.br, c.readRemaining); err != nil {
			return noFrame, err
		}
	}

	var errs []string

	p, err := c.read(2)
	if err != nil {
		return noFrame, err
	}

	frameType := int(p[0] & 0xf)
	final := p[0]&finalBit != 0
	rsv1 := p[0]&rsv1Bit != 0
	rsv2 := p[0]&rsv2Bit != 0
	rsv3 := p[0]&rsv3Bit != 0
	mask := p[1]&maskBit != 0
	_ = c.setReadRemaining(int64(p[1] & 0x7f))

	c.readDecompress = false
	if rsv1 {
		if c.newDecompressionReader != nil {
			c.readDecompress = true
		} else {
			errs = append(errs, "RSV1 set")
		}
	}

	if rsv2 {
		errs = append(errs, "RSV2 set")
	}

	if rsv3 {
		errs = append(errs, "RSV3 set")
	}

	switch frameType {
	case CloseMessage, PingMessage, PongMessage:
		if c.readRemaining > maxControlFramePayloadSize {
			errs = append(errs, "len > 125 for control")
		}
		if !final {
			errs = append(errs, "FIN not set on control")
		}
	case TextMessage, BinaryMessage:
		if !c.readFinal {
			errs = append(errs, "data before FIN")
		}
		c.readFinal = final
	case continuationFrame:
		if c.readFinal {
			errs = append(errs, "continuation after FIN")
		}
		c.readFinal = final
	default:
		errs = append(errs, "bad opcode "+strconv.Itoa(frameType))
	}

	if mask != c.isServer {
		errs = append(errs, "bad MASK")
	}

	if len(errs) > 0 {
		return noFrame, c.handleProtocolError(strings.Join(errs, ", "))
	}

	switch c.readRemaining {
	case 126:
		p, err := c.read(2)
		if err != nil {
			return noFrame, err
		}

		if err := c.setReadRemaining(int64(binary.BigEndian.Uint16(p))); err != nil {
			return noFrame, err
		}
	case 127:
		p, err := c.read(8)
		if err != nil {
			return noFrame, err
		}

		if err := c.setReadRemaining(int64(binary.BigEndian.Uint64(p))); err != nil {
			return noFrame, err
		}
	}

	if mask {
		c.readMaskPos = 0
		p, err := c.read(len(c.readMaskKey))
		if err != nil {
			return noFrame, err
		}
		copy(c.readMaskKey[:], p)
	}

	if frameType == continuationFrame || frameType == TextMessage || frameType == BinaryMessage {

		c.readLength += c.readRemaining
		if c.readLength < 0 {
			return noFrame, ErrReadLimit
		}

		if c.readLimit > 0 && c.readLength > c.readLimit {
			_ = c.WriteControl(CloseMessage, FormatCloseMessage(CloseMessageTooBig, ""), time.Now().Add(writeWait))
			return noFrame, ErrReadLimit
		}

		return frameType, nil
	}

	var payload []byte
	if c.readRemaining > 0 {
		payload, err = c.read(int(c.readRemaining))
		_ = c.setReadRemaining(0)
		if err != nil {
			return noFrame, err
		}
		if c.isServer {
			maskBytes(c.readMaskKey, 0, payload)
		}
	}

	switch frameType {
	case PongMessage:
		if err := c.handlePong(payload); err != nil {
			return noFrame, err
		}
	case PingMessage:
		if err := c.handlePing(payload); err != nil {
			return noFrame, err
		}
	case CloseMessage:
		closeCode := CloseNoStatusReceived
		closeText := ""
		if len(payload) >= 2 {
			closeCode = int(binary.BigEndian.Uint16(payload))
			if !isValidReceivedCloseCode(closeCode) {
				return noFrame, c.handleProtocolError("bad close code " + strconv.Itoa(closeCode))
			}
			closeText = string(payload[2:])
			if !utf8.ValidString(closeText) {
				return noFrame, c.handleProtocolError("invalid utf8 payload in close frame")
			}
		}
		if err := c.handleClose(closeCode, closeText); err != nil {
			return noFrame, err
		}
		return noFrame, &CloseError{Code: closeCode, Text: closeText}
	}

	return frameType, nil
}

func (c *Conn) handleProtocolError(message string) error {
	if c == nil {
		return ErrNilConn
	}
	data := FormatCloseMessage(CloseProtocolError, message)
	if len(data) > maxControlFramePayloadSize {
		data = data[:maxControlFramePayloadSize]
	}
	_ = c.WriteControl(CloseMessage, data, time.Now().Add(writeWait))
	return errors.Warning("websocket: " + message)
}

func (c *Conn) NextReader() (messageType int, r io.Reader, err error) {
	if c == nil {
		return 0, nil, ErrNilConn
	}
	if c.reader != nil {
		_ = c.reader.Close()
		c.reader = nil
	}

	c.messageReader = nil
	c.readLength = 0

	for c.readErr == nil {
		frameType, err := c.advanceFrame()
		if err != nil {
			c.readErr = hideTempErr(err)
			break
		}

		if frameType == TextMessage || frameType == BinaryMessage {
			c.messageReader = &messageReader{c}
			c.reader = c.messageReader
			if c.readDecompress {
				c.reader = c.newDecompressionReader(c.reader)
			}
			return frameType, c.reader, nil
		}
	}

	c.readErrCount++
	if c.readErrCount >= 1000 {
		err = fmt.Errorf("repeated read on failed websocket connection")
		return
	}

	return noFrame, nil, c.readErr
}

func (c *Conn) ReadMessage() (messageType int, p []byte, err error) {
	if c == nil {
		return 0, nil, ErrNilConn
	}
	var r io.Reader
	messageType, r, err = c.NextReader()
	if err != nil {
		return messageType, nil, err
	}
	p, err = io.ReadAll(r)
	return messageType, p, err
}

func (c *Conn) SetReadDeadline(t time.Time) error {
	if c == nil {
		return ErrNilConn
	}
	if c.conn == nil {
		return ErrNilNetConn
	}
	return c.conn.SetReadDeadline(t)
}

func (c *Conn) SetReadLimit(limit int64) {
	if c == nil {
		return
	}
	c.readLimit = limit
}

func (c *Conn) CloseHandler() func(code int, text string) error {
	if c == nil {
		return nil
	}
	return c.handleClose
}

func (c *Conn) SetCloseHandler(h func(code int, text string) error) {
	if c == nil {
		return
	}
	if h == nil {
		h = func(code int, text string) error {
			message := FormatCloseMessage(code, "")
			_ = c.WriteControl(CloseMessage, message, time.Now().Add(writeWait))
			return nil
		}
	}
	c.handleClose = h
}

func (c *Conn) PingHandler() func(appData []byte) error {
	if c == nil {
		return nil
	}
	return c.handlePing
}

func (c *Conn) SetPingHandler(h func(appData []byte) error) {
	if c == nil {
		return
	}
	if h == nil {
		h = func(message []byte) error {
			err := c.WriteControl(PongMessage, message, time.Now().Add(writeWait))
			if err == ErrCloseSent {
				return nil
			} else if e, ok := err.(net.Error); ok && e.Timeout() {
				return nil
			}
			return err
		}
	}
	c.handlePing = h
}

func (c *Conn) PongHandler() func(appData []byte) error {
	if c == nil {
		return nil
	}
	return c.handlePong
}

func (c *Conn) SetPongHandler(h func(appData []byte) error) {
	if c == nil {
		return
	}
	if h == nil {
		h = func([]byte) error { return nil }
	}
	c.handlePong = h
}

func (c *Conn) NetConn() net.Conn {
	if c == nil {
		return nil
	}
	return c.conn
}

func (c *Conn) EnableWriteCompression(enable bool) {
	if c == nil {
		return
	}
	c.enableWriteCompression = enable
}

func (c *Conn) SetCompressionLevel(level int) error {
	if c == nil {
		return ErrNilConn
	}
	if !isValidCompressionLevel(level) {
		return errors.Warning("websocket: invalid compression level")
	}
	c.compressionLevel = level
	return nil
}

func (c *Conn) WriteJSON(v interface{}) error {
	w, err := c.NextWriter(TextMessage)
	if err != nil {
		return err
	}
	p, encodeErr := json.Marshal(v)
	if encodeErr != nil {
		return errors.Warning("websocket: write json failed").WithCause(encodeErr)
	}
	_, _ = w.Write(p)
	return w.Close()
}

func (c *Conn) WriteText(p []byte) error {
	w, err := c.NextWriter(TextMessage)
	if err != nil {
		return err
	}
	_, _ = w.Write(p)
	return w.Close()
}
