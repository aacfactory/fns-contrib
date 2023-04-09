package websocket

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/service"
	"github.com/aacfactory/fns/service/transports"
	"net"
	"net/http"
	"net/url"
	"sync"
	"time"
)

const badHandshake = "websocket: the client is not using the websocket protocol: "

var strPermessageDeflate = []byte("permessage-deflate")

var poolWriteBuffer = sync.Pool{
	New: func() interface{} {
		var buf []byte
		return buf
	},
}

type HandshakeError struct {
	message string
}

func (e HandshakeError) Error() string { return e.message }

type Handler func(ctx context.Context, conn *Conn, header transports.Header)

type Upgrader struct {
	HandshakeTimeout  time.Duration
	ReadBufferSize    int
	WriteBufferSize   int
	WriteBufferPool   BufferPool
	Subprotocols      []string
	Error             func(w transports.ResponseWriter, r *transports.Request, status int, reason error)
	CheckOrigin       func(r *transports.Request) bool
	EnableCompression bool
}

func (u *Upgrader) responseError(w transports.ResponseWriter, r *transports.Request, status int, reason string) error {
	err := HandshakeError{reason}
	if u.Error != nil {
		u.Error(w, r, status, err)
	} else {
		w.Header().Set("Sec-Websocket-Version", "13")
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.SetStatus(status)
		_, _ = w.Write(bytex.FromString(http.StatusText(status)))
	}
	return err
}

func (u *Upgrader) selectSubprotocol(w transports.ResponseWriter, r *transports.Request) []byte {
	if u.Subprotocols != nil {
		clientProtocols := parseDataHeader(bytex.FromString(r.Header().Get("Sec-Websocket-Protocol")))
		for _, serverProtocol := range u.Subprotocols {
			for _, clientProtocol := range clientProtocols {
				if bytex.ToString(clientProtocol) == serverProtocol {
					return clientProtocol
				}
			}
		}
	} else if len(w.Header()) > 0 {
		return bytex.FromString(w.Header().Get("Sec-Websocket-Protocol"))
	}
	return nil
}

func (u *Upgrader) isCompressionEnable(r *transports.Request) bool {
	extensions := parseDataHeader(bytex.FromString(r.Header().Get("Sec-WebSocket-Extensions")))
	if u.EnableCompression {
		for _, ext := range extensions {
			if bytes.HasPrefix(ext, strPermessageDeflate) {
				return true
			}
		}
	}
	return false
}

func (u *Upgrader) Upgrade(w transports.ResponseWriter, r *transports.Request, handler Handler) error {
	if !r.IsGet() {
		return u.responseError(w, r, http.StatusMethodNotAllowed, fmt.Sprintf("%s request method is not GET", badHandshake))
	}

	if !tokenContainsValue(r.Header().Get("Connection"), "Upgrade") {
		return u.responseError(w, r, http.StatusBadRequest, fmt.Sprintf("%s 'upgrade' token not found in 'Connection' header", badHandshake))
	}

	if !tokenContainsValue(r.Header().Get("Upgrade"), "Websocket") {
		return u.responseError(w, r, http.StatusBadRequest, fmt.Sprintf("%s 'websocket' token not found in 'Upgrade' header", badHandshake))
	}

	if !tokenContainsValue(r.Header().Get("Sec-Websocket-Version"), "13") {
		return u.responseError(w, r, http.StatusBadRequest, "websocket: unsupported version: 13 not found in 'Sec-Websocket-Version' header")
	}

	if len(w.Header().Get("Sec-Websocket-Extensions")) > 0 {
		return u.responseError(w, r, http.StatusInternalServerError, "websocket: application specific 'Sec-WebSocket-Extensions' headers are unsupported")
	}

	checkOrigin := u.CheckOrigin
	if checkOrigin == nil {
		checkOrigin = checkSameOrigin
	}
	if !checkOrigin(r) {
		return u.responseError(w, r, http.StatusForbidden, "websocket: request origin not allowed by FastHTTPUpgrader.CheckOrigin")
	}

	challengeKey := r.Header().Get("Sec-Websocket-Key")
	if len(challengeKey) == 0 {
		return u.responseError(w, r, http.StatusBadRequest, "websocket: not a websocket handshake: `Sec-WebSocket-Key' header is missing or blank")
	}

	subprotocol := u.selectSubprotocol(w, r)
	compress := u.isCompressionEnable(r)

	async, hijackErr := w.Hijack(func(netConn net.Conn, rw *bufio.ReadWriter) (err error) {
		var br *bufio.Reader
		var writeBuf []byte
		if rw != nil {
			if rw.Reader.Buffered() > 0 {
				_ = netConn.Close()
				err = errors.Warning("websocket: client sent data before handshake is complete")
				return
			}
			if u.ReadBufferSize == 0 && bufioReaderSize(netConn, rw.Reader) > 256 {
				// Reuse hijacked buffered reader as connection reader.
				br = rw.Reader
			}
			buf := bufioWriterBuffer(netConn, rw.Writer)
			if u.WriteBufferPool == nil && u.WriteBufferSize == 0 && len(buf) >= maxFrameHeaderSize+256 {
				// Reuse hijacked write buffer as connection buffer.
				writeBuf = buf
			}
		} else {
			writeBuf = poolWriteBuffer.Get().([]byte)
		}

		c := newConn(netConn, true, u.ReadBufferSize, u.WriteBufferSize, u.WriteBufferPool, br, writeBuf)
		if subprotocol != nil {
			c.subprotocol = bytex.ToString(subprotocol)
		}
		if compress {
			c.newCompressionWriter = compressNoContextTakeover
			c.newDecompressionReader = decompressNoContextTakeover
		}
		if rw != nil {
			p := writeBuf
			if len(c.writeBuf) > len(p) {
				p = c.writeBuf
			}
			p = p[:0]
			p = append(p, "HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: "...)
			p = append(p, computeAcceptKey(challengeKey)...)
			p = append(p, "\r\n"...)
			if c.subprotocol != "" {
				p = append(p, "Sec-WebSocket-Protocol: "...)
				p = append(p, c.subprotocol...)
				p = append(p, "\r\n"...)
			}
			if compress {
				p = append(p, "Sec-WebSocket-Extensions: permessage-deflate; server_no_context_takeover; client_no_context_takeover\r\n"...)
			}
			for k, vs := range w.Header() {
				if k == "Sec-Websocket-Protocol" {
					continue
				}
				for _, v := range vs {
					p = append(p, k...)
					p = append(p, ": "...)
					for i := 0; i < len(v); i++ {
						b := v[i]
						if b <= 31 {
							// prevent response splitting.
							b = ' '
						}
						p = append(p, b)
					}
					p = append(p, "\r\n"...)
				}
			}
			p = append(p, "\r\n"...)
			if u.HandshakeTimeout > 0 {
				_ = netConn.SetWriteDeadline(time.Now().Add(u.HandshakeTimeout))
			}
			if _, err = netConn.Write(p); err != nil {
				_ = netConn.Close()
				return
			}
			if u.HandshakeTimeout > 0 {
				_ = netConn.SetWriteDeadline(time.Time{})
			}
		} else {
			_ = netConn.SetDeadline(time.Time{})
		}

		rt := service.GetRuntime(r.Context())
		ctx := rt.SetIntoContext(context.TODO())
		handler(ctx, c, r.Header())
		if rw == nil {
			writeBuf = writeBuf[0:0]
			poolWriteBuffer.Put(writeBuf)
		}
		return
	})
	if hijackErr != nil {
		return errors.Warning("websocket: upgrade failed").WithCause(hijackErr)
	}
	if async {
		w.SetStatus(http.StatusSwitchingProtocols)
		w.Header().Set("Upgrade", "websocket")
		w.Header().Set("Connection", "Upgrade")
		w.Header().Set("Sec-WebSocket-Accept", computeAcceptKeyBytes(bytex.FromString(challengeKey)))
		if compress {
			w.Header().Set("Sec-WebSocket-Extensions", "permessage-deflate; server_no_context_takeover; client_no_context_takeover")
		}
		if subprotocol != nil {
			w.Header().Set("Sec-WebSocket-Protocol", bytex.ToString(subprotocol))
		}
	}
	return nil
}

func checkSameOrigin(r *transports.Request) bool {
	origin := r.Header().Get("Origin")
	if len(origin) == 0 {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	return equalASCIIFold(u.Host, bytex.ToString(r.Host()))
}
