package websocket

import (
	"bytes"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/commons/bytex"
	"github.com/aacfactory/fns/service/transports"
	"github.com/savsgio/gotils/strconv"
	"github.com/valyala/fasthttp"
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

type Handler func(conn *Conn)

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
		return u.responseError(w, r, fasthttp.StatusMethodNotAllowed, fmt.Sprintf("%s request method is not GET", badHandshake))
	}

	if !tokenContainsValue(r.Header().Get("Connection"), "Upgrade") {
		return u.responseError(w, r, fasthttp.StatusBadRequest, fmt.Sprintf("%s 'upgrade' token not found in 'Connection' header", badHandshake))
	}

	if !tokenContainsValue(r.Header().Get("Upgrade"), "Websocket") {
		return u.responseError(w, r, fasthttp.StatusBadRequest, fmt.Sprintf("%s 'websocket' token not found in 'Upgrade' header", badHandshake))
	}

	if !tokenContainsValue(r.Header().Get("Sec-Websocket-Version"), "13") {
		return u.responseError(w, r, fasthttp.StatusBadRequest, "websocket: unsupported version: 13 not found in 'Sec-Websocket-Version' header")
	}

	if len(w.Header().Get("Sec-Websocket-Extensions")) > 0 {
		return u.responseError(w, r, fasthttp.StatusInternalServerError, "websocket: application specific 'Sec-WebSocket-Extensions' headers are unsupported")
	}

	checkOrigin := u.CheckOrigin
	if checkOrigin == nil {
		checkOrigin = checkSameOrigin
	}
	if !checkOrigin(r) {
		return u.responseError(w, r, fasthttp.StatusForbidden, "websocket: request origin not allowed by FastHTTPUpgrader.CheckOrigin")
	}

	challengeKey := r.Header().Get("Sec-Websocket-Key")
	if len(challengeKey) == 0 {
		return u.responseError(w, r, fasthttp.StatusBadRequest, "websocket: not a websocket handshake: `Sec-WebSocket-Key' header is missing or blank")
	}

	subprotocol := u.selectSubprotocol(w, r)
	compress := u.isCompressionEnable(r)

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

	hijackErr := w.Hijack(func(netConn net.Conn) {
		writeBuf := poolWriteBuffer.Get().([]byte)
		c := newConn(netConn, true, u.ReadBufferSize, u.WriteBufferSize, u.WriteBufferPool, nil, writeBuf)
		if subprotocol != nil {
			c.subprotocol = strconv.B2S(subprotocol)
		}
		if compress {
			c.newCompressionWriter = compressNoContextTakeover
			c.newDecompressionReader = decompressNoContextTakeover
		}
		_ = netConn.SetDeadline(time.Time{})
		handler(c)
		writeBuf = writeBuf[0:0]
		poolWriteBuffer.Put(writeBuf)
	})

	if hijackErr != nil {
		return errors.Warning("websocket: upgrade failed").WithCause(hijackErr)
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
