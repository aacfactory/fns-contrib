package websocket

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"github.com/aacfactory/errors"
	"github.com/aacfactory/fns/transports"
	"io"
	"net"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"strings"
	"time"
)

var DefaultDialer = &Dialer{
	Proxy:            http.ProxyFromEnvironment,
	HandshakeTimeout: 45 * time.Second,
}

type Dialer struct {
	NetDial           func(network, addr string) (net.Conn, error)
	NetDialContext    func(ctx context.Context, network, addr string) (net.Conn, error)
	NetDialTLSContext func(ctx context.Context, network, addr string) (net.Conn, error)
	Proxy             func(*http.Request) (*url.URL, error)
	TLSClientConfig   *tls.Config
	HandshakeTimeout  time.Duration
	ReadBufferSize    int
	WriteBufferSize   int
	WriteBufferPool   BufferPool
	Subprotocols      []string
	EnableCompression bool
	Jar               http.CookieJar
}

func (d *Dialer) Dial(urlStr string, requestHeader http.Header) (*Conn, *http.Response, error) {
	return d.DialContext(context.Background(), urlStr, requestHeader)
}

func (d *Dialer) DialContext(ctx context.Context, urlStr string, requestHeader http.Header) (*Conn, *http.Response, error) {
	if d == nil {
		*d = *DefaultDialer
	}

	challengeKey, err := generateChallengeKey()
	if err != nil {
		return nil, nil, err
	}

	u, err := url.Parse(urlStr)
	if err != nil {
		return nil, nil, err
	}

	switch u.Scheme {
	case "ws":
		u.Scheme = "http"
	case "wss":
		u.Scheme = "https"
	default:
		return nil, nil, errMalformedURL
	}

	if u.User != nil {
		return nil, nil, errMalformedURL
	}

	req := &http.Request{
		Method:     http.MethodGet,
		URL:        u,
		Proto:      "HTTP/1.1",
		ProtoMajor: 1,
		ProtoMinor: 1,
		Header:     make(http.Header),
		Host:       u.Host,
	}
	req = req.WithContext(ctx)

	if d.Jar != nil {
		for _, cookie := range d.Jar.Cookies(u) {
			req.AddCookie(cookie)
		}
	}

	req.Header["Upgrade"] = []string{"websocket"}
	req.Header["Connection"] = []string{"Upgrade"}
	req.Header["Sec-WebSocket-Key"] = []string{challengeKey}
	req.Header["Sec-WebSocket-Version"] = []string{"13"}
	if len(d.Subprotocols) > 0 {
		req.Header["Sec-WebSocket-Protocol"] = []string{strings.Join(d.Subprotocols, ", ")}
	}
	for k, vs := range requestHeader {
		switch {
		case k == "Host":
			if len(vs) > 0 {
				req.Host = vs[0]
			}
		case k == "Upgrade" ||
			k == "Connection" ||
			k == "Sec-Websocket-Key" ||
			k == "Sec-Websocket-Version" ||
			k == "Sec-Websocket-Extensions" ||
			(k == "Sec-Websocket-Protocol" && len(d.Subprotocols) > 0):
			return nil, nil, errors.Warning("websocket: duplicate header not allowed: " + k)
		case k == "Sec-Websocket-Protocol":
			req.Header["Sec-WebSocket-Protocol"] = vs
		default:
			req.Header[k] = vs
		}
	}

	if d.EnableCompression {
		req.Header["Sec-WebSocket-Extensions"] = []string{"permessage-deflate; server_no_context_takeover; client_no_context_takeover"}
	}

	if d.HandshakeTimeout != 0 {
		var cancel func()
		ctx, cancel = context.WithTimeout(ctx, d.HandshakeTimeout)
		defer cancel()
	}

	var netDial func(network, add string) (net.Conn, error)

	switch u.Scheme {
	case "http":
		if d.NetDialContext != nil {
			netDial = func(network, addr string) (net.Conn, error) {
				return d.NetDialContext(ctx, network, addr)
			}
		} else if d.NetDial != nil {
			netDial = d.NetDial
		}
	case "https":
		if d.NetDialTLSContext != nil {
			netDial = func(network, addr string) (net.Conn, error) {
				return d.NetDialTLSContext(ctx, network, addr)
			}
		} else if d.NetDialContext != nil {
			netDial = func(network, addr string) (net.Conn, error) {
				return d.NetDialContext(ctx, network, addr)
			}
		} else if d.NetDial != nil {
			netDial = d.NetDial
		}
	default:
		return nil, nil, errMalformedURL
	}

	if netDial == nil {
		netDialer := &net.Dialer{}
		netDial = func(network, addr string) (net.Conn, error) {
			return netDialer.DialContext(ctx, network, addr)
		}
	}

	if deadline, ok := ctx.Deadline(); ok {
		forwardDial := netDial
		netDial = func(network, addr string) (net.Conn, error) {
			c, err := forwardDial(network, addr)
			if err != nil {
				return nil, err
			}
			err = c.SetDeadline(deadline)
			if err != nil {
				_ = c.Close()
				return nil, err
			}
			return c, nil
		}
	}

	if d.Proxy != nil {
		proxyURL, err := d.Proxy(req)
		if err != nil {
			return nil, nil, err
		}
		if proxyURL != nil {
			dialer, err := proxy_FromURL(proxyURL, netDialerFunc(netDial))
			if err != nil {
				return nil, nil, err
			}
			netDial = dialer.Dial
		}
	}

	hostPort, hostNoPort := hostPortNoPort(u)
	trace := httptrace.ContextClientTrace(ctx)
	if trace != nil && trace.GetConn != nil {
		trace.GetConn(hostPort)
	}

	netConn, err := netDial("tcp", hostPort)
	if err != nil {
		return nil, nil, err
	}
	if trace != nil && trace.GotConn != nil {
		trace.GotConn(httptrace.GotConnInfo{
			Conn: netConn,
		})
	}

	defer func() {
		if netConn != nil {
			_ = netConn.Close()
		}
	}()

	if u.Scheme == "https" && d.NetDialTLSContext == nil {

		cfg := cloneTLSConfig(d.TLSClientConfig)
		if cfg.ServerName == "" {
			cfg.ServerName = hostNoPort
		}
		tlsConn := tls.Client(netConn, cfg)
		netConn = tlsConn

		if trace != nil && trace.TLSHandshakeStart != nil {
			trace.TLSHandshakeStart()
		}
		err := doHandshake(ctx, tlsConn, cfg)
		if trace != nil && trace.TLSHandshakeDone != nil {
			trace.TLSHandshakeDone(tlsConn.ConnectionState(), err)
		}

		if err != nil {
			return nil, nil, err
		}
	}

	conn := newConn(netConn, false, d.ReadBufferSize, d.WriteBufferSize, d.WriteBufferPool, nil, nil)

	if err := req.Write(netConn); err != nil {
		return nil, nil, err
	}

	if trace != nil && trace.GotFirstResponseByte != nil {
		if peek, err := conn.br.Peek(1); err == nil && len(peek) == 1 {
			trace.GotFirstResponseByte()
		}
	}

	resp, err := http.ReadResponse(conn.br, req)
	if err != nil {
		if d.TLSClientConfig != nil {
			for _, proto := range d.TLSClientConfig.NextProtos {
				if proto != "http/1.1" {
					return nil, nil, fmt.Errorf(
						"websocket: protocol %q was given but is not supported;"+
							"sharing tls.Config with net/http Transport can cause this error: %w",
						proto, err,
					)
				}
			}
		}
		return nil, nil, err
	}

	if d.Jar != nil {
		if rc := resp.Cookies(); len(rc) > 0 {
			d.Jar.SetCookies(u, rc)
		}
	}

	trh := transports.AcquireHeader()
	defer transports.ReleaseHeader(trh)
	for k, vv := range resp.Header {
		for _, v := range vv {
			trh.Add([]byte(k), []byte(v))
		}
	}

	if resp.StatusCode != 101 ||
		!tokenListContainsValue(trh, "Upgrade", "websocket") ||
		!tokenListContainsValue(trh, "Connection", "upgrade") ||
		resp.Header.Get("Sec-Websocket-Accept") != computeAcceptKey(challengeKey) {
		buf := make([]byte, 1024)
		n, _ := io.ReadFull(resp.Body, buf)
		resp.Body = io.NopCloser(bytes.NewReader(buf[:n]))
		return nil, resp, ErrBadHandshake
	}

	for _, ext := range parseExtensions(resp.Header) {
		if ext[""] != "permessage-deflate" {
			continue
		}
		_, snct := ext["server_no_context_takeover"]
		_, cnct := ext["client_no_context_takeover"]
		if !snct || !cnct {
			return nil, resp, errInvalidCompression
		}
		conn.newCompressionWriter = compressNoContextTakeover
		conn.newDecompressionReader = decompressNoContextTakeover
		break
	}

	resp.Body = io.NopCloser(bytes.NewReader([]byte{}))
	conn.subprotocol = resp.Header.Get("Sec-Websocket-Protocol")

	_ = netConn.SetDeadline(time.Time{})
	netConn = nil
	return conn, resp, nil
}
