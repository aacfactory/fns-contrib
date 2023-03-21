package mqtt

import "github.com/aacfactory/fns-contrib/http/handlers/websockets"

func NewConnection(conn websockets.Connection) *Connection {
	return &Connection{
		conn: conn,
	}
}

type Connection struct {
	conn websockets.Connection
}

func (conn *Connection) Write(p []byte) (err error) {

	return
}

func (conn *Connection) Read() (p []byte, err error) {

	return
}

func (conn *Connection) Close() {
	_ = conn.conn.Close()
	return
}
