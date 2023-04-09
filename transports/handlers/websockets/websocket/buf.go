package websocket

type BufferPool interface {
	Get() interface{}
	Put(interface{})
}
