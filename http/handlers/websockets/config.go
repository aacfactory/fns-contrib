package websockets

type Config struct {
	MaxConnections    int    `json:"maxConnections"`
	HandshakeTimeout  string `json:"handshakeTimeout"`
	ReadBufferSize    string `json:"readBufferSize"`
	WriteBufferSize   string `json:"writeBufferSize"`
	EnableCompression bool   `json:"enableCompression"`
}
