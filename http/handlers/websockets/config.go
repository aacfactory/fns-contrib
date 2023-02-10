package websockets

type Config struct {
	ReadBufferSize    string `json:"readBufferSize"`
	WriteBufferSize   string `json:"writeBufferSize"`
	EnableCompression bool   `json:"enableCompression"`
	MaxConns          int64  `json:"maxConns"`
}
