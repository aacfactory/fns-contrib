package websockets

type Config struct {
	MaxConnections        int    `json:"maxConnections"`
	HandshakeTimeout      string `json:"handshakeTimeout"`
	ReadTimeout           string `json:"readTimeout"`
	ReadBufferSize        string `json:"readBufferSize"`
	WriteTimeout          string `json:"writeTimeout"`
	WriteBufferSize       string `json:"writeBufferSize"`
	EnableCompression     bool   `json:"enableCompression"`
	MaxRequestMessageSize string `json:"maxRequestMessageSize"`
}
