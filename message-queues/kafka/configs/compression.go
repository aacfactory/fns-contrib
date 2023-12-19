package configs

import "github.com/twmb/franz-go/pkg/kgo"

type Compression struct {
	Name  string `json:"name"`
	Level int    `json:"level"`
}

func (compression *Compression) Config() (v kgo.CompressionCodec) {
	switch compression.Name {
	case "none":
		v = kgo.NoCompression()
		break
	case "gzip":
		v = kgo.GzipCompression()
		break
	case "snappy":
		v = kgo.SnappyCompression()
		break
	case "lz4":
		v = kgo.Lz4Compression()
		break
	case "zstd":
		v = kgo.ZstdCompression()
		break
	default:
		return kgo.NoCompression()
	}
	if compression.Level > 0 {
		v = v.WithLevel(compression.Level)
	}
	return
}
