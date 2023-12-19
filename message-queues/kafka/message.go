package kafka

import "time"

const (
	UnknownTimestamp = iota - 1
	DefaultTimestamp
	AlternativeTimestamp
)

type TimestampType int8

const (
	NoCompression = uint8(iota)
	Gzip
	Snappy
	Lz4
	Zstd
)

type CompressionType uint8

type RecordAttrs interface {
	TimestampType() TimestampType
	CompressionType() CompressionType
	IsTransactional() bool
	IsControl() bool
}

type Message interface {
	Topic() (topic string)
	Key() (key []byte)
	Headers() (headers Headers)
	Body() (body []byte)
	Partition() (no int)
	Offset() (offset int64)
	Time() (v time.Time)
	Attributes() RecordAttrs
	ProducerEpoch() int16
	ProducerId() int64
	LeaderEpoch() int32
}
