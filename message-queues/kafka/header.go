package kafka

import "github.com/segmentio/kafka-go"

type Header struct {
	Key   string
	Value []byte
}

type Headers []Header

func (headers Headers) ConvertToKafkaHeaders() (v []kafka.Header) {
	hLen := len(headers)
	if hLen == 0 {
		return
	}
	v = make([]kafka.Header, hLen)
	for i, header := range headers {
		v[i] = kafka.Header{
			Key:   header.Key,
			Value: header.Value,
		}
	}
	return
}
