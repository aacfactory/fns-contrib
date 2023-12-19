package kafka

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

type Header struct {
	Key   string `json:"key"`
	Value []byte `json:"value"`
}

type Headers []Header

func (headers Headers) ConvertToKafkaHeaders() (v []kgo.RecordHeader) {
	hLen := len(headers)
	if hLen == 0 {
		return
	}
	v = make([]kgo.RecordHeader, hLen)
	for i, header := range headers {
		v[i] = kgo.RecordHeader{
			Key:   header.Key,
			Value: header.Value,
		}
	}
	return
}

func convertHeaders(headers []kgo.RecordHeader) (v Headers) {
	for _, header := range headers {
		v = append(v, Header{
			Key:   header.Key,
			Value: header.Value,
		})
	}
	return
}
