package hazelcasts

import (
	"github.com/aacfactory/logs"
	"github.com/hazelcast/hazelcast-go-client/logger"
)

type log struct {
	raw logs.Logger
}

func (l *log) Log(weight logger.Weight, f func() string) {
	switch weight {
	case logger.WeightDebug:
		if l.raw.DebugEnabled() {
			txt := f()
			l.raw.Debug().Message(txt)
		}
		break
	case logger.WeightWarn:
		if l.raw.WarnEnabled() {
			txt := f()
			l.raw.Warn().Message(txt)
		}
		break
	case logger.WeightError:
		if l.raw.ErrorEnabled() {
			txt := f()
			l.raw.Error().Message(txt)
		}
		break
	default:
		break
	}
	return
}
