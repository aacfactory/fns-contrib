package configs

import (
	"fmt"
	"github.com/aacfactory/logs"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Logger struct {
	raw logs.Logger
}

func (log *Logger) Level() kgo.LogLevel {
	if log.raw.DebugEnabled() {
		return kgo.LogLevelDebug
	}
	if log.raw.InfoEnabled() {
		return kgo.LogLevelInfo
	}
	if log.raw.WarnEnabled() {
		return kgo.LogLevelWarn
	}
	if log.raw.ErrorEnabled() {
		return kgo.LogLevelError
	}
	return kgo.LogLevelNone
}

func (log *Logger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	var event logs.Event
	switch level {
	case kgo.LogLevelError:
		if !log.raw.ErrorEnabled() {
			return
		}
		event = log.raw.Error()
		break
	case kgo.LogLevelWarn:
		if !log.raw.WarnEnabled() {
			return
		}
		event = log.raw.Warn()
		break
	case kgo.LogLevelInfo:
		if !log.raw.InfoEnabled() {
			return
		}
		event = log.raw.Info()
		break
	case kgo.LogLevelDebug:
		if !log.raw.DebugEnabled() {
			return
		}
		event = log.raw.Debug()
		break
	default:
		return
	}
	if event == nil {
		return
	}
	kvLen := len(keyvals)
	for i := 0; i < kvLen; i++ {
		key, ok := keyvals[0].(string)
		if !ok {
			key = fmt.Sprintf("%v", keyvals[0])
		}
		v := fmt.Sprintf("%v", keyvals[i+1])
		event = event.With(key, v)
		i++
	}
	event.Message(msg)
	return
}
