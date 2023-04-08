package http3

import (
	"fmt"
	"github.com/aacfactory/logs"
)

type Printf struct {
	Core logs.Logger
}

func (p *Printf) Printf(layout string, v ...interface{}) {
	if p.Core.DebugEnabled() {
		p.Core.Debug().Message(fmt.Sprintf("fns: %s", fmt.Sprintf(layout, v...)))
	}
}

func (p *Printf) Write(b []byte) (n int, err error) {
	if p.Core.DebugEnabled() {
		p.Core.Debug().Message(fmt.Sprintf("fns: %s", string(b)))
		n = len(b)
	}
	return
}
