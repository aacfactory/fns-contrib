package redis

import (
	"github.com/aacfactory/fns-contrib/databases/redis/cmds"
	"github.com/redis/rueidis"
	"time"
)

type Command struct {
	Name   string        `json:"name"`
	Params []string      `json:"params"`
	TTL    time.Duration `json:"ttl"`
}

type IncompleteCommand interface {
	Build() (cmd Command)
}

type Commands []Command

func (cc Commands) Len() int {
	return len(cc)
}

func (cc Commands) Build(client rueidis.Client) (v []rueidis.Completed, ok bool) {
	v = make([]rueidis.Completed, cc.Len())
	for i, value := range cc {
		b, has := cmds.GetBuilder(value.Name)
		if has {
			var c rueidis.Completed
			c, ok = b.Completed(client, value.Params)
			if !ok {
				return
			}
			v[i] = c
		} else {
			v[i] = client.B().Arbitrary().Keys(value.Name).Args(value.Params...).Build()
		}
	}
	ok = true
	return
}

type Cacheables []Command

func (cc Cacheables) Len() int {
	return len(cc)
}

func (cc Cacheables) Valid() bool {
	n := 0
	for _, cacheable := range cc {
		if cacheable.TTL > 0 {
			n++
		}
	}
	return n > 0
}

func (cc Cacheables) Build(client rueidis.Client) (v []rueidis.CacheableTTL, ok bool) {
	v = make([]rueidis.CacheableTTL, len(cc))
	for i, value := range cc {
		b, has := cmds.GetBuilder(value.Name)
		if has {
			var c rueidis.Cacheable
			c, ok = b.Cacheable(client, value.Params)
			if !ok {
				return
			}
			v[i] = rueidis.CT(c, value.TTL)
		} else {
			completed := client.B().Arbitrary().Keys(value.Name).Args(value.Params...).Build()
			v[i] = rueidis.CT(rueidis.Cacheable(completed), value.TTL)
		}
	}
	ok = true
	return
}
