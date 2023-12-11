package cmds

import "github.com/redis/rueidis"

type Builder interface {
	Completed(client rueidis.Client, params []string) (v rueidis.Completed, ok bool)
	Cacheable(client rueidis.Client, params []string) (v rueidis.Cacheable, ok bool)
}

var (
	builders = map[string]Builder{}
)

func LoadBuilder(name string) (b Builder, has bool) {
	b, has = builders[name]
	return
}

func init() {
	registerGeneric()
	registerString()
}
