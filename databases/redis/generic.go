package redis

import "github.com/aacfactory/fns-contrib/databases/redis/cmds"

func Copy(dst string, src string) IncompleteCommand {
	return CopyBuilder{
		src: src,
		dst: dst,
	}
}

type CopyBuilder struct {
	src string
	dst string
}

func (builder CopyBuilder) Build() (cmd Command) {
	cmd.Name = cmds.COPY
	cmd.Params = []string{builder.src, builder.dst}
	return
}

func Keys(key ...string) IncompleteCommand {
	return KeysBuilder{
		keys: key,
	}
}

type KeysBuilder struct {
	keys []string
}

func (builder KeysBuilder) Build() (cmd Command) {
	cmd.Name = cmds.KEYS
	cmd.Params = builder.keys
	return
}
