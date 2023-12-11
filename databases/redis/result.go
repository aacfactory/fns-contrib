package redis

import (
	"github.com/redis/rueidis"
)

type Result struct {
	Message
	err error
}

type result struct {
	raw rueidis.RedisResult
}
