package databases

import "github.com/redis/rueidis"

type Database struct {
	client rueidis.Client
}
