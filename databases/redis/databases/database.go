package databases

import "github.com/redis/rueidis"

type Database struct {
	client rueidis.Client
}

func (db *Database) Do() {
	db.client.Do()
}
