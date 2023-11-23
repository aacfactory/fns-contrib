package databases

import "github.com/aacfactory/json"

type Column struct {
	Type         string          `json:"type"`
	DatabaseType string          `json:"databaseType"`
	Name         string          `json:"name"`
	Value        json.RawMessage `json:"value"`
	Nil          bool            `json:"nil"`
}
