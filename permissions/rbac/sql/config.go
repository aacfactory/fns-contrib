package sql

import "time"

type Config struct {
	Endpoint  string      `json:"endpoint"`
	RoleTable TableConfig `json:"roleTable"`
	UserTable TableConfig `json:"userTable"`
	Cache     CacheConfig `json:"cache"`
}

type TableConfig struct {
	Schema string `json:"schema"`
	Table  string `json:"table"`
}

type CacheConfig struct {
	Disable      bool          `json:"disable"`
	RolesTTL     time.Duration `json:"rolesTTL"`
	UserRolesTTL time.Duration `json:"userRolesTTL"`
}
