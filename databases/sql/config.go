package sql

import (
	"context"
	db "database/sql"
	"fmt"
	"runtime"
	"strings"
)

type Config struct {
	Driver           string   `json:"driver"`
	MasterSlaverMode bool     `json:"masterSlaverMode,omitempty"`
	DSN              []string `json:"dsn,omitempty"`
	MaxIdles         int      `json:"maxIdles,omitempty"`
	MaxOpens         int      `json:"maxOpens,omitempty"`
	EnableDebugLog   bool     `json:"enableDebugLog"`
}

func (config *Config) CreateClient() (client Client, err error) {
	if config.DSN == nil || len(config.DSN) < 1 {
		err = fmt.Errorf("fns SQL: dsn is invalid")
		return
	}
	driver := strings.TrimSpace(config.Driver)
	if driver == "" {
		err = fmt.Errorf("fns SQL: driver is invalid")
		return
	}
	maxIdles := config.MaxIdles
	if maxIdles < 1 {
		maxIdles = 1
	}
	maxOpens := config.MaxOpens
	if maxOpens < 1 {
		maxOpens = runtime.NumCPU() * 2
	}

	if config.MasterSlaverMode {
		if len(config.DSN) < 2 {
			err = fmt.Errorf("fns SQL: masterSlaverMode is enabled but num of dsn is not gt 1")
			return
		}
		masterDSN := strings.TrimSpace(config.DSN[0])
		if masterDSN == "" {
			err = fmt.Errorf("fns SQL: masterSlaverMode is enabled but first of dsn is empty")
			return
		}
		master, openMasterErr := db.Open(driver, masterDSN)
		if openMasterErr != nil {
			err = fmt.Errorf("fns SQL: create master failed, dsn is %s, %v", masterDSN, openMasterErr)
			return
		}
		master.SetMaxIdleConns(maxIdles)
		master.SetMaxOpenConns(maxOpens)
		pingErr := master.PingContext(context.TODO())
		if pingErr != nil {
			err = fmt.Errorf("fns SQL: ping %s failed, %v", masterDSN, pingErr)
			return
		}

		slavers := make([]*db.DB, 0, 1)

		slaverDSNs := config.DSN[1:]
		for _, slaverDSN := range slaverDSNs {
			slaverDSN = strings.TrimSpace(slaverDSN)
			if slaverDSN == "" {
				err = fmt.Errorf("fns SQL: masterSlaverMode is enabled but one of slaver dsns is empty")
				return
			}
			slaver, openSlaverErr := db.Open(driver, slaverDSN)
			if openSlaverErr != nil {
				err = fmt.Errorf("fns SQL: create slaver failed, dsn is %s, %v", slaverDSN, openSlaverErr)
				return
			}
			slaver.SetMaxIdleConns(maxIdles)
			slaver.SetMaxOpenConns(maxOpens)
			pingSlaverErr := slaver.PingContext(context.TODO())
			if pingSlaverErr != nil {
				err = fmt.Errorf("fns SQL: ping %s failed, %v", slaverDSN, pingSlaverErr)
				return
			}
			slavers = append(slavers, slaver)
		}

		client = NewMasterSlaver(master, slavers)

	} else {
		if len(config.DSN) == 1 {
			dsn := strings.TrimSpace(config.DSN[0])
			if dsn == "" {
				err = fmt.Errorf("fns SQL: dsn is empty")
				return
			}
			d, openErr := db.Open(driver, dsn)
			if openErr != nil {
				err = fmt.Errorf("fns SQL: create connection failed, dsn is %s, %v", dsn, openErr)
				return
			}
			d.SetMaxIdleConns(maxIdles)
			d.SetMaxOpenConns(maxOpens)
			pingErr := d.PingContext(context.TODO())
			if pingErr != nil {
				err = fmt.Errorf("fns SQL: ping %s failed, %v", dsn, pingErr)
				return
			}
			client = NewStandalone(d)
		} else {
			v := make([]*db.DB, 0, 1)
			for _, dsn := range config.DSN {
				dsn = strings.TrimSpace(dsn)
				if dsn == "" {
					err = fmt.Errorf("fns SQL: dsn is empty")
					return
				}
				d, openErr := db.Open(driver, dsn)
				if openErr != nil {
					err = fmt.Errorf("fns SQL: create connection failed, dsn is %s, %v", dsn, openErr)
					return
				}
				d.SetMaxIdleConns(maxIdles)
				d.SetMaxOpenConns(maxOpens)
				pingErr := d.PingContext(context.TODO())
				if pingErr != nil {
					err = fmt.Errorf("fns SQL: ping %s failed, %v", dsn, pingErr)
					return
				}
				v = append(v, d)
			}
			client = NewCluster(v)
		}
	}
	return
}
