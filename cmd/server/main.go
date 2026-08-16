package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"time"

	"github.com/downfa11-org/tabellarius/pkg/bootstrap"
	"github.com/downfa11-org/tabellarius/pkg/config"
	"github.com/downfa11-org/tabellarius/pkg/source"
	_ "github.com/go-sql-driver/mysql"
)

func main() {
	confPath := flag.String("config", "cdc-config.yaml", "config file path")
	flag.Parse()

	cfg, err := config.Load(*confPath)
	if err != nil {
		log.Fatalf("failed to load config: %v", err)
	}

	db, err := connectWithRetry(cfg.Database.Type.DriverName(), cfg.DSN(), 3)
	if err != nil {
		log.Fatalf("[FATAL] %v", err)
	}

	log.Println("[OK] db connected")

	ok, err := bootstrap.Inspect(db, cfg)
	if err != nil {
		log.Fatalf("[FATAL] inspect failed: %v", err)
	}

	if !ok {
		log.Fatalf("[FATAL] cdc_log table not found. bootstrap required")
	}

	src, err := source.NewFromConfig(cfg)
	if err != nil {
		log.Fatalf("[FATAL] failed to initialize source: %v", err)
	}
	ctx := context.Background()
	src.Start(ctx)

	select {}
}

func connectWithRetry(driver, dsn string, maxRetry int) (*sql.DB, error) {
	var lastErr error

	for i := 1; i <= maxRetry; i++ {
		db, err := sql.Open(driver, dsn)
		if err == nil {
			if pingErr := db.Ping(); pingErr == nil {
				log.Printf("[OK] db connected (attempt=%d)", i)
				return db, nil
			} else {
				lastErr = pingErr
				db.Close()
			}
		} else {
			lastErr = err
		}

		log.Printf("[WARN] db connection failed (attempt=%d/%d): %v", i, maxRetry, lastErr)
		time.Sleep(time.Duration(i) * time.Second)
	}

	return nil, fmt.Errorf("db connection failed after %d attempts: %w", maxRetry, lastErr)
}
