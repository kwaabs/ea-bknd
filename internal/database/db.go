package database

import (
	"bknd-1/internal/config"
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/uptrace/bun"
	"github.com/uptrace/bun/dialect/pgdialect"
	"github.com/uptrace/bun/driver/pgdriver"
	"github.com/uptrace/bun/extra/bundebug"
)

// New connects to Postgres and returns a Bun DB handle.
func New(dsn string, cfg *config.Config) (*bun.DB, error) {
	// Create a connector and database handle
	sqldb := sql.OpenDB(pgdriver.NewConnector(pgdriver.WithDSN(dsn)))
	db := bun.NewDB(sqldb, pgdialect.New())

	// Optional query logging
	if cfg.BunDebug {
		db.AddQueryHook(bundebug.NewQueryHook(bundebug.WithVerbose(true)))
	}

	// Create context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// ✅ Verify connection first (ensures database is reachable)
	if err := db.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("failed to connect to database: %w", err)
	}

	// ✅ Set search path *after* verifying connection
	_, err := db.ExecContext(ctx, `SET search_path TO app, public`)
	if err != nil {
		return nil, fmt.Errorf("failed to set search path: %w", err)
	}

	return db, nil
}
