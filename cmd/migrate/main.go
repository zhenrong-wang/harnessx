package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
)

func main() {
	directory := flag.String("dir", "db/migrations", "directory containing .sql migrations")
	flag.Parse()
	databaseURL := strings.TrimSpace(os.Getenv("DATABASE_URL"))
	if databaseURL == "" {
		fatal("DATABASE_URL is required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	conn, err := pgx.Connect(ctx, databaseURL)
	if err != nil {
		fatal("connect: %v", err)
	}
	defer conn.Close(context.Background())
	if _, err := conn.Exec(ctx, `CREATE SCHEMA IF NOT EXISTS harnessx_migrations;
CREATE TABLE IF NOT EXISTS harnessx_migrations.versions (
  name text PRIMARY KEY,
  checksum text NOT NULL,
  applied_at timestamptz NOT NULL DEFAULT now()
)`); err != nil {
		fatal("initialize migration table: %v", err)
	}
	if _, err := conn.Exec(ctx, `SELECT pg_advisory_lock(hashtext('harnessx-schema-migrations'))`); err != nil {
		fatal("acquire migration lock: %v", err)
	}
	defer conn.Exec(context.Background(), `SELECT pg_advisory_unlock(hashtext('harnessx-schema-migrations'))`)

	entries, err := os.ReadDir(*directory)
	if err != nil {
		fatal("read migrations: %v", err)
	}
	var names []string
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".sql") {
			names = append(names, entry.Name())
		}
	}
	sort.Strings(names)
	for _, name := range names {
		raw, err := os.ReadFile(filepath.Join(*directory, name))
		if err != nil {
			fatal("read %s: %v", name, err)
		}
		sum := sha256.Sum256(raw)
		checksum := hex.EncodeToString(sum[:])
		var existing string
		err = conn.QueryRow(ctx, `SELECT checksum FROM harnessx_migrations.versions WHERE name=$1`, name).Scan(&existing)
		if err == nil {
			if existing != checksum {
				fatal("migration %s was modified after it was applied", name)
			}
			continue
		}
		if err != pgx.ErrNoRows {
			fatal("inspect migration %s: %v", name, err)
		}
		tx, err := conn.Begin(ctx)
		if err != nil {
			fatal("begin migration %s: %v", name, err)
		}
		if _, err = tx.Exec(ctx, string(raw)); err == nil {
			_, err = tx.Exec(ctx, `INSERT INTO harnessx_migrations.versions(name, checksum) VALUES($1,$2)`, name, checksum)
		}
		if err != nil {
			_ = tx.Rollback(ctx)
			fatal("apply migration %s: %v", name, err)
		}
		if err := tx.Commit(ctx); err != nil {
			fatal("commit migration %s: %v", name, err)
		}
		fmt.Printf("applied %s\n", name)
	}
}

func fatal(format string, args ...any) {
	fmt.Fprintf(os.Stderr, format+"\n", args...)
	os.Exit(1)
}
