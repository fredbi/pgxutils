package pgrepo

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/jmoiron/sqlx"
	"go.uber.org/zap"
)

func EnsureDB(ctx context.Context, dbName string, opts ...Option) (db *sqlx.DB, created bool, err error) {
	s := settingsFromOptions(opts)
	dbs := s.DBSettingsFor(dbName)
	l := s.logger

	created, err = CreateDB(ctx, dbName, opts...)
	if err != nil {
		return nil, true, err
	}

	db, err = sqlx.Open(driverName, dbs.URL)
	l.Info("database open", zap.String("db_url", dbs.RedactedURL()))

	return db, created, err
}

func CreateDB(parentCtx context.Context, dbName string, opts ...Option) (bool, error) {
	s := settingsFromOptions(opts)
	dbs := s.DBSettingsFor(dbName)
	l := s.logger.With(zap.String("db_name", dbName))

	if dbs.URL == "" {
		return false, fmt.Errorf(`no database URL found in config file. Expected  "url" in config section %q`, dbName)
	}

	if dbName == DefaultDBAlias {
		u, err := url.Parse(dbs.URL)
		if err != nil {
			return false, err
		}

		dbName = strings.TrimPrefix(u.Path, "/")
	}

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	db, closer, err := connectNoDB(ctx, dbs.URL, dbs)
	if err != nil {
		return false, err
	}
	defer closer()

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return false, err
	}

	ok, err := dbExists(ctx, tx, dbName)
	if err != nil {
		return false, err
	}

	if ok {
		return false, nil
	}

	l.Info("creating database")

	_, err = db.ExecContext(ctx, fmt.Sprintf(`CREATE DATABASE %s`, dbName))
	if err != nil {
		return false, fmt.Errorf("could not create database %s: %w", dbName, err)
	}

	err = tx.Commit()
	if err != nil {
		return false, fmt.Errorf("could not create database %s: %w", dbName, err)
	}

	l.Info("new database created")

	return true, nil
}

func DropDB(parentCtx context.Context, dbName string, opts ...Option) (bool, error) {
	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	s := settingsFromOptions(opts)
	dbs := s.DBSettingsFor(dbName)

	db, closer, err := connectNoDB(ctx, dbs.URL, dbs)
	if err != nil {
		return false, err
	}
	defer closer()

	tx, err := db.BeginTxx(ctx, nil)
	if err != nil {
		return false, err
	}

	ok, err := dbExists(ctx, tx, dbName)
	if err != nil {
		return false, err
	}

	if !ok {
		return false, nil
	}

	_, err = db.ExecContext(ctx, fmt.Sprintf(`DROP DATABASE IF EXISTS %s`, dbName))
	if err != nil {
		return false, fmt.Errorf("could not drop database %s: %w", dbName, err)
	}

	err = tx.Commit()
	if err != nil {
		return false, fmt.Errorf("could not drop database %s: %w", dbName, err)
	}

	return true, nil
}

// connectNoDB connects to the postgres engine pointed to by the DSN, but without any DB open
func connectNoDB(ctx context.Context, dsn string, s databaseSettings) (*sqlx.DB, func(), error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, nil, fmt.Errorf("DB URL is invalid: %w", err)
	}
	u.Path = ""

	db, err := sqlx.Open(driverName, u.String())
	if err != nil {
		return nil, nil, fmt.Errorf("could not connect to database server %v: %w", u, err)
	}

	if err = waitPing(ctx, db, s.maxWait()); err != nil {
		return nil, nil, err
	}

	return db, func() { _ = db.Close() }, nil
}

func dbExists(ctx context.Context, tx *sqlx.Tx, dbName string) (bool, error) {
	var ignored sql.NullString
	err := tx.QueryRowContext(ctx, "SELECT datname FROM pg_database WHERE datname = $1", dbName).Scan(&ignored)
	if err == nil {
		// already there
		return true, nil
	}

	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}

	return false, err
}
