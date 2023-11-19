package pgrepo

import (
	"context"
	"database/sql"
	"errors"
	"time"

	"github.com/fredbi/go-trace/log"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/stdlib"
	"github.com/jmoiron/sqlx"
	"github.com/opencensus-integrations/ocsql"
	"go.uber.org/zap"
)

const driverName = "pgx"

// Repository knows how to handle a postgres backend database.
//
// The database driver is instrumented for tracing.
type Repository struct {
	db  *sqlx.DB // master instance
	log log.Factory
	app string

	databaseSettings
}

// Newcreates a new postgres repository for one DB alias declared in the settings.
//
// The new repository needs to be started wih Start() in order to create the connection pool.
func New(dbAlias string, opts ...Option) *Repository {
	settings := settingsFromOptions(opts)
	dbSettings := settings.DBSettingsFor(dbAlias)

	return &Repository{
		log:              log.NewFactory(settings.logger),
		databaseSettings: dbSettings,
		app:              settings.app,
	}
}

// DB master instance
func (r *Repository) DB() *sqlx.DB {
	return r.db
}

// Logger returns a logger factory
func (r Repository) Logger() log.Factory {
	return r.log
}

// Start a connection pool to a database, plus possibly another one to the read-only version of it
func (r *Repository) Start() error {
	l := r.log.Bg()
	s := r.databaseSettings

	if err := s.Validate(); err != nil {
		return err
	}

	connCfg := s.ConnConfig(s.DBURL(), r.log, r.app)
	db, err := r.open(connCfg)
	if err != nil {
		return err
	}
	r.db = db

	l.Info("connection pool ok", zap.String("db", connCfg.Database))

	return nil
}

// Stop the repository and close all connection pools.
//
// Stop may be called safely even if the database connection failed to start properly.
func (r *Repository) Stop() error {
	if r.db == nil {
		return nil
	}

	return r.db.Close()
}

// HealthCheck pings the database
func (r *Repository) HealthCheck() error {
	if r.db == nil {
		return errors.New("db not initialized")
	}

	ctxTimeout, cancel := context.WithTimeout(context.Background(), r.databaseSettings.PGConfig.PingTimeout)
	defer cancel()

	return r.db.PingContext(ctxTimeout)
}

func (r Repository) open(dcfg *pgx.ConnConfig) (*sqlx.DB, error) {
	addr := stdlib.RegisterConnConfig(dcfg)
	lg := r.log.Bg()
	lg.Debug("registered driver", zap.String("driver", driverName), zap.String("driver_config", dcfg.ConnString()), zap.String("db", dcfg.Database))

	s := r.databaseSettings
	opts := s.TraceOptions(dcfg.ConnString())
	instrumentedDriver := driverName

	if len(opts) > 0 {
		lg.Info("trace enabled for sql driver", zap.String("db", dcfg.Database))

		// opencensus tracing registered in the sql driver
		// (this wraps the sql driver with an instrumented version)
		var err error
		instrumentedDriver, err = ocsql.RegisterWithSource(driverName, addr, opts...)
		if err != nil {
			lg.Error("failed to register trace driver", zap.Error(err))
			return nil, err
		}

		lg.Debug("registered instrumented driver", zap.String("driver", instrumentedDriver))
	}

	db, err := sql.Open(instrumentedDriver, addr)
	if err != nil {
		return nil, err
	}

	if err = waitPing(db, s.maxWait()); err != nil {
		return nil, err
	}

	// connection pool settings
	s.SetPool(db)

	if s.PGConfig != nil {
		lg.Info("db pool settings",
			zap.String("driver", driverName),
			zap.Int("maxIdleConns", s.PGConfig.MaxIdleConns),
			zap.Int("maxOpenConns", s.PGConfig.MaxOpenConns),
			zap.Duration("connMaxIdleTime", s.PGConfig.ConnMaxIdleTime),
			zap.Duration("connMaxLifetime", s.PGConfig.ConnMaxLifeTime),
		)
	}

	return sqlx.NewDb(db, driverName), nil
}

// waitPing checks for the availability of the database connection for maxWait.
//
// If the database is not immediately available, it tries every second up to maxWait.
//
// This avoids a hard container restart when the database is not immediatly available
// (e.g. when a db proxy container is not ready yet).
func waitPing(db interface{ PingContext(context.Context) error }, maxWait time.Duration) (err error) {
	if maxWait < time.Second {
		maxWait = time.Second
	}

	ctxTimeout, cancel := context.WithTimeout(context.Background(), maxWait)
	defer cancel()

	err = db.PingContext(ctxTimeout)
	if err == nil {
		return
	}

	timer := time.NewTimer(maxWait)
	defer timer.Stop()

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err = db.PingContext(ctxTimeout)
			if err == nil {
				return
			}
		case <-timer.C:
			return db.PingContext(ctxTimeout)
		}
	}
}

func sqlDefaultTraceOptions() []ocsql.TraceOption {
	// _almost_ WithAllTraceOptions: just remove the WithRowsNext and Ping which produce a lot of clutter in traces
	return []ocsql.TraceOption{
		ocsql.WithAllowRoot(true),
		ocsql.WithLastInsertID(true),
		ocsql.WithQuery(true),
		ocsql.WithQueryParams(true),
		ocsql.WithRowsAffected(true),
		ocsql.WithRowsClose(true),
	}
}
