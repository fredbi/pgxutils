package pgrepo

import (
	"time"

	"github.com/spf13/viper"
	"go.uber.org/zap"
)

type (
	// Option controls settings for a set of databases, as well as a default pool configuration
	Option func(*settings)

	// Option controls database settings
	DBOption func(*databaseSettings)

	// PoolOption controls pool settings for a database
	PoolOption func(*poolSettings)
)

/*
func withSettings(s settings) Option {
	return func(o *settings) {
		*o = s
	}
}
*/

func settingsFromOptions(opts []Option) settings {
	s := defaultSettings
	for _, apply := range opts {
		apply(&s)
	}

	return s
}

func databaseSettingsFromOptions(opts []DBOption) databaseSettings {
	dbs := defaultSettings.Databases[DefaultDBAlias]
	for _, apply := range opts {
		apply(&dbs)
	}

	return dbs
}

func poolSettingsFromOptions(opts []PoolOption) *poolSettings {
	ps := defaultSettings.PGConfig
	for _, apply := range opts {
		apply(ps)
	}

	return ps
}

// SettingsFromViper builds settings from a *viper.Viper configuration registry.
//
// Extra options (e.g. WithLogger, ...) can be added.
//
// It returns an error if the configuration cannot be unmarshalled into settings.
func SettingsFromViper(cfg *viper.Viper, opts ...Option) (Option, error) {
	s := settingsFromOptions(opts)
	s, err := makeSettingsFromViper(cfg, s.logger)

	return func(o *settings) {
		*o = s
	}, err
}

// WithLogger injects a parent logger for logging the pgx driver and tracing operations.
func WithLogger(lg *zap.Logger) Option {
	return func(o *settings) {
		o.logger = lg
	}
}

// WithName declares a name for the app using the DB, so the pgx logger is named.
func WithName(app string) Option {
	return func(o *settings) {
		o.app = app
	}
}

// WithViper is the same as SettingsFromViper, but it doesn't check for errors.
func WithViper(cfg *viper.Viper) Option {
	return func(o *settings) {
		s, _ := makeSettingsFromViper(cfg, o.logger)
		*o = s
	}
}

func WithDatabaseSettings(alias string, opts ...DBOption) Option {
	return func(o *settings) {
		dbs := databaseSettingsFromOptions(opts)

		o.Databases[alias] = dbs
	}
}

func WithDefaultPoolOptions(opts ...PoolOption) Option {
	return func(o *settings) {
		ps := poolSettingsFromOptions(opts)
		o.PGConfig = ps
	}
}

func WithURL(url string) DBOption {
	return func(o *databaseSettings) {
		o.URL = url
	}
}

func WithUser(user string) DBOption {
	return func(o *databaseSettings) {
		o.User = user
	}
}

func WithPassword(password string) DBOption {
	return func(o *databaseSettings) {
		o.Password = password
	}
}

func WithPoolSettings(opts ...PoolOption) DBOption {
	return func(o *databaseSettings) {
		ps := &poolSettings{}
		for _, apply := range opts {
			apply(ps)
		}

		o.PGConfig = ps
	}
}

func WithMaxIdleConns(maxIdleConns int) PoolOption {
	return func(o *poolSettings) {
		o.MaxIdleConns = maxIdleConns
	}
}

func WithMaxOpenConns(maxOpenConns int) PoolOption {
	return func(o *poolSettings) {
		o.MaxOpenConns = maxOpenConns
	}
}

func WithConnMaxIdleTime(connMaxIdleTime time.Duration) PoolOption {
	return func(o *poolSettings) {
		o.ConnMaxIdleTime = connMaxIdleTime
	}
}

func WithConnMaxLifeTime(connMaxLifeTime time.Duration) PoolOption {
	return func(o *poolSettings) {
		o.ConnMaxLifeTime = connMaxLifeTime
	}
}

func WithLogLevel(level string) PoolOption {
	return func(o *poolSettings) {
		o.Log.Level = level
	}
}

func WithTracing(enabled bool) PoolOption {
	return func(o *poolSettings) {
		o.Trace.Enabled = enabled
	}
}

func WithPingTimeout(timeout time.Duration) PoolOption {
	return func(o *poolSettings) {
		o.PingTimeout = timeout
	}
}

func WithSetClause(param, value string) PoolOption {
	return func(o *poolSettings) {
		if o.Set == nil {
			o.Set = make(map[string]string)
		}
		o.Set[param] = value
	}
}
