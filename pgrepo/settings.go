package pgrepo

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/fredbi/go-cli/config"
	"github.com/fredbi/go-trace/log"
	zapadapter "github.com/jackc/pgx-zap"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/tracelog"
	"github.com/opencensus-integrations/ocsql"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"gopkg.in/yaml.v3"
)

const (
	// DefaultURL points to a local test DB with user postgres.
	//
	// It is defined for a simple, workable demo setup.
	DefaultURL = "postgresql://postgres@localhost:5432/testdb?sslmode=disable"

	// DefaultDBAlias is the default configuration alias for your DB.
	//
	// This is suited for most single-DB configurations.
	DefaultDBAlias = "default"

	// DefaultLogLevel is the default log level for the database driver.
	//
	// The default is set to warn, as the pgx driver is pretty verbose in "info" mode.
	DefaultLogLevel = "warn"
)

var (
	// standard lib defaults
	defaultSettings = settings{
		PGConfig: &poolSettings{
			MaxIdleConns:    2,
			MaxOpenConns:    0,
			ConnMaxLifeTime: 0,
			ConnMaxIdleTime: 0,
			Log: logSettings{
				Level: DefaultLogLevel,
			},
			Trace: traceSettings{
				Enabled: false,
			},
			PingTimeout: 10 * time.Second,
		},
		Databases: map[string]databaseSettings{
			DefaultDBAlias: {
				URL: DefaultURL,
			},
		},
		app:    "",
		logger: zap.NewExample(),
	}

	defaultsMx sync.Mutex
)

type (
	settings struct {
		PGConfig  *poolSettings
		Databases map[string]databaseSettings `mapstructure:"postgres" yaml:"postgres" json:"postgres"`

		app    string
		logger *zap.Logger
	}

	poolSettings struct {
		MaxIdleConns    int
		MaxOpenConns    int
		ConnMaxLifeTime time.Duration
		ConnMaxIdleTime time.Duration
		PingTimeout     time.Duration
		Log             logSettings
		Trace           traceSettings
		Set             map[string]string //	plan_cache_mode: auto|force_custom_plan|force_generic_plan
	}

	logSettings struct {
		Level string
	}

	traceSettings struct {
		Enabled bool
	}

	databaseSettings struct {
		URL      string
		User     string
		Password string
		PGConfig *poolSettings
		// Replicas []string
	}
)

// DefaultSettings returns all defaults for this package as a viper registry.
//
// This is primarily intended for documentation & help purpose.
//
// The configuration is hierarchized like so:
//
// databases:
//
//	postgres:
//	  default:
//	    url: postgres://localhost:5432/test
//	    user: $PG_USER
//	    password: $PG_PASSWORD
//	    pgconfig: # pool settings for this database
//	      maxIdleConns: 25
//	      maxOpenConns: 50
//	      connMaxLifetime: 5m
//	      pingTimeout: 10s
//	      log:
//	        level: warn
//	      trace:
//	        enabled: false
//	  otherDB:
//	    url: postgres://user:password@localhost:5433/other
//	    config:  # pool settings for a postgres databases
//	      maxIdleConns: 55
//	pgconfig: # pool settings for this database
//	  maxIdleConns: 2
//	  maxOpenConns: 0
func DefaultSettings() *viper.Viper {
	v := viper.New()
	v.SetConfigType("yaml")
	asYAML, _ := yaml.Marshal(defaultSettings)
	_ = v.ReadConfig(bytes.NewReader(asYAML))

	return v
}

// SetDefaults sets the package-level defauts
func SetDefaults(opts ...Option) {
	defaultsMx.Lock()
	defer defaultsMx.Unlock()

	defaultSettings = settingsFromOptions(opts)
}

func makeSettingsFromViper(cfg *viper.Viper, l *zap.Logger) (settings, error) {
	s := defaultSettings

	if cfg == nil {
		l.Warn("no config passed. Using defaults")

		return s, nil
	}

	allDBConfig := config.ViperSub(cfg, "databases") // workaround for known issue with cfg.Sub()
	if allDBConfig == nil {
		l.Warn("no databases section passed in config. Using defaults")

		return s, nil
	}

	if err := allDBConfig.Unmarshal(&s); err != nil {
		return s, err
	}

	return s, nil
}

func (s settings) DBSettingsFor(db string) databaseSettings {
	l := s.logger.With(zap.String("db_alias", db))
	dbConfig, ok := s.Databases[db]
	if !ok {
		if defaultDBSettings, hasDefault := s.Databases[DefaultDBAlias]; hasDefault {
			return defaultDBSettings
		}

		l.Warn("no defaults available. Returning empty settings")

		return databaseSettings{}
	}

	if dbConfig.PGConfig == nil {
		dbConfig.PGConfig = s.PGConfig
	}

	return dbConfig
}

// LogLevel returns a pgx log level from config
func (r databaseSettings) LogLevel() tracelog.LogLevel {
	if r.PGConfig == nil {
		level, _ := tracelog.LogLevelFromString(DefaultLogLevel)

		return level
	}

	level, err := tracelog.LogLevelFromString(r.PGConfig.Log.Level)
	if err != nil {
		level, _ = tracelog.LogLevelFromString(DefaultLogLevel)
	}

	return level
}

// SetPool sets the connection pool parameters from config
func (r databaseSettings) SetPool(db *sql.DB) {
	if r.PGConfig == nil {
		return
	}

	if r.PGConfig.MaxIdleConns > 0 {
		db.SetMaxIdleConns(r.PGConfig.MaxIdleConns)
	}
	if r.PGConfig.MaxOpenConns > 0 {
		db.SetMaxOpenConns(r.PGConfig.MaxOpenConns)
	}
	if r.PGConfig.ConnMaxLifeTime > 0 {
		db.SetConnMaxLifetime(r.PGConfig.ConnMaxLifeTime)
	}
}

// TraceOptions returns the trace options for the opencensus driver wrapper
func (r databaseSettings) TraceOptions(u string) []ocsql.TraceOption {
	if r.PGConfig == nil {
		return nil
	}

	if !r.PGConfig.Trace.Enabled {
		return nil
	}

	v, _ := url.Parse(u)

	return append(sqlDefaultTraceOptions(), ocsql.WithInstanceName(v.Redacted()))
}

func (r *databaseSettings) SwitchDB(dbName string) error {
	target := r.DBURL()
	u, err := url.Parse(target)
	if err != nil {
		return err
	}

	u.Path = "/" + dbName
	r.URL = u.String()

	return nil
}

// ConnConfig builds a pgx configuration from the URL and additional settings.
//
// Under the hood, pgx merges standard pg parameters such as env variables and pgpass file.
func (r databaseSettings) ConnConfig(u string, lg log.Factory, app string) *pgx.ConnConfig {
	// driver settings with logs and tag for logs
	l := lg.Bg()

	var rtParams map[string]string
	if app != "" {
		rtParams = map[string]string{
			"application_name": app,
		}
	}

	dcfg, err := pgx.ParseConfig(u)
	if err != nil {
		l.Error("invalid postgres URL specification", zap.Error(err))

		return nil
	}

	if user := os.ExpandEnv(r.User); user != "" {
		dcfg.User = user
	}

	if password := os.ExpandEnv(r.Password); password != "" {
		dcfg.Password = password
	}

	if r.PGConfig != nil && len(r.PGConfig.Set) > 0 {
		// execute SET key = value commands when the connection is established
		for k, v := range r.PGConfig.Set {
			l.Info("set command configured after db connect", zap.String("db_set_cmd", fmt.Sprintf(`SET %s = %s`, k, v)))
		}

		dcfg.AfterConnect = func(ctx context.Context, conn *pgconn.PgConn) error {
			for k, v := range r.PGConfig.Set {
				k = os.ExpandEnv(k)
				v = os.ExpandEnv(v)

				m := conn.Exec(ctx, fmt.Sprintf(`SET %s = %s`, k, v))
				_, e := m.ReadAll()
				if e != nil {
					return e
				}

				e = m.Close()
				if e != nil {
					return e
				}
			}

			return nil
		}
	}

	var pgxLoggerName string
	if app != "" {
		pgxLoggerName = fmt.Sprintf("pgx-%s", app)
	} else {
		pgxLoggerName = "pgx"
	}

	// relevel the inner logger for pgx
	var driverLogger tracelog.Logger
	zapLogger := lg.Zap().Named(pgxLoggerName).WithOptions(zap.AddCallerSkip(1))
	pgxLevel := r.LogLevel()
	switch pgxLevel {
	case tracelog.LogLevelTrace:
		break
	case tracelog.LogLevelNone:
		driverLogger = zapadapter.NewLogger(zap.NewNop())
	default:
		zapLevel, _ := zapcore.ParseLevel(pgxLevel.String())
		driverLogger = zapadapter.NewLogger(zapLogger.WithOptions(zap.IncreaseLevel(zapLevel)))
	}
	tr := &tracelog.TraceLog{
		Logger:   driverLogger,
		LogLevel: pgxLevel,
	}
	dcfg.Tracer = tr
	dcfg.Config.RuntimeParams = rtParams

	tr.Logger.Log(context.Background(),
		tracelog.LogLevelInfo, "db log level",
		map[string]interface{}{
			"log-level": tr.LogLevel.String(),
		})

	return dcfg
}

func (r databaseSettings) DBURL() string {
	u := os.ExpandEnv(r.URL)

	return u
}

func (r databaseSettings) RedactedURL() string {
	v, _ := url.Parse(r.DBURL())

	return v.Redacted()
}

func (r databaseSettings) validateURL(value string) error {
	if value == "" {
		return fmt.Errorf(`connection string is required`)
	}

	_, err := url.Parse(value)

	return err
}

// Validate the configuration
func (r databaseSettings) Validate() error {
	if err := r.validateURL(r.DBURL()); err != nil {
		return err
	}

	_, err := pgx.ParseConfig(r.DBURL())
	if err != nil {
		return fmt.Errorf("invalid connection string: %s", err)
	}

	if r.PGConfig != nil && r.PGConfig.Log.Level != "" {
		lvl := r.PGConfig.Log.Level
		if _, err := tracelog.LogLevelFromString(lvl); err != nil {
			return fmt.Errorf("invalid log level for pgx driver [%q]: %w", lvl, err)
		}
	}

	return nil
}

func (r databaseSettings) maxWait() time.Duration {
	if r.PGConfig == nil || r.PGConfig.PingTimeout < time.Second {
		return defaultSettings.PGConfig.PingTimeout
	}

	return r.PGConfig.PingTimeout
}
