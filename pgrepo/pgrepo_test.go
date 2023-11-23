package pgrepo

import (
	"context"
	"database/sql"
	"net/url"
	"strings"
	"testing"

	"github.com/spf13/viper"
	"github.com/stretchr/testify/require"
)

const yamlConfig = `
databases:
  postgres:
    default:
      url: 'postgresql://postgres:postgres@localhost:5432/testdb?sslmode=disable'

`

func substDBName(t *testing.T, cfg *viper.Viper, dbName string) {
	t.Helper()

	dbURL := cfg.GetString("databases.postgres.default.url")
	u, err := url.Parse(dbURL)
	require.NoError(t, err)
	u.Path = "/" + dbName

	cfg.Set("databases.postgres.default.url", u.String())
}

func TestNew(t *testing.T) {
	cfg := viper.New()
	cfg.SetConfigType("yaml")
	require.NoError(t, cfg.ReadConfig(strings.NewReader(yamlConfig)))
	dbName := randomDBName()
	substDBName(t, cfg, dbName)

	t.Run("with viper config", testWithViper(dbName, cfg))

	dbName = randomDBName()
	substDBName(t, cfg, dbName)
	cfg.Set("databases.pgconfig.trace.enabled", true)
	t.Run("with trace enabled", testWithViper(dbName, cfg))
}

func testWithViper(dbName string, cfg *viper.Viper) func(*testing.T) {
	return func(t *testing.T) {
		ctx := context.Background()

		db, _, err := EnsureDB(ctx, dbName, WithViper(cfg))
		t.Cleanup(func() {
			if db != nil {
				_ = db.Close()
			}
			_, _ = DropDB(ctx, dbName, WithViper(cfg))
		})
		require.NoError(t, err)

		repo := New(DefaultDBAlias, WithViper(cfg))
		require.NotNil(t, repo)

		require.NoError(t, repo.Start())
		defer func() {
			_ = repo.Stop()
		}()

		t.Run("DB is open", func(t *testing.T) {
			require.NotNil(t, repo.DB())

			var ignored sql.NullString
			require.NoError(t,
				repo.DB().QueryRowContext(ctx, "SELECT datname FROM pg_database WHERE datname = $1", dbName).Scan(&ignored),
			)

			require.NoError(t, repo.HealthCheck())
		})

		t.Run("DB is closed", func(t *testing.T) {
			require.NoError(t, repo.Stop())

			require.Error(t, repo.HealthCheck())
		})
	}
}
