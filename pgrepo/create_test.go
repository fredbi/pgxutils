package pgrepo

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	pgUser       = "postgres"
	pgPassword   = "postgres"
	urlWithoutDB = "postgresql://localhost:5432/?sslmode=disable"
)

func TestEnsureDB(t *testing.T) {
	t.Run("with default config", func(t *testing.T) {
		ctx := context.Background()
		const dbName = "unittest_db"

		t.Run("drop DB", func(t *testing.T) {
			_, err := DropDB(ctx, dbName,
				WithDatabaseSettings("default",
					WithPassword(pgPassword), // force password to avoid pollution by environment or a local password file
				),
			)
			require.NoError(t, err)
		})

		t.Run("ensure DB", func(t *testing.T) {
			db, created, err := EnsureDB(context.Background(), dbName,
				WithDefaultPoolOptions(WithLogLevel("debug")),
				WithDatabaseSettings("default",
					WithPassword(pgPassword),
				),
			)
			t.Cleanup(func() {
				if db != nil {
					_ = db.Close()
				}
				_, _ = DropDB(ctx, dbName)
			})
			require.NoError(t, err)
			require.True(t, created)
			require.NotNil(t, db)
			require.NoError(t, db.Close())

			db2, created2, err2 := EnsureDB(context.Background(), dbName,
				WithDefaultPoolOptions(WithLogLevel("debug")),
				WithDatabaseSettings("default",
					WithPassword(pgPassword),
				),
			)
			t.Cleanup(func() {
				if db2 != nil {
					_ = db2.Close()
				}
				_, _ = DropDB(ctx, dbName)
			})
			require.NoError(t, err2)
			require.False(t, created2)
			require.NotNil(t, db2)
			require.NoError(t, db2.Close())

			t.Run("drop DB", func(t *testing.T) {
				dropped, err := DropDB(ctx, dbName,
					WithDatabaseSettings("default",
						WithPassword(pgPassword),
					),
				)
				require.NoError(t, err)
				require.True(t, dropped)
			})
		})
	})

	t.Run("with user/password", func(t *testing.T) {
		ctx := context.Background()
		dbName := randomDBName()

		db, created, err := EnsureDB(ctx, dbName,
			WithDatabaseSettings("default",
				WithURL(urlWithoutDB),
				WithUser(pgUser),
				WithPassword(pgPassword),
			),
		)
		t.Cleanup(func() {
			if db != nil {
				_ = db.Close()
			}
			_, _ = DropDB(ctx, dbName)
		})
		require.NoError(t, err)
		require.NotNil(t, db)
		require.True(t, created)
	})

	t.Run("with user/password from env", func(t *testing.T) {
		os.Setenv("PG_TEST_USER", pgUser)
		os.Setenv("PG_TEST_PASSWORD", pgPassword)
		ctx := context.Background()
		dbName := randomDBName()

		db, created, err := EnsureDB(ctx, dbName,
			WithDatabaseSettings("default",
				WithURL(urlWithoutDB),
				WithUser("$PG_TEST_USER"),
				WithPassword("$PG_TEST_PASSWORD"),
			),
		)
		t.Cleanup(func() {
			if db != nil {
				_ = db.Close()
			}
			_, _ = DropDB(ctx, dbName)
		})
		require.NoError(t, err)
		require.NotNil(t, db)
		require.True(t, created)
	})

	t.Run("with invalid user/password", func(t *testing.T) {
		ctx := context.Background()
		dbName := randomDBName()

		db, created, err := EnsureDB(ctx, dbName,
			WithDatabaseSettings("default",
				WithURL(urlWithoutDB),
				WithUser(pgUser),
				WithPassword("invalid"),
			),
		)
		t.Cleanup(func() {
			if db != nil {
				_ = db.Close()
			}
			_, _ = DropDB(ctx, dbName)
		})
		require.ErrorIs(t, err, ErrPGAuth)
		require.Nil(t, db)
		require.True(t, created)
	})
}

func randomDBName() string {
	n := rand.Intn(10000) //#nosec

	return fmt.Sprintf("unittest_rand_%d", n)
}
