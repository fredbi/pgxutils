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

		t.Run("drop DB", func(t *testing.T) {
			_, err := DropDB(ctx, "unittest_db",
				WithDatabaseSettings("default",
					WithPassword(pgPassword), // force password to avoid pollution by environment or a local password file
				),
			)
			require.NoError(t, err)
		})

		t.Run("ensure DB", func(t *testing.T) {
			const dbName = "unittest_db"

			db, created, err := EnsureDB(context.Background(), dbName,
				WithDefaultPoolOptions(WithLogLevel("debug")),
				WithDatabaseSettings("default",
					WithPassword(pgPassword),
				),
			)
			t.Cleanup(func() {
				_, _ = DropDB(ctx, "unittest_db")
			})
			require.NoError(t, err)
			require.True(t, created)
			require.NotNil(t, db)
			require.NoError(t, db.Close())

			db, created, err = EnsureDB(context.Background(), dbName,
				WithDefaultPoolOptions(WithLogLevel("debug")),
				WithDatabaseSettings("default",
					WithPassword(pgPassword),
				),
			)
			require.NoError(t, err)
			require.False(t, created)
			require.NotNil(t, db)
			require.NoError(t, db.Close())

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
