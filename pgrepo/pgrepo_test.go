package pgrepo

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEnsureDB(t *testing.T) {
	t.Run("with default config", func(t *testing.T) {
		ctx := context.Background()

		t.Run("drop DB", func(t *testing.T) {
			_, err := DropDB(ctx, "unittest_db")
			require.NoError(t, err)
		})

		t.Run("ensure DB", func(t *testing.T) {
			db, _, err := EnsureDB(context.Background(), "unittest_db",
				WithDefaultPoolOptions(WithLogLevel("debug")),
			)
			require.NoError(t, err)
			require.NotNil(t, db)
			require.NoError(t, db.Close())
		})

		t.Run("drop DB", func(t *testing.T) {
			dropped, err := DropDB(ctx, "unittest_db")
			require.NoError(t, err)
			require.True(t, dropped)
		})
	})
}
