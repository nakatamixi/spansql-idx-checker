package query_test

import (
	"testing"

	"cloud.google.com/go/spanner/spansql"
	"github.com/nakatamixi/spansql-idx-checker/query"
	"github.com/stretchr/testify/require"
)

func TestQuery_NewQuery(t *testing.T) {
	t.Run("SELECT", func(t *testing.T) {
		t.Run("valid", func(t *testing.T) {
			t.Run("WHERE TRUE", func(t *testing.T) {
				q, err := query.NewQuery("SELECT * FROM test_table WHERE TRUE")
				require.NoError(t, err)
				require.Equal(t, spansql.ID("test_table"), q.Table)
				w, ok := q.Where.(spansql.BoolLiteral)
				require.True(t, ok)
				require.Equal(t, w, spansql.True)
			})
			t.Run("where true lower case", func(t *testing.T) {
				q, err := query.NewQuery("select * from test_table where true")
				require.NoError(t, err)
				require.Equal(t, spansql.ID("test_table"), q.Table)
				w, ok := q.Where.(spansql.BoolLiteral)
				require.True(t, ok)
				require.Equal(t, w, spansql.True)
			})
			t.Run("WHERE eq", func(t *testing.T) {
				q, err := query.NewQuery("SELECT * FROM test_table WHERE column = @column")
				require.NoError(t, err)
				require.Equal(t, spansql.ID("test_table"), q.Table)
				w, ok := q.Where.(spansql.ComparisonOp)
				require.True(t, ok)
				require.Equal(t, w, spansql.ComparisonOp{Op: spansql.Eq, LHS: spansql.ID("column"), RHS: spansql.Param("column")})
			})
		})
		t.Run("invalid", func(t *testing.T) {
			_, err := query.NewQuery("SELECT * invalid")
			require.Error(t, err)
			// TODO checker doesnt support join
			_, err = query.NewQuery("SELECT * FROM a JOIN b ON (a_col = b_col) WHERE column = @column")
			require.Error(t, err)
		})
	})
	t.Run("UPDATE", func(t *testing.T) {
		t.Run("valid", func(t *testing.T) {
			t.Run("WHERE TRUE", func(t *testing.T) {
				q, err := query.NewQuery("UPDATE test_table SET column = @column WHERE TRUE")
				require.NoError(t, err)
				require.Equal(t, spansql.ID("test_table"), q.Table)
				w, ok := q.Where.(spansql.BoolLiteral)
				require.True(t, ok)
				require.Equal(t, w, spansql.True)
			})
			t.Run("where true lower case", func(t *testing.T) {
				q, err := query.NewQuery("update test_table set column = @column where true")
				require.NoError(t, err)
				require.Equal(t, spansql.ID("test_table"), q.Table)
				w, ok := q.Where.(spansql.BoolLiteral)
				require.True(t, ok)
				require.Equal(t, w, spansql.True)
			})
			t.Run("WHERE eq", func(t *testing.T) {
				q, err := query.NewQuery("UPDATE test_table SET test = @test WHERE column = @column")
				require.NoError(t, err)
				require.Equal(t, spansql.ID("test_table"), q.Table)
				w, ok := q.Where.(spansql.ComparisonOp)
				require.True(t, ok)
				require.Equal(t, w, spansql.ComparisonOp{Op: spansql.Eq, LHS: spansql.ID("column"), RHS: spansql.Param("column")})
			})
		})
		t.Run("invalid", func(t *testing.T) {
			_, err := query.NewQuery("UPDATE invalid")
			require.Error(t, err)
		})
	})
	t.Run("DELETE", func(t *testing.T) {
		t.Run("valid", func(t *testing.T) {
			t.Run("WHERE TRUE", func(t *testing.T) {
				q, err := query.NewQuery("DELETE FROM test_table WHERE TRUE")
				require.NoError(t, err)
				require.Equal(t, spansql.ID("test_table"), q.Table)
				w, ok := q.Where.(spansql.BoolLiteral)
				require.True(t, ok)
				require.Equal(t, w, spansql.True)
			})
			t.Run("where true lower case", func(t *testing.T) {
				q, err := query.NewQuery("delete from test_table where true")
				require.NoError(t, err)
				require.Equal(t, spansql.ID("test_table"), q.Table)
				w, ok := q.Where.(spansql.BoolLiteral)
				require.True(t, ok)
				require.Equal(t, w, spansql.True)
			})
			t.Run("WHERE eq", func(t *testing.T) {
				q, err := query.NewQuery("DELETE FROM test_table WHERE column = @column")
				require.NoError(t, err)
				require.Equal(t, spansql.ID("test_table"), q.Table)
				w, ok := q.Where.(spansql.ComparisonOp)
				require.True(t, ok)
				require.Equal(t, w, spansql.ComparisonOp{Op: spansql.Eq, LHS: spansql.ID("column"), RHS: spansql.Param("column")})
			})
		})
		t.Run("invalid", func(t *testing.T) {
			_, err := query.NewQuery("DELETE * invalid")
			require.Error(t, err)
		})
	})
}
