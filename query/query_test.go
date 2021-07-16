package query_test

import (
	"testing"

	"cloud.google.com/go/spanner/spansql"
	"github.com/stretchr/testify/require"
	"github.com/nakatamixi/spansql-idx-checker/query"
)

func TestQuery_NewQuery(t *testing.T) {
	t.Run("SELECT", func(t *testing.T) {
		t.Run("valid", func(t *testing.T) {
			q, err := query.NewQuery("SELECT * FROM test_table WHERE TRUE")
			require.NoError(t, err)
			require.Equal(t, spansql.ID("test_table"), q.Table)
			require.Equal(t, []spansql.ID{}, q.WhereColumns)
			q, err = query.NewQuery("SELECT * FROM test_table WHERE column = @column")
			require.NoError(t, err)
			require.Equal(t, spansql.ID("test_table"), q.Table)
			require.Equal(t, []spansql.ID{"column"}, q.WhereColumns)
		})
		t.Run("invalid", func(t *testing.T) {
			_, err := query.NewQuery("SELECT * invalid")
			require.Error(t, err)
			// TODO spansql doesnt support lower case
			_, err = query.NewQuery("select * from test where true")
			require.Error(t, err)
			// TODO checker doesnt support join
			_, err = query.NewQuery("SELECT * FROM a JOIN b ON (a_col = b_col) WHERE column = @column")
			require.Error(t, err)
		})
	})
	t.Run("UPDATE", func(t *testing.T) {
		t.Run("valid", func(t *testing.T) {
			q, err := query.NewQuery("UPDATE test_table SET column = @column WHERE TRUE")
			require.NoError(t, err)
			require.Equal(t, spansql.ID("test_table"), q.Table)
			require.Equal(t, []spansql.ID{}, q.WhereColumns)
			q, err = query.NewQuery("UPDATE test_table SET test = @test WHERE column = @column")
			require.NoError(t, err)
			require.Equal(t, spansql.ID("test_table"), q.Table)
			require.Equal(t, []spansql.ID{"column"}, q.WhereColumns)
		})
		t.Run("invalid", func(t *testing.T) {
			_, err := query.NewQuery("UPDATE invalid")
			require.Error(t, err)
			// spansql doesnt support lower case
			_, err = query.NewQuery("update test_table set test = @test where TRUE")
			require.Error(t, err)
		})
	})
	t.Run("DELETE", func(t *testing.T) {
		t.Run("valid", func(t *testing.T) {
			q, err := query.NewQuery("DELETE FROM test_table WHERE TRUE")
			require.NoError(t, err)
			require.Equal(t, spansql.ID("test_table"), q.Table)
			require.Equal(t, []spansql.ID{}, q.WhereColumns)
			q, err = query.NewQuery("DELETE FROM test_table WHERE column = @column")
			require.NoError(t, err)
			require.Equal(t, spansql.ID("test_table"), q.Table)
			require.Equal(t, []spansql.ID{"column"}, q.WhereColumns)
		})
		t.Run("invalid", func(t *testing.T) {
			_, err := query.NewQuery("DELETE * invalid")
			require.Error(t, err)
			// spansql doesnt support lower case
			_, err = query.NewQuery("delete from test where TRUE")
			require.Error(t, err)
		})
	})
}
