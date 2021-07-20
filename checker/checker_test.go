package checker_test

import (
	_ "embed"
	"strings"
	"testing"

	"cloud.google.com/go/spanner/spansql"
	"github.com/nakatamixi/spansql-idx-checker/checker"
	"github.com/nakatamixi/spansql-idx-checker/query"
	"github.com/stretchr/testify/require"
)

//go:embed testdata/spanner.sql
var b []byte

func TestChecker_Check(t *testing.T) {
	ddl, err := schema()
	require.NoError(t, err)
	checker := checker.NewChecker(ddl)
	t.Run("include pk first key", func(t *testing.T) {
		q, err := query.NewQuery("SELECT * FROM test WHERE pk_first = @pk_first")
		require.NoError(t, err)
		ok, err := checker.Check(q)
		require.NoError(t, err)
		require.True(t, ok)
		q, err = query.NewQuery("SELECT * FROM test WHERE @pk_first = pk_first")
		require.NoError(t, err)
		ok, err = checker.Check(q)
		require.NoError(t, err)
		require.True(t, ok)
		q, err = query.NewQuery("SELECT * FROM test WHERE pk_first IN (@pk_first1, @pk_first2)")
		require.NoError(t, err)
		ok, err = checker.Check(q)
		require.NoError(t, err)
		require.True(t, ok)
		q, err = query.NewQuery("SELECT * FROM test WHERE @pk_first = pk_first AND no_idx = @no_idx")
		require.NoError(t, err)
		ok, err = checker.Check(q)
		require.NoError(t, err)
		require.True(t, ok)
		q, err = query.NewQuery("SELECT * FROM test WHERE pk_first < @pk_first")
		require.NoError(t, err)
		ok, err = checker.Check(q)
		require.NoError(t, err)
		require.True(t, ok)
	})
	t.Run("include idx first key", func(t *testing.T) {
		q, err := query.NewQuery("SELECT * FROM test WHERE idx_first = @idx_first")
		require.NoError(t, err)
		ok, err := checker.Check(q)
		require.NoError(t, err)
		require.True(t, ok)
		q, err = query.NewQuery("SELECT * FROM test WHERE @idx_first = idx_first")
		require.NoError(t, err)
		ok, err = checker.Check(q)
		require.NoError(t, err)
		require.True(t, ok)
		q, err = query.NewQuery("SELECT * FROM test WHERE idx_first IN (@idx_first1, @idx_first2)")
		require.NoError(t, err)
		ok, err = checker.Check(q)
		require.NoError(t, err)
		require.True(t, ok)
		q, err = query.NewQuery("SELECT * FROM test WHERE @idx_first = idx_first AND no_idx = @no_idx")
		require.NoError(t, err)
		ok, err = checker.Check(q)
		require.NoError(t, err)
		require.True(t, ok)
		q, err = query.NewQuery("SELECT * FROM test WHERE no_idx = @no_idx AND @idx_first = idx_first")
		require.NoError(t, err)
		ok, err = checker.Check(q)
		require.NoError(t, err)
		require.True(t, ok)
		q, err = query.NewQuery("SELECT * FROM test WHERE idx_first < @idx_first")
		require.NoError(t, err)
		ok, err = checker.Check(q)
		require.NoError(t, err)
		require.True(t, ok)
		q, err = query.NewQuery("SELECT * FROM test WHERE pk_first = @pk_first OR @idx_first = idx_first")
		require.NoError(t, err)
		ok, err = checker.Check(q)
		require.NoError(t, err)
		require.True(t, ok)
		q, err = query.NewQuery("SELECT * FROM test WHERE (pk_first = @pk_first AND pk_second = @pk_second) OR (idx_first = @idx_first AND pk_second = @pk_second)")
		require.NoError(t, err)
		ok, err = checker.Check(q)
		require.NoError(t, err)
		require.True(t, ok)
	})
	t.Run("not include first key", func(t *testing.T) {
		q, err := query.NewQuery("SELECT * FROM test WHERE pk_second = @pk_second")
		require.NoError(t, err)
		ok, err := checker.Check(q)
		require.NoError(t, err)
		require.False(t, ok)
		q, err = query.NewQuery("SELECT * FROM test WHERE idx_second = @idx_second")
		require.NoError(t, err)
		ok, err = checker.Check(q)
		require.NoError(t, err)
		require.False(t, ok)
		q, err = query.NewQuery("SELECT * FROM test WHERE not_idx = @not_idx")
		require.NoError(t, err)
		ok, err = checker.Check(q)
		require.NoError(t, err)
		require.False(t, ok)
		q, err = query.NewQuery("SELECT * FROM test WHERE pk_first = @pk_first OR pk_second = @pk_second")
		require.NoError(t, err)
		ok, err = checker.Check(q)
		require.NoError(t, err)
		require.False(t, ok)
		q, err = query.NewQuery("SELECT * FROM test WHERE (pk_first = @pk_first OR pk_second = @pk_second) AND (idx_first = @idx_first OR pk_second = @pk_second)")
		require.NoError(t, err)
		ok, err = checker.Check(q)
		require.NoError(t, err)
		require.False(t, ok)
	})
}
func schema() (*spansql.DDL, error) {
	body := string(b)
	// spansql not allow backquote
	ddls := strings.Replace(body, "`", "", -1)
	d, err := spansql.ParseDDL("", ddls)
	if err != nil {
		return nil, err
	}
	return d, nil

}
