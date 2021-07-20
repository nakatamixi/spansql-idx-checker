package query

import (
	"fmt"
	"regexp"

	"cloud.google.com/go/spanner/spansql"
)

type Query struct {
	Table spansql.ID
	Where spansql.BoolExpr
}

var (
	rs = regexp.MustCompile(`(?i)^SELECT`)
	ru = regexp.MustCompile(`(?i)^UPDATE`)
	rd = regexp.MustCompile(`(?i)^DELETE`)
)

func NewQuery(s string) (*Query, error) {
	var query *Query
	switch {
	case rs.MatchString(s):
		q, err := spansql.ParseQuery(s)
		if err != nil {
			return nil, fmt.Errorf("cant parse query. query: %s err: %v", s, err)
		}
		if len(q.Select.From) > 1 {
			return nil, fmt.Errorf("does not support join query. %s", q.SQL())
		}
		if _, ok := q.Select.From[0].(spansql.SelectFromJoin); ok {
			return nil, fmt.Errorf("does not support join query. %s", q.SQL())
		}
		selectTable, ok := q.Select.From[0].(spansql.SelectFromTable)
		if !ok {
			return nil, fmt.Errorf("cant parse select query %s", q.SQL())
		}
		query = &Query{
			Table: selectTable.Table,
			Where: q.Select.Where,
		}
	case ru.MatchString(s), rd.MatchString(s):
		dml, err := spansql.ParseDMLStmt(s)
		if err != nil {
			return nil, fmt.Errorf("cant parse dml. dml: %s err: %v", s, err)
		}
		if u, ok := dml.(*spansql.Update); ok {
			query = &Query{
				Table: u.Table,
				Where: u.Where,
			}
		}
		if d, ok := dml.(*spansql.Delete); ok {
			query = &Query{
				Table: d.Table,
				Where: d.Where,
			}
		}
	default:
		return nil, fmt.Errorf("query needs to start with (SELECT|UPDATE|DELETE). query: %s", s)
	}
	return query, nil
}
