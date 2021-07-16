package query

import (
	"fmt"
	"strings"

	"cloud.google.com/go/spanner/spansql"
)

type Query struct {
	Table        spansql.ID
	WhereColumns []spansql.ID
}

func NewQuery(s string) (*Query, error) {
	var query *Query
	switch {
	case strings.HasPrefix(s, "SELECT"):
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
			Table:        selectTable.Table,
			WhereColumns: whereColumns(q.Select.Where),
		}
	case strings.HasPrefix(s, "UPDATE"), strings.HasPrefix(s, "DELETE"):
		dml, err := spansql.ParseDMLStmt(s)
		if err != nil {
			return nil, fmt.Errorf("cant parse dml. dml: %s err: %v", s, err)
		}
		if u, ok := dml.(*spansql.Update); ok {
			query = &Query{
				Table:        u.Table,
				WhereColumns: whereColumns(u.Where),
			}
		}
		if d, ok := dml.(*spansql.Delete); ok {
			query = &Query{
				Table:        d.Table,
				WhereColumns: whereColumns(d.Where),
			}
		}
	default:
		return nil, fmt.Errorf("query needs to start with (SELECT|UPDATE|DELETE). query: %s", s)
	}
	return query, nil
}
func whereColumns(where interface{}) []spansql.ID {
	switch op := where.(type) {
	case spansql.LogicalOp:
		lhs := whereColumns(op.LHS)
		rhs := whereColumns(op.RHS)
		return append(lhs, rhs...)
	case spansql.ComparisonOp:
		lhs := whereColumns(op.LHS)
		rhs := whereColumns(op.RHS)
		return append(lhs, rhs...)
	case spansql.Paren:
		return whereColumns(op.Expr)
	case spansql.InOp:
		lhs := whereColumns(op.LHS)
		rhs := whereColumns(op.RHS)
		return append(lhs, rhs...)
	case spansql.ID:
		return []spansql.ID{op}
	}
	return []spansql.ID{}
}
