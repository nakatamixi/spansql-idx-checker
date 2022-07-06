package checker

import (
	"errors"

	"cloud.google.com/go/spanner/spansql"
	"github.com/nktks/spansql-idx-checker/query"
)

type Checker struct {
	tables map[spansql.ID]table
}

type table struct {
	def     *spansql.CreateTable
	indices []*spansql.CreateIndex
}

func NewChecker(ddl *spansql.DDL) *Checker {
	ts := map[spansql.ID]table{}
	for _, e := range ddl.List {
		switch v := e.(type) {
		case *spansql.CreateTable:
			t := ts[v.Name]
			t.def = v
			ts[v.Name] = t
		case *spansql.CreateIndex:
			t := ts[v.Table]
			t.indices = append(t.indices, v)
			ts[v.Table] = t
		}
	}
	return &Checker{
		tables: ts,
	}
}

func (c *Checker) Check(query *query.Query) (bool, error) {
	name := query.Table
	table, ok := c.tables[name]
	if !ok {
		return false, errors.New("table not found in schema.")
	}
	where := query.Where
	indices := [][]spansql.KeyPart{table.def.PrimaryKey}

	for _, index := range table.indices {
		indices = append(indices, index.Columns)
	}
	return checkKeyIncluded(indices, where), nil

}

func checkKeyIncluded(indices [][]spansql.KeyPart, where interface{}) bool {
	switch op := where.(type) {
	case spansql.LogicalOp:
		switch op.Op {
		case spansql.And:
			return checkKeyIncluded(indices, op.RHS) || checkKeyIncluded(indices, op.LHS)
		case spansql.Or:
			return checkKeyIncluded(indices, op.RHS) && checkKeyIncluded(indices, op.LHS)
		case spansql.Not:
			return checkKeyIncluded(indices, op.RHS)
		}
	case spansql.ComparisonOp:
		return checkKeyIncluded(indices, op.RHS) || checkKeyIncluded(indices, op.LHS)
	case spansql.Paren:
		return checkKeyIncluded(indices, op.Expr)
	case spansql.InOp:
		return checkKeyIncluded(indices, op.RHS) || checkKeyIncluded(indices, op.LHS)
	case spansql.ID:
		for _, index := range indices {
			keys := []spansql.ID{}
			if len(index) == 0 {
				continue
			}
			for _, key := range index {
				keys = append(keys, key.Column)
			}
			if keys[0] == op {
				return true
			}
		}
		return false
	}
	return false
}
