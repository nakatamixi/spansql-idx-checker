package checker

import (
	"errors"

	"cloud.google.com/go/spanner/spansql"
	"github.com/nakatamixi/spansql-idx-checker/query"
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
	whereColumns := query.WhereColumns
	pks := []spansql.ID{}
	for _, key := range table.def.PrimaryKey {
		pks = append(pks, key.Column)
	}

	if checkKeyIncluded(pks[0], whereColumns) {
		return true, nil
	}

	if len(table.indices) == 0 {
		return false, nil
	}

	for _, index := range table.indices {
		keys := []spansql.ID{}
		if len(index.Columns) == 0 {
			continue
		}
		for _, key := range index.Columns {
			keys = append(keys, key.Column)
		}
		if checkKeyIncluded(keys[0], whereColumns) {
			return true, nil
		}
	}
	return false, nil
}

func checkKeyIncluded(key spansql.ID, whereColumns []spansql.ID) bool {
	for _, col := range whereColumns {
		if key == col {
			return true
		}
	}
	return false
}
