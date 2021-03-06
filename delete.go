package dat

import "bytes"

// DeleteBuilder contains the clauses for a DELETE statement
type DeleteBuilder struct {
	Execer

	table          string
	whereFragments []*whereFragment
	orderBys       []string
	limitCount     uint64
	limitValid     bool
	offsetCount    uint64
	offsetValid    bool
	id             int
}

// NewDeleteBuilder creates a new DeleteBuilder for the given table.
func NewDeleteBuilder(table string) *DeleteBuilder {
	return &DeleteBuilder{table: table}
}

// Where appends a WHERE clause to the statement whereSqlOrMap can be a
// string or map. If it's a string, args wil replaces any places holders
func (b *DeleteBuilder) Where(whereSqlOrMap interface{}, args ...interface{}) *DeleteBuilder {
	b.whereFragments = append(b.whereFragments, newWhereFragment(whereSqlOrMap, args))
	return b
}

// OrderBy appends an ORDER BY clause to the statement
func (b *DeleteBuilder) OrderBy(ord string) *DeleteBuilder {
	b.orderBys = append(b.orderBys, ord)
	return b
}

// Limit sets a LIMIT clause for the statement; overrides any existing LIMIT
func (b *DeleteBuilder) Limit(limit uint64) *DeleteBuilder {
	b.limitCount = limit
	b.limitValid = true
	return b
}

// Offset sets an OFFSET clause for the statement; overrides any existing OFFSET
func (b *DeleteBuilder) Offset(offset uint64) *DeleteBuilder {
	b.offsetCount = offset
	b.offsetValid = true
	return b
}

// ToSQL serialized the DeleteBuilder to a SQL string
// It returns the string with placeholders and a slice of query arguments
func (b *DeleteBuilder) ToSQL() (string, []interface{}) {
	if len(b.table) == 0 {
		panic("no table specified")
	}

	var sql bytes.Buffer
	var args []interface{}

	sql.WriteString("DELETE FROM ")
	sql.WriteString(b.table)

	var placeholderStartPos int64 = 1

	// Write WHERE clause if we have any fragments
	if len(b.whereFragments) > 0 {
		sql.WriteString(" WHERE ")
		writeWhereFragmentsToSql(b.whereFragments, &sql, &args, &placeholderStartPos)
	}

	// Ordering and limiting
	if len(b.orderBys) > 0 {
		sql.WriteString(" ORDER BY ")
		for i, s := range b.orderBys {
			if i > 0 {
				sql.WriteString(", ")
			}
			sql.WriteString(s)
		}
	}

	if b.limitValid {
		sql.WriteString(" LIMIT ")
		writeUint64(&sql, b.limitCount)
	}

	if b.offsetValid {
		sql.WriteString(" OFFSET ")
		writeUint64(&sql, b.offsetCount)
	}

	return sql.String(), args
}

// Interpolate interpolates this builder's SQL.
func (b *DeleteBuilder) Interpolate() (string, []interface{}, error) {
	return interpolate(b)
}

// MustInterpolate interpolates this builder's SQL or panics.
func (b *DeleteBuilder) MustInterpolate() (string, []interface{}) {
	return mustInterpolate(b)
}
