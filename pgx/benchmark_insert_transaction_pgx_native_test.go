package runner

import (
	"testing"
)

// These benchmarks compare the total cost of interpolating the SQL then
// executing the query on the same connection using a transaction.
// Both database/sql and jmoiron/sqlx can take advantage of a prepared
// statements.

func BenchmarkPgxNativeTransactedDat2(b *testing.B) {
	benchmarkPgxNativeTransactedDatN(b, 1, 2)
}

func BenchmarkPgxNativeTransactedDat4(b *testing.B) {
	benchmarkPgxNativeTransactedDatN(b, 1, 4)
}

func BenchmarkPgxNativeTransactedDat8(b *testing.B) {
	benchmarkPgxNativeTransactedDatN(b, 2, 4)
}

func benchmarkPgxNativeTransactedDatN(b *testing.B, rows int, argc int) {
	benchReset()
	builder, err := benchInsertBuilder(rows, argc)
	if err != nil {
		b.Fatal(err)
	}

	c, err := conn.DB.Acquire()
	if err != nil {
		b.Fatal(err)
	}
	defer conn.DB.Release(c)

	sql, args := builder.ToSQL()

	_, err = c.Prepare("doInsert", sql)
	if err != nil {
		b.Fatal(err)
	}
	defer c.Deallocate("doInsert")

	tx, err := c.Begin()
	if err != nil {
		b.Fatal(err)
	}
	defer tx.Commit()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err = c.Exec("doInsert", args...)
		if err != nil {
			//fmt.Println(builder)
			b.Fatal(err)
		}
	}
}
