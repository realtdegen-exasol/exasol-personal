// Copyright 2026 Exasol AG
// SPDX-License-Identifier: MIT

package exasol

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/exasol/exasol-driver-go/pkg/connection"
	"github.com/exasol/exasol-personal/internal/connect/exasol/types"
	"github.com/exasol/exasol-personal/internal/connect/exasol/types/typesfakes"
	generaltypes "github.com/exasol/exasol-personal/internal/connect/types"
	"github.com/stretchr/testify/require"
)

// fakeSessionConn is a driver connection that answers table-existence queries
// and records every executed statement.
type tableAnswer struct {
	exists bool
	err    error
}

type fakeSessionConn struct {
	tableExists  bool
	laterAnswers []tableAnswer
	tableChecks  int
	failOn       string
	cancelAfter  string
	cancel       context.CancelFunc
	statements   []string
	tableQuery   []driver.NamedValue
	closed       bool
}

func (*fakeSessionConn) Prepare(string) (driver.Stmt, error) { return nil, errTest }

func (*fakeSessionConn) Begin() (driver.Tx, error) { return nil, errTest }

func (f *fakeSessionConn) Close() error {
	f.closed = true

	return nil
}

func (f *fakeSessionConn) Exec(query string, _ []driver.Value) (driver.Result, error) {
	f.statements = append(f.statements, query)

	return fakeResult{}, nil
}

func (f *fakeSessionConn) ExecContext(
	_ context.Context, query string, _ []driver.NamedValue,
) (driver.Result, error) {
	f.statements = append(f.statements, query)
	if f.failOn != "" && strings.HasPrefix(query, f.failOn) {
		return nil, errTest
	}
	if f.cancelAfter != "" && strings.HasPrefix(query, f.cancelAfter) {
		f.cancel()
	}

	return fakeResult{}, nil
}

func (f *fakeSessionConn) QueryContext(
	_ context.Context, query string, args []driver.NamedValue,
) (driver.Rows, error) {
	if !strings.Contains(query, "EXA_ALL_TABLES") {
		return nil, errTest
	}

	f.tableQuery = args
	f.tableChecks++
	exists := f.tableExists
	if f.tableChecks > 1 && len(f.laterAnswers) > 0 {
		answer := f.laterAnswers[0]
		f.laterAnswers = f.laterAnswers[1:]
		if answer.err != nil {
			return nil, answer.err
		}
		exists = answer.exists
	}
	count := int64(0)
	if exists {
		count = 1
	}

	return &fakeRows{columns: []string{"COUNT"}, data: [][]driver.Value{{count}}}, nil
}

type parquetImportCall struct {
	schema, table, filePath string
	options                 connection.ParquetImportOptions
}

type parquetImportHarness struct {
	session  *fakeSessionConn
	database *Database
	calls    []parquetImportCall
}

func newParquetImportHarness(t *testing.T, session types.ExasolConnector) *parquetImportHarness {
	t.Helper()

	harness := &parquetImportHarness{}
	if fake, ok := session.(*fakeSessionConn); ok {
		harness.session = fake
	}

	connect := func(string) (types.ExasolConnector, error) { return session, nil }
	database, ok := testDatabaseFactory(t, connect).(*Database)
	require.True(t, ok)
	require.NoError(t, database.Connect(t.Context()))

	database.importParquet = func(
		ctx context.Context,
		pool *sql.DB,
		schema, table, filePath string,
		options connection.ParquetImportOptions,
	) (int64, error) {
		harness.calls = append(harness.calls, parquetImportCall{schema, table, filePath, options})
		if _, err := pool.ExecContext(ctx, "IMPORT"); err != nil {
			return 0, err
		}

		return 3, nil
	}
	harness.database = database

	return harness
}

func writeParquetFile(t *testing.T) string {
	t.Helper()

	path := filepath.ToSlash(filepath.Join(t.TempDir(), "sales.parquet"))
	require.NoError(t, os.WriteFile(path, []byte("PAR1"), 0o600))

	return path
}

const createSalesFrom = "CREATE TABLE demo.sales AS (IMPORT FROM LOCAL PARQUET FILE '"

const replaceSalesFrom = "CREATE OR REPLACE TABLE demo.sales AS (IMPORT FROM LOCAL PARQUET FILE '"

func (h *parquetImportHarness) staging(t *testing.T) string {
	t.Helper()
	require.Len(t, h.calls, 1)
	require.True(t, strings.HasPrefix(h.calls[0].table, "PARQUET_IMPORT_"), h.calls[0].table)

	return `"DEMO"."` + h.calls[0].table + `"`
}

func TestExecCreatesTableFromLocalParquetFileOnTheSession(t *testing.T) {
	t.Parallel()

	// Given
	harness := newParquetImportHarness(t, &fakeSessionConn{})
	path := writeParquetFile(t)

	// When
	result, err := harness.database.Exec(t.Context(), createSalesFrom+path+"')", 0)

	// Then
	require.NoError(t, err)
	require.Equal(t, generaltypes.StatementTypeCreate, result.StatementType())
	require.Equal(t, int64(3), result.RowsAffected())
	staging := harness.staging(t)
	require.Equal(t, "DEMO", harness.calls[0].schema)
	require.Equal(t, path, harness.calls[0].filePath)
	require.Equal(
		t,
		connection.ParquetImportOptions{ColumnNames: connection.RawParquetNames},
		harness.calls[0].options,
	)
	require.Equal(t, "DEMO", harness.session.tableQuery[0].Value)
	require.Equal(t, "SALES", harness.session.tableQuery[1].Value)
	require.Equal(
		t,
		[]string{"IMPORT", "RENAME TABLE " + staging + ` TO "SALES"`},
		harness.session.statements,
	)
	require.False(t, harness.session.closed)
}

func TestExecRejectsExistingTableWithoutReplace(t *testing.T) {
	t.Parallel()

	// Given
	harness := newParquetImportHarness(t, &fakeSessionConn{tableExists: true})
	path := writeParquetFile(t)

	// When
	_, err := harness.database.Exec(t.Context(), createSalesFrom+path+"')", 0)

	// Then
	require.ErrorContains(t, err, `table "DEMO"."SALES" already exists`)
	require.Empty(t, harness.calls)
	require.Empty(t, harness.session.statements)
}

func TestExecReplacesExistingTableAfterImport(t *testing.T) {
	t.Parallel()

	// Given
	harness := newParquetImportHarness(t, &fakeSessionConn{tableExists: true})
	path := writeParquetFile(t)

	// When
	_, err := harness.database.Exec(t.Context(), replaceSalesFrom+path+"')", 0)

	// Then
	require.NoError(t, err)
	require.Equal(t, []string{
		"IMPORT",
		`DROP TABLE "DEMO"."SALES"`,
		"RENAME TABLE " + harness.staging(t) + ` TO "SALES"`,
	}, harness.session.statements)
}

func TestExecKeepsExistingTableWhenImportFails(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name      string
		statement string
		exists    bool
	}{
		{name: "create", statement: createSalesFrom},
		{name: "replace", statement: replaceSalesFrom, exists: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// Given
			harness := newParquetImportHarness(
				t, &fakeSessionConn{tableExists: test.exists, failOn: "IMPORT"},
			)
			path := writeParquetFile(t)

			// When
			_, err := harness.database.Exec(t.Context(), test.statement+path+"')", 0)

			// Then
			require.ErrorIs(t, err, errTest)
			require.Equal(t, []string{
				"IMPORT",
				"DROP TABLE IF EXISTS " + harness.staging(t),
			}, harness.session.statements)
		})
	}
}

func TestExecRemovesStagingTableWhenRenameFails(t *testing.T) {
	t.Parallel()

	// Given
	harness := newParquetImportHarness(t, &fakeSessionConn{failOn: "RENAME"})
	path := writeParquetFile(t)

	// When
	_, err := harness.database.Exec(t.Context(), createSalesFrom+path+"')", 0)

	// Then
	require.ErrorIs(t, err, errTest)
	staging := harness.staging(t)
	require.Equal(t, []string{
		"IMPORT",
		"RENAME TABLE " + staging + ` TO "SALES"`,
		"DROP TABLE IF EXISTS " + staging,
	}, harness.session.statements)
}

func TestExecKeepsImportedRowsWhenRenameFailsAfterReplace(t *testing.T) {
	t.Parallel()

	// Given
	harness := newParquetImportHarness(
		t, &fakeSessionConn{tableExists: true, failOn: "RENAME"},
	)
	path := writeParquetFile(t)

	// When
	_, err := harness.database.Exec(t.Context(), replaceSalesFrom+path+"')", 0)

	// Then
	require.ErrorIs(t, err, errTest)
	staging := harness.staging(t)
	require.ErrorContains(t, err, "the imported rows remain in table "+staging)
	require.NotContains(t, harness.session.statements, "DROP TABLE IF EXISTS "+staging)
}

func TestExecFinishesReplaceWhenCancelledAfterImport(t *testing.T) {
	t.Parallel()

	// Given
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	session := &fakeSessionConn{tableExists: true, cancelAfter: "IMPORT", cancel: cancel}
	harness := newParquetImportHarness(t, session)
	path := writeParquetFile(t)

	// When
	_, err := harness.database.Exec(ctx, replaceSalesFrom+path+"')", 0)

	// Then
	require.NoError(t, err)
	require.Equal(t, []string{
		"IMPORT",
		`DROP TABLE "DEMO"."SALES"`,
		"RENAME TABLE " + harness.staging(t) + ` TO "SALES"`,
	}, session.statements)
}

func TestExecReconcilesFailedDropOfExistingTable(t *testing.T) {
	t.Parallel()

	const dropTarget = `DROP TABLE "DEMO"."SALES"`

	for _, test := range []struct {
		name          string
		answer        tableAnswer
		expectedError string
		expectedLast  func(staging string) string
	}{
		{
			name:          "target still exists",
			answer:        tableAnswer{exists: true},
			expectedError: errTest.Error(),
			expectedLast:  func(staging string) string { return "DROP TABLE IF EXISTS " + staging },
		},
		{
			name:   "drop was committed",
			answer: tableAnswer{exists: false},
			expectedLast: func(staging string) string {
				return "RENAME TABLE " + staging + ` TO "SALES"`
			},
		},
		{
			name:          "target state unknown",
			answer:        tableAnswer{err: errors.New("connection lost")},
			expectedError: "the imported rows remain in table",
			expectedLast:  func(string) string { return dropTarget },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// Given
			session := &fakeSessionConn{
				tableExists:  true,
				failOn:       dropTarget,
				laterAnswers: []tableAnswer{test.answer},
			}
			harness := newParquetImportHarness(t, session)
			path := writeParquetFile(t)

			// When
			_, err := harness.database.Exec(t.Context(), replaceSalesFrom+path+"')", 0)

			// Then
			if test.expectedError == "" {
				require.NoError(t, err)
			} else {
				require.ErrorContains(t, err, test.expectedError)
			}
			staging := harness.staging(t)
			require.Equal(
				t,
				test.expectedLast(staging),
				session.statements[len(session.statements)-1],
			)
			require.Equal(t, 2, session.tableChecks)
		})
	}
}

func TestExecLeavesTableUntouchedForUnusablePath(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name          string
		path          string
		expectedError string
	}{
		{
			name:          "missing file",
			path:          "/does/not/exist.parquet",
			expectedError: "cannot open local Parquet file",
		},
		{
			name:          "backslash separator",
			path:          `C:\data\sales.parquet`,
			expectedError: "use / as the separator",
		},
		{
			name:          "parentheses",
			path:          "/data/sales(2024).parquet",
			expectedError: "may only contain ASCII letters",
		},
		{
			name:          "non-ASCII letter",
			path:          "/home/j\u00fcrgen/sales.parquet",
			expectedError: "may only contain ASCII letters",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			// Given
			harness := newParquetImportHarness(t, &fakeSessionConn{tableExists: true})

			// When
			_, err := harness.database.Exec(t.Context(), replaceSalesFrom+test.path+"')", 0)

			// Then
			require.ErrorContains(t, err, test.expectedError)
			require.Empty(t, harness.calls)
			require.Empty(t, harness.session.statements)
		})
	}
}

func TestExecRejectsUnsupportedLocalParquetTableShape(t *testing.T) {
	t.Parallel()

	// Given
	harness := newParquetImportHarness(t, &fakeSessionConn{})

	// When
	_, err := harness.database.Exec(
		t.Context(),
		"CREATE TABLE s.t AS SELECT id FROM (IMPORT FROM LOCAL PARQUET FILE '/f.parquet')",
		0,
	)

	// Then
	require.ErrorIs(t, err, generaltypes.ErrUnsupportedLocalParquetTable)
	require.Empty(t, harness.calls)
	require.Empty(t, harness.session.statements)
}

func TestExecRequiresSessionConnectionForLocalParquetTable(t *testing.T) {
	t.Parallel()

	// Given
	fakeConnector := &typesfakes.FakeExasolConnector{}
	fakeConnector.QueryContextReturns(nil, errTest)
	harness := newParquetImportHarness(t, fakeConnector)
	path := writeParquetFile(t)

	// When
	_, err := harness.database.Exec(
		t.Context(),
		"CREATE TABLE demo.sales AS (IMPORT FROM LOCAL PARQUET FILE '"+path+"')",
		0,
	)

	// Then
	require.ErrorIs(t, err, errNoSessionConnection)
	require.Empty(t, harness.calls)
	require.Zero(t, fakeConnector.ExecCallCount())
}
