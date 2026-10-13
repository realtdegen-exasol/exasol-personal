## Context

The database cannot infer the schema of a client-local file. exasol-driver-go 1.2.0 adds
`ImportParquetWithInferredSchema`, which reads the Parquet metadata, creates the table, and imports
the file. It only accepts unquoted schema and table names, requires a schema, and imports into an
existing table with only a logged warning. Its import accepts only ASCII letters, digits, spaces
and `: < > / . _ - ~` in file paths, and a failed import leaves the created table behind.

## Decisions

**Recognize one statement shape on tokens.** The statement is tokenized with comments removed and
string literals and quoted identifiers kept as single tokens, so keywords inside comments or
literals never match. Only the exact `CREATE [OR REPLACE] TABLE <schema>.<table> AS (IMPORT FROM
LOCAL PARQUET FILE '<path>')` shape is routed to the driver. A statement starting with
`CREATE [OR REPLACE] TABLE` that contains `LOCAL PARQUET` in any other shape fails before anything
is sent, naming the supported form. Everything else keeps its existing execution path.

**Run on the user's session.** The driver function takes a `*sql.DB`. Instead of opening a second
session, the launcher wraps its open connection in a single-connection `*sql.DB` whose close leaves
the connection open. The created table and the import therefore join the session's transaction and
see its uncommitted work, which removes the "separate session" risk listed in the epic.

**Keep SQL semantics the driver does not provide.** Unquoted names are upper-cased before they are
passed on, because the driver quotes them. Plain `CREATE TABLE` fails when the table exists instead
of importing into it. Paths the driver cannot import are rejected up front with an error naming the
supported characters.

**Import into a staging table.** The driver imports into a uniquely named table in the target
schema. Only after the import succeeds is an existing table dropped (for `OR REPLACE`) and the
staging table renamed to the target. A failed import drops the staging table, so the user's table
is never left empty or half-filled.

## Risks / Trade-offs

- With autocommit on, a failure between dropping the old table and the rename leaves the imported
  rows in the staging table, whose name the error reports.
- Column sizes computed by the driver may differ from the database's inference for the same file in
  object storage.
- Windows paths need `/` separators, and paths with other characters such as `(` or non-ASCII
  letters are rejected until the driver accepts them.
- Subselects such as `CREATE TABLE ... AS SELECT ... FROM (IMPORT FROM LOCAL PARQUET ...)` stay
  unsupported until the driver detects local imports inside subselects; an upstream proposal covers
  that.
