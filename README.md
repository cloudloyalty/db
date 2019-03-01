Go PostgreSQL Toolkit
=====================

*db.go* - query formatter

*migrate.go* - schema migration helpers

Migrations usage example:
```go

var migrations = []db.Migration{
	{
		1,
		db.InitialMigration,
	},
	{
		2,
		`
CREATE TABLE test (
    id   BIGSERIAL NOT NULL PRIMARY KEY,
    text TEXT
);
`,
	},
}

dbh, err := sql.Open("postgres", config.DB.DSN)
if err != nil {
	panic(err)
}

err = db.NewMigrate(dbh).Run(migrations)
if err != nil {
  panic(err)
}
```

Formatter usage example:
```go
// run a query with param substitution
// in order to use db.ScanRowsIntoStruct() you have to format query result in JSON
rows, err := db.Query(dbh, `SELECT row_to_json(t.*) FROM test AS t WHERE id = :id`, db.Params{"id": 1})
if err != nil && err != sql.ErrNoRows {
  panic(err)
}

// a struct for filling up with select data
struct testModel {
  id int
}

var tests []testModel
// iterate over results and build slice of structs
for rows.Next() {
  var test testModel
  if err := db.ScanRowsIntoStruct(rows, &test); err != nil {
    panic(err)
  }
  tests = append(tests, test)
}
```
