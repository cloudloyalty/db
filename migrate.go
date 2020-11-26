package db

import (
	"context"
	"database/sql"
	"strings"
)

const InitialMigration = `
CREATE TABLE IF NOT EXISTS migrations (version INT NOT NULL PRIMARY KEY);
INSERT INTO migrations VALUES (1);
`

type Migrate struct {
	db *sql.DB
}

type Migration struct {
	Version int
	Sql     string
}

func NewMigrate(db *sql.DB) *Migrate {
	return &Migrate{
		db: db,
	}
}

func (m *Migrate) Run(migrations []Migration) error {
	latest, err := m.getLatestVersion()
	if err != nil {
		return err
	}

	tx, err := m.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	ctx := context.Background()

	for _, m := range migrations {
		if err != nil {
			break
		}
		if m.Version > latest {
			_, err = tx.Exec(m.Sql)
			latest = m.Version
		}
	}

	if err == nil {
		_, err := Exec(ctx, tx, "UPDATE migrations SET version = :latest", Params{"latest": latest})
		if err != nil {
			return err
		}
		tx.Commit()
	}

	return err
}

func (m *Migrate) getLatestVersion() (int, error) {
	var latest int
	row := m.db.QueryRow("SELECT version FROM migrations")
	err := row.Scan(&latest)
	if err != nil && err != sql.ErrNoRows && !strings.Contains(err.Error(), "migrations") {
		return 0, err
	}
	return latest, nil
}
