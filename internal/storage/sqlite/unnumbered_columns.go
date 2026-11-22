package sqlite

import (
	"context"
	"fmt"
)

// episodeColumnsVersion is the migration number an earlier build of this
// branch recorded for the reaction episode columns. This build adds those
// columns outside the numbered sequence instead, so the number stays free
// for the upstream sequence.
const episodeColumnsVersion = 55

// addUnnumberedColumns adds, after the numbered migrations, each reaction
// episode column the database lacks. It records no version, so a later
// numbered migration of the same number still applies, and it is a no-op
// on a database that has them.
func (s *Store) addUnnumberedColumns(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin reaction episode columns: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // rollback on error path; no-op after commit
	if err := addReactionEpisodeColumns(ctx, tx); err != nil {
		return fmt.Errorf("add reaction episode columns: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit reaction episode columns: %w", err)
	}
	return nil
}

// dropEarlierEpisodeVersion removes the version row an earlier build of this
// branch recorded for the reaction episode columns, so this build, whose
// numbered sequence ends at maxRegistered, does not refuse the database as
// newer. It acts only when that version is past maxRegistered and the
// columns the earlier build added are present; any other database is left
// for the newer-schema check.
func (s *Store) dropEarlierEpisodeVersion(ctx context.Context, maxRegistered int) error {
	if maxRegistered >= episodeColumnsVersion {
		return nil
	}
	var recorded int
	if err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM schema_migrations WHERE version = ?", episodeColumnsVersion).Scan(&recorded); err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}
	if recorded == 0 {
		return nil
	}
	var newer int
	if err := s.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM schema_migrations WHERE version > ?", episodeColumnsVersion).Scan(&newer); err != nil {
		return fmt.Errorf("read schema_migrations: %w", err)
	}
	if newer > 0 {
		return nil
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin schema_migrations cleanup: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // rollback on error path; no-op after commit
	cols, err := columnNames(ctx, tx, "reaction_fingerprints")
	if err != nil {
		return err
	}
	for _, name := range []string{"attempts", "last_exit"} {
		if _, ok := cols[name]; !ok {
			return nil
		}
	}
	if _, err := tx.ExecContext(ctx, "DELETE FROM schema_migrations WHERE version = ?", episodeColumnsVersion); err != nil {
		return fmt.Errorf("drop earlier reaction episode version: %w", err)
	}
	return tx.Commit()
}
