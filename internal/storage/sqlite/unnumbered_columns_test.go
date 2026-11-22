package sqlite

import (
	"context"
	"testing"
)

func episodeVersionRecorded(t *testing.T, s *Store) bool {
	t.Helper()
	var n int
	if err := s.db.QueryRowContext(context.Background(),
		"SELECT COUNT(*) FROM schema_migrations WHERE version = ?", episodeColumnsVersion).Scan(&n); err != nil {
		t.Fatalf("read schema_migrations: %v", err)
	}
	return n > 0
}

func TestMigrate_UnnumberedEpisodeColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t)
	migrateOrFatal(t, s)
	migrateOrFatal(t, s) // idempotent

	if episodeVersionRecorded(t, s) {
		t.Error("schema_migrations records the episode columns' version, want it left free")
	}
	if err := s.UpsertReactionFingerprint(ctx, "ISS-1", "merge-conflict", "fp"); err != nil {
		t.Fatalf("UpsertReactionFingerprint: %v", err)
	}
	if err := s.SetReactionEpisodes(ctx, "ISS-1", "merge-conflict", 2); err != nil {
		t.Fatalf("SetReactionEpisodes: %v", err)
	}
	if got, err := s.GetReactionEpisode(ctx, "ISS-1", "merge-conflict"); err != nil || got.Episodes != 2 {
		t.Errorf("GetReactionEpisode() = %+v, %v; want 2 episodes", got, err)
	}
}

// TestMigrate_AdoptsEarlierNumberedEpisodeColumns covers a database an
// earlier build migrated with the episode columns as a numbered migration:
// its version row goes, so this build does not refuse the database, and the
// column that build lacked is added.
func TestMigrate_AdoptsEarlierNumberedEpisodeColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t)
	migrateOrFatal(t, s)
	if _, err := s.db.ExecContext(ctx, "ALTER TABLE reaction_fingerprints DROP COLUMN episodes"); err != nil {
		t.Fatalf("drop episodes column: %v", err)
	}
	if _, err := s.db.ExecContext(ctx,
		"INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)", episodeColumnsVersion, "2026-11-22T00:00:00Z"); err != nil {
		t.Fatalf("record the earlier version: %v", err)
	}

	migrateOrFatal(t, s)

	if episodeVersionRecorded(t, s) {
		t.Error("earlier episode columns version still recorded, want it dropped")
	}
	if err := s.SetReactionEpisodes(ctx, "ISS-1", "merge-conflict", 1); err != nil {
		t.Errorf("SetReactionEpisodes after adoption: %v", err)
	}
}

// TestMigrate_RefusesNewerSchemaWithoutEpisodeColumns covers a version 55
// that is not the earlier build's: it stays, and the newer-schema check
// refuses the database.
func TestMigrate_RefusesNewerSchemaWithoutEpisodeColumns(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	s := openTestStore(t)
	migrateToVersion(t, s, 54)
	if _, err := s.db.ExecContext(ctx,
		"INSERT INTO schema_migrations (version, applied_at) VALUES (?, ?)", episodeColumnsVersion, "2026-11-22T00:00:00Z"); err != nil {
		t.Fatalf("record a newer version: %v", err)
	}

	if err := s.Migrate(ctx); err == nil {
		t.Fatal("Migrate() on a database with an unknown version 55 = nil, want the newer-schema refusal")
	}
	if !episodeVersionRecorded(t, s) {
		t.Error("unknown version 55 dropped, want it kept")
	}
}
