package files

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/moov-io/base/telemetry"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// Repository persists the file index and the staged-reception ledger.
type Repository interface {
	// File index

	// Record inserts a file index row. Rows are created unpublished
	// (PublishedAt nil); legacy backfill may insert an already-published row.
	Record(ctx context.Context, file AcceptedFile) error
	Cleanup(ctx context.Context, file AcceptedFile) error

	// Cancel marks a published file canceled. Unpublished or already-canceled
	// rows are left untouched (callers persist cancel intent separately).
	Cancel(ctx context.Context, fileID string) error
	GetFile(ctx context.Context, fileID string) (*AcceptedFile, error)
	MarkFilePublished(ctx context.Context, fileID string, at time.Time) error
	ListPublishedFileIDs(ctx context.Context) (map[string]struct{}, error)

	// Reception ledger

	// RecordReception durably records the reception fact. It is idempotent:
	// an existing row for commitID is never overwritten.
	RecordReception(ctx context.Context, reception Reception) error
	// RecordPublishedReception backfills a ledger row already in published
	// state (startup recovery of pre-upgrade mergable files). Existing rows
	// win.
	RecordPublishedReception(ctx context.Context, reception Reception) error
	GetReception(ctx context.Context, commitID string) (*Reception, error)

	MarkStaged(ctx context.Context, commitID, contentSHA256 string, at time.Time) error
	MarkIndexed(ctx context.Context, commitID string, at time.Time) error

	// MarkPublished publishes the ledger row and the file index row together,
	// freezing the serialized reception result.
	MarkPublished(ctx context.Context, commitID, fileID string, resultJSON []byte, at time.Time) error
	MarkNotified(ctx context.Context, commitID string, at time.Time) error
	MarkFailed(ctx context.Context, commitID, failure string, at time.Time) error
	// MarkFailedWithResult is the unrecoverable-recovery path: it fails the
	// reception and freezes its error result in the same update.
	MarkFailedWithResult(ctx context.Context, commitID, failure string, resultJSON []byte, at time.Time) error

	// RequestCancellation durably records a cancel intent for a commit. It is
	// idempotent and works before the file has been published.
	RequestCancellation(ctx context.Context, commitID string, at time.Time) error

	ListPendingNotifications(ctx context.Context) ([]Reception, error)
	ListUnfinishedReceptions(ctx context.Context) ([]Reception, error)
}

// NewRepository builds a MySQL-backed Repository, or the in-memory mock when
// no database is configured.
func NewRepository(db *sql.DB) Repository {
	if db == nil {
		return &MockRepository{}
	}
	return &sqlRepository{db: db}
}

// NewSpannerRepository builds a Spanner-backed Repository, or the in-memory
// mock when no client is configured.
func NewSpannerRepository(client *spanner.Client) Repository {
	if client == nil {
		return &MockRepository{}
	}
	return &spannerRepository{client: client}
}

type sqlRepository struct {
	db *sql.DB
}

// filesColumns is the canonical file index column list for SELECT scans.
var filesColumns = []string{"file_id", "shard_key", "hostname", "accepted_at", "published_at", "canceled_at"}

func scanAcceptedFile(row interface {
	Scan(dest ...interface{}) error
}) (*AcceptedFile, error) {
	var (
		file        AcceptedFile
		publishedAt sql.NullTime
		canceledAt  sql.NullTime
	)
	if err := row.Scan(
		&file.FileID,
		&file.ShardKey,
		&file.Hostname,
		&file.AcceptedAt,
		&publishedAt,
		&canceledAt,
	); err != nil {
		return nil, err
	}
	if publishedAt.Valid {
		ts := publishedAt.Time
		file.PublishedAt = &ts
	}
	if canceledAt.Valid {
		file.CanceledAt = canceledAt.Time
	}
	return &file, nil
}

func (r *sqlRepository) Record(ctx context.Context, file AcceptedFile) error {
	ctx, span := telemetry.StartSpan(ctx, "files-record", trace.WithAttributes(
		attribute.String("achgateway.file_id", file.FileID),
		attribute.String("achgateway.database", "mysql"),
	))
	defer span.End()

	qry := `INSERT INTO files (file_id, shard_key, hostname, accepted_at, published_at) VALUES (?,?,?,?,?);`
	_, err := r.db.ExecContext(ctx, qry,
		file.FileID,
		file.ShardKey,
		file.Hostname,
		file.AcceptedAt,
		file.PublishedAt,
	)
	if err != nil {
		return fmt.Errorf("recording file failed: %w", err)
	}
	return nil
}

func (r *sqlRepository) Cleanup(ctx context.Context, file AcceptedFile) error {
	ctx, span := telemetry.StartSpan(ctx, "files-cleanup", trace.WithAttributes(
		attribute.String("achgateway.file_id", file.FileID),
		attribute.String("achgateway.database", "sql"),
	))
	defer span.End()

	qry := `DELETE FROM files WHERE file_id = ? AND shard_key = ?;`
	_, err := r.db.ExecContext(ctx, qry,
		file.FileID,
		file.ShardKey,
	)
	if err != nil {
		return fmt.Errorf("cleaning up fileID record: %w", err)
	}
	return nil
}

func (r *sqlRepository) Cancel(ctx context.Context, fileID string) error {
	ctx, span := telemetry.StartSpan(ctx, "files-cancel", trace.WithAttributes(
		attribute.String("achgateway.file_id", fileID),
		attribute.String("achgateway.database", "mysql"),
	))
	defer span.End()

	// Only published files are canceled here. A cancel that arrives before
	// publication is held as a cancel intent on the reception ledger.
	qry := `UPDATE files SET canceled_at = ? WHERE file_id = ? AND canceled_at IS NULL AND published_at IS NOT NULL;`
	_, err := r.db.ExecContext(ctx, qry,
		// SET
		time.Now().In(time.UTC),
		// WHERE
		fileID,
	)
	if err != nil {
		return fmt.Errorf("saving file cancellation failed: %w", err)
	}
	return nil
}

func (r *sqlRepository) GetFile(ctx context.Context, fileID string) (*AcceptedFile, error) {
	ctx, span := telemetry.StartSpan(ctx, "files-get", trace.WithAttributes(
		attribute.String("achgateway.file_id", fileID),
		attribute.String("achgateway.database", "mysql"),
	))
	defer span.End()

	row := r.db.QueryRowContext(ctx,
		`SELECT file_id, shard_key, hostname, accepted_at, published_at, canceled_at FROM files WHERE file_id = ?;`,
		fileID,
	)
	file, err := scanAcceptedFile(row)
	if err != nil {
		return nil, fmt.Errorf("getting file %s: %w", fileID, err)
	}
	return file, nil
}

func (r *sqlRepository) MarkFilePublished(ctx context.Context, fileID string, at time.Time) error {
	ctx, span := telemetry.StartSpan(ctx, "files-mark-published", trace.WithAttributes(
		attribute.String("achgateway.file_id", fileID),
		attribute.String("achgateway.database", "mysql"),
	))
	defer span.End()

	qry := `UPDATE files SET published_at = ? WHERE file_id = ? AND published_at IS NULL;`
	res, err := r.db.ExecContext(ctx, qry, at, fileID)
	if err != nil {
		return fmt.Errorf("marking file published failed: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("checking published rows affected: %w", err)
	}
	if n == 0 {
		// Either already published (fine) or the index row is missing (error
		// surfaced through GetFile so callers can reconcile).
		if _, getErr := r.GetFile(ctx, fileID); getErr != nil {
			return getErr
		}
	}
	return nil
}

func (r *sqlRepository) ListPublishedFileIDs(ctx context.Context) (map[string]struct{}, error) {
	ctx, span := telemetry.StartSpan(ctx, "files-list-published", trace.WithAttributes(
		attribute.String("achgateway.database", "mysql"),
	))
	defer span.End()

	rows, err := r.db.QueryContext(ctx, `SELECT file_id FROM files WHERE published_at IS NOT NULL;`)
	if err != nil {
		return nil, fmt.Errorf("listing published files: %w", err)
	}
	defer rows.Close()

	out := make(map[string]struct{})
	for rows.Next() {
		var fileID string
		if err := rows.Scan(&fileID); err != nil {
			return nil, err
		}
		out[fileID] = struct{}{}
	}
	return out, rows.Err()
}
