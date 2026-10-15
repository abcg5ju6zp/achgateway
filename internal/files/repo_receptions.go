package files

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"github.com/moov-io/base/telemetry"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// receptionColumns is the canonical ledger column list for SELECT scans.
var receptionColumns = []string{
	"commit_id", "shard_name", "shard_key", "file_id", "source", "hostname",
	"stage", "accepted_at", "staged_at", "published_at", "notified_at",
	"cancel_requested_at", "content_sha256", "result_json", "last_error", "updated_at",
}

func scanReception(row interface {
	Scan(dest ...interface{}) error
}) (*Reception, error) {
	var (
		r                 Reception
		stagedAt          sql.NullTime
		publishedAt       sql.NullTime
		notifiedAt        sql.NullTime
		cancelRequestedAt sql.NullTime
		contentSHA256     sql.NullString
		resultJSON        sql.NullString
		lastError         sql.NullString
	)
	if err := row.Scan(
		&r.CommitID,
		&r.ShardName,
		&r.ShardKey,
		&r.FileID,
		&r.Source,
		&r.Hostname,
		&r.Stage,
		&r.AcceptedAt,
		&stagedAt,
		&publishedAt,
		&notifiedAt,
		&cancelRequestedAt,
		&contentSHA256,
		&resultJSON,
		&lastError,
		&r.UpdatedAt,
	); err != nil {
		return nil, err
	}
	if stagedAt.Valid {
		ts := stagedAt.Time
		r.StagedAt = &ts
	}
	if publishedAt.Valid {
		ts := publishedAt.Time
		r.PublishedAt = &ts
	}
	if notifiedAt.Valid {
		ts := notifiedAt.Time
		r.NotifiedAt = &ts
	}
	if cancelRequestedAt.Valid {
		ts := cancelRequestedAt.Time
		r.CancelRequestedAt = &ts
	}
	r.ContentSHA256 = contentSHA256.String
	r.ResultJSON = []byte(resultJSON.String)
	r.LastError = lastError.String
	return &r, nil
}

func (r *sqlRepository) RecordReception(ctx context.Context, reception Reception) error {
	ctx, span := telemetry.StartSpan(ctx, "reception-record", trace.WithAttributes(
		attribute.String("achgateway.commit_id", reception.CommitID),
		attribute.String("achgateway.database", "mysql"),
	))
	defer span.End()

	// INSERT IGNORE keeps the first writer's accepted_at and stage untouched.
	qry := `INSERT IGNORE INTO file_receptions
		(commit_id, shard_name, shard_key, file_id, source, hostname, stage, accepted_at, updated_at)
		VALUES (?,?,?,?,?,?,?,?,?);`
	_, err := r.db.ExecContext(ctx, qry,
		reception.CommitID,
		reception.ShardName,
		reception.ShardKey,
		reception.FileID,
		reception.Source,
		reception.Hostname,
		StageReceived,
		reception.AcceptedAt,
		reception.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("recording reception failed: %w", err)
	}
	return nil
}

func (r *sqlRepository) RecordPublishedReception(ctx context.Context, reception Reception) error {
	ctx, span := telemetry.StartSpan(ctx, "reception-record-published", trace.WithAttributes(
		attribute.String("achgateway.commit_id", reception.CommitID),
		attribute.String("achgateway.database", "mysql"),
	))
	defer span.End()

	qry := `INSERT IGNORE INTO file_receptions
		(commit_id, shard_name, shard_key, file_id, source, hostname, stage,
		 accepted_at, staged_at, published_at, notified_at, cancel_requested_at,
		 content_sha256, result_json, last_error, updated_at)
		VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?);`
	_, err := r.db.ExecContext(ctx, qry,
		reception.CommitID, reception.ShardName, reception.ShardKey, reception.FileID,
		reception.Source, reception.Hostname, StagePublished,
		reception.AcceptedAt, reception.StagedAt, reception.PublishedAt, reception.NotifiedAt,
		reception.CancelRequestedAt, reception.ContentSHA256, string(reception.ResultJSON),
		reception.LastError, reception.UpdatedAt,
	)
	if err != nil {
		return fmt.Errorf("recording published reception failed: %w", err)
	}
	return nil
}

func (r *sqlRepository) GetReception(ctx context.Context, commitID string) (*Reception, error) {
	ctx, span := telemetry.StartSpan(ctx, "reception-get", trace.WithAttributes(
		attribute.String("achgateway.commit_id", commitID),
		attribute.String("achgateway.database", "mysql"),
	))
	defer span.End()

	row := r.db.QueryRowContext(ctx,
		`SELECT commit_id, shard_name, shard_key, file_id, source, hostname,
			stage, accepted_at, staged_at, published_at, notified_at,
			cancel_requested_at, content_sha256, result_json, last_error, updated_at
		FROM file_receptions WHERE commit_id = ?;`,
		commitID,
	)
	reception, err := scanReception(row)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, fmt.Errorf("%s: %w", commitID, ErrReceptionNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("getting reception %s: %w", commitID, err)
	}
	return reception, nil
}

// advanceStage runs a guarded stage UPDATE. A no-op (row already beyond the
// transition) is success; a missing row or genuine regression is an error.
func (r *sqlRepository) advanceStage(ctx context.Context, spanName, commitID, set string, allowedStages string, wantStage string, args []interface{}) error {
	qry := `UPDATE file_receptions SET ` + set + ` WHERE commit_id = ? AND stage IN (` + allowedStages + `);`
	dbArgs := append([]interface{}{}, args...)
	dbArgs = append(dbArgs, commitID)

	res, err := r.db.ExecContext(ctx, qry, dbArgs...)
	if err != nil {
		return fmt.Errorf("%s failed: %w", spanName, err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return fmt.Errorf("%s: reading rows affected: %w", spanName, err)
	}
	if n == 0 {
		current, getErr := r.GetReception(ctx, commitID)
		if getErr != nil {
			return getErr
		}
		if stageRank(current.Stage) < stageRank(wantStage) {
			return fmt.Errorf("%s: %w (stage=%s)", spanName, ErrInvalidStage, current.Stage)
		}
	}
	return nil
}

func (r *sqlRepository) MarkStaged(ctx context.Context, commitID, contentSHA256 string, at time.Time) error {
	ctx, span := telemetry.StartSpan(ctx, "reception-mark-staged", trace.WithAttributes(
		attribute.String("achgateway.commit_id", commitID),
		attribute.String("achgateway.database", "mysql"),
	))
	defer span.End()

	return r.advanceStage(ctx, "mark staged", commitID,
		`stage = 'staged', staged_at = ?, content_sha256 = ?, updated_at = ?`,
		`'received','staged'`, StageStaged,
		[]interface{}{at, contentSHA256, at},
	)
}

func (r *sqlRepository) MarkIndexed(ctx context.Context, commitID string, at time.Time) error {
	ctx, span := telemetry.StartSpan(ctx, "reception-mark-indexed", trace.WithAttributes(
		attribute.String("achgateway.commit_id", commitID),
		attribute.String("achgateway.database", "mysql"),
	))
	defer span.End()

	return r.advanceStage(ctx, "mark indexed", commitID,
		`stage = 'indexed', updated_at = ?`,
		`'staged','indexed'`, StageIndexed,
		[]interface{}{at},
	)
}

func (r *sqlRepository) MarkPublished(ctx context.Context, commitID, fileID string, resultJSON []byte, at time.Time) error {
	ctx, span := telemetry.StartSpan(ctx, "reception-mark-published", trace.WithAttributes(
		attribute.String("achgateway.commit_id", commitID),
		attribute.String("achgateway.file_id", fileID),
		attribute.String("achgateway.database", "mysql"),
	))
	defer span.End()

	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin publish tx: %w", err)
	}
	defer tx.Rollback() //nolint:errcheck // committed transactions return ErrTxDone here

	// Publish the index row first: a missing files row aborts publication so
	// the caller can reconcile instead of marking a phantom file published.
	res, err := tx.ExecContext(ctx,
		`UPDATE files SET published_at = ? WHERE file_id = ? AND published_at IS NULL;`,
		at, fileID,
	)
	if err != nil {
		return fmt.Errorf("publishing file index: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		var publishedAt sql.NullTime
		row := tx.QueryRowContext(ctx, `SELECT published_at FROM files WHERE file_id = ?;`, fileID)
		if scanErr := row.Scan(&publishedAt); scanErr != nil {
			return fmt.Errorf("publishing file index %s: %w", fileID, scanErr)
		}
		if !publishedAt.Valid {
			return fmt.Errorf("publishing file index %s: row exists but unpublished", fileID)
		}
	}

	res, err = tx.ExecContext(ctx,
		`UPDATE file_receptions SET stage = 'published', published_at = ?, result_json = ?, updated_at = ?
		 WHERE commit_id = ? AND stage IN ('staged','indexed','published');`,
		at, string(resultJSON), at, commitID,
	)
	if err != nil {
		return fmt.Errorf("publishing reception: %w", err)
	}
	n, err = res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		current, getErr := r.GetReception(ctx, commitID)
		if getErr != nil {
			return getErr
		}
		if stageRank(current.Stage) < stageRank(StagePublished) {
			return fmt.Errorf("mark published: %w (stage=%s)", ErrInvalidStage, current.Stage)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit publish tx: %w", err)
	}
	return nil
}

func (r *sqlRepository) MarkNotified(ctx context.Context, commitID string, at time.Time) error {
	ctx, span := telemetry.StartSpan(ctx, "reception-mark-notified", trace.WithAttributes(
		attribute.String("achgateway.commit_id", commitID),
		attribute.String("achgateway.database", "mysql"),
	))
	defer span.End()

	qry := `UPDATE file_receptions SET notified_at = ?, updated_at = ? WHERE commit_id = ? AND stage IN ('published','failed');`
	res, err := r.db.ExecContext(ctx, qry, at, at, commitID)
	if err != nil {
		return fmt.Errorf("marking reception notified: %w", err)
	}
	n, err := res.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		current, getErr := r.GetReception(ctx, commitID)
		if getErr != nil {
			return getErr
		}
		if current.Stage != StagePublished && current.Stage != StageFailed {
			return fmt.Errorf("mark notified: %w (stage=%s)", ErrInvalidStage, current.Stage)
		}
	}
	return nil
}

func (r *sqlRepository) MarkFailed(ctx context.Context, commitID, failure string, at time.Time) error {
	ctx, span := telemetry.StartSpan(ctx, "reception-mark-failed", trace.WithAttributes(
		attribute.String("achgateway.commit_id", commitID),
		attribute.String("achgateway.database", "mysql"),
	))
	defer span.End()

	qry := `UPDATE file_receptions SET stage = 'failed', last_error = ?, updated_at = ?
		WHERE commit_id = ? AND stage <> 'failed';`
	res, err := r.db.ExecContext(ctx, qry, failure, at, commitID)
	if err != nil {
		return fmt.Errorf("marking reception failed: %w", err)
	}
	if n, _ := res.RowsAffected(); n == 0 {
		if _, getErr := r.GetReception(ctx, commitID); getErr != nil {
			return getErr
		}
	}
	return nil
}

func (r *sqlRepository) MarkFailedWithResult(ctx context.Context, commitID, failure string, resultJSON []byte, at time.Time) error {
	ctx, span := telemetry.StartSpan(ctx, "reception-mark-failed-result", trace.WithAttributes(
		attribute.String("achgateway.commit_id", commitID),
		attribute.String("achgateway.database", "mysql"),
	))
	defer span.End()

	qry := `UPDATE file_receptions
		SET stage = 'failed', last_error = ?, result_json = ?, updated_at = ?
		WHERE commit_id = ? AND stage <> 'failed';`
	_, err := r.db.ExecContext(ctx, qry, failure, string(resultJSON), at, commitID)
	if err != nil {
		return fmt.Errorf("marking reception failed with result: %w", err)
	}
	return nil
}

func (r *sqlRepository) RequestCancellation(ctx context.Context, commitID string, at time.Time) error {
	ctx, span := telemetry.StartSpan(ctx, "reception-request-cancel", trace.WithAttributes(
		attribute.String("achgateway.commit_id", commitID),
		attribute.String("achgateway.database", "mysql"),
	))
	defer span.End()

	qry := `UPDATE file_receptions SET cancel_requested_at = ?, updated_at = ? WHERE commit_id = ? AND cancel_requested_at IS NULL;`
	_, err := r.db.ExecContext(ctx, qry, at, at, commitID)
	if err != nil {
		return fmt.Errorf("recording cancel intent: %w", err)
	}
	return nil
}

func (r *sqlRepository) queryReceptions(ctx context.Context, spanName, where string) ([]Reception, error) {
	qry := `SELECT commit_id, shard_name, shard_key, file_id, source, hostname,
		stage, accepted_at, staged_at, published_at, notified_at,
		cancel_requested_at, content_sha256, result_json, last_error, updated_at
		FROM file_receptions WHERE ` + where + ` ORDER BY accepted_at;`
	rows, err := r.db.QueryContext(ctx, qry)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", spanName, err)
	}
	defer rows.Close()

	var out []Reception
	for rows.Next() {
		reception, err := scanReception(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *reception)
	}
	return out, rows.Err()
}

func (r *sqlRepository) ListPendingNotifications(ctx context.Context) ([]Reception, error) {
	ctx, span := telemetry.StartSpan(ctx, "reception-list-pending-notify", trace.WithAttributes(
		attribute.String("achgateway.database", "mysql"),
	))
	defer span.End()

	return r.queryReceptions(ctx, "listing pending notifications",
		`stage IN ('published','failed') AND notified_at IS NULL`)
}

func (r *sqlRepository) ListUnfinishedReceptions(ctx context.Context) ([]Reception, error) {
	ctx, span := telemetry.StartSpan(ctx, "reception-list-unfinished", trace.WithAttributes(
		attribute.String("achgateway.database", "mysql"),
	))
	defer span.End()

	return r.queryReceptions(ctx, "listing unfinished receptions",
		`stage IN ('received','staged','indexed') OR (stage = 'published' AND notified_at IS NULL)`)
}
