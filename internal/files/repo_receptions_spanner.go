package files

import (
	"context"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/moov-io/base/telemetry"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func scanSpannerReception(row *spanner.Row) (*Reception, error) {
	var (
		r                 Reception
		stagedAt          spanner.NullTime
		publishedAt       spanner.NullTime
		notifiedAt        spanner.NullTime
		cancelRequestedAt spanner.NullTime
		contentSHA256     spanner.NullString
		resultJSON        spanner.NullString
		lastError         spanner.NullString
	)
	if err := row.Columns(
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
	r.ContentSHA256 = contentSHA256.StringVal
	r.ResultJSON = []byte(resultJSON.StringVal)
	r.LastError = lastError.StringVal
	return &r, nil
}

func (r *spannerRepository) RecordReception(ctx context.Context, reception Reception) error {
	ctx, span := telemetry.StartSpan(ctx, "reception-record", trace.WithAttributes(
		attribute.String("achgateway.commit_id", reception.CommitID),
		attribute.String("achgateway.database", "spanner"),
	))
	defer span.End()

	// Read-then-insert keeps the first writer's row intact. A concurrent
	// first writer loses the unique constraint and its caller re-reads.
	_, err := r.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		row, err := txn.ReadRow(ctx, "file_receptions", spanner.Key{reception.CommitID}, []string{"commit_id"})
		if err != nil && spanner.ErrCode(err) != codes.NotFound {
			return err
		}
		if row != nil {
			return nil
		}
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.Insert("file_receptions",
				[]string{"commit_id", "shard_name", "shard_key", "file_id", "source", "hostname", "stage", "accepted_at", "updated_at"},
				[]interface{}{
					reception.CommitID, reception.ShardName, reception.ShardKey, reception.FileID,
					reception.Source, reception.Hostname, StageReceived, reception.AcceptedAt, reception.UpdatedAt,
				},
			),
		})
	})
	if err != nil {
		return fmt.Errorf("recording reception failed: %w", err)
	}
	return nil
}

func (r *spannerRepository) RecordPublishedReception(ctx context.Context, reception Reception) error {
	ctx, span := telemetry.StartSpan(ctx, "reception-record-published", trace.WithAttributes(
		attribute.String("achgateway.commit_id", reception.CommitID),
		attribute.String("achgateway.database", "spanner"),
	))
	defer span.End()

	_, err := r.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		row, err := txn.ReadRow(ctx, "file_receptions", spanner.Key{reception.CommitID}, []string{"commit_id"})
		if err != nil && spanner.ErrCode(err) != codes.NotFound {
			return err
		}
		if row != nil {
			return nil
		}
		return txn.BufferWrite([]*spanner.Mutation{
			spanner.Insert("file_receptions",
				[]string{
					"commit_id", "shard_name", "shard_key", "file_id", "source", "hostname",
					"stage", "accepted_at", "staged_at", "published_at", "notified_at",
					"cancel_requested_at", "content_sha256", "result_json", "last_error", "updated_at",
				},
				[]interface{}{
					reception.CommitID, reception.ShardName, reception.ShardKey, reception.FileID,
					reception.Source, reception.Hostname, StagePublished,
					reception.AcceptedAt, reception.StagedAt, reception.PublishedAt, reception.NotifiedAt,
					reception.CancelRequestedAt, reception.ContentSHA256, string(reception.ResultJSON),
					reception.LastError, reception.UpdatedAt,
				},
			),
		})
	})
	if err != nil {
		return fmt.Errorf("recording published reception failed: %w", err)
	}
	return nil
}

func (r *spannerRepository) GetReception(ctx context.Context, commitID string) (*Reception, error) {
	ctx, span := telemetry.StartSpan(ctx, "reception-get", trace.WithAttributes(
		attribute.String("achgateway.commit_id", commitID),
		attribute.String("achgateway.database", "spanner"),
	))
	defer span.End()

	row, err := r.client.Single().ReadRow(ctx, "file_receptions", spanner.Key{commitID}, receptionColumns)
	if err != nil {
		if spanner.ErrCode(err) == codes.NotFound {
			return nil, fmt.Errorf("%s: %w", commitID, ErrReceptionNotFound)
		}
		return nil, fmt.Errorf("getting reception %s: %w", commitID, err)
	}
	reception, err := scanSpannerReception(row)
	if err != nil {
		return nil, fmt.Errorf("reading reception row: %w", err)
	}
	return reception, nil
}

// updateReceptionStage runs a guarded stage DML and validates the row count
// against the current stage (regression errors surface, no-ops pass).
func (r *spannerRepository) updateReceptionStage(ctx context.Context, spanName, sqlText string, params map[string]interface{}, wantStage string) error {
	var rowCount int64
	_, err := r.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		n, err := txn.Update(ctx, spanner.Statement{SQL: sqlText, Params: params})
		if err != nil {
			return err
		}
		rowCount = n
		return nil
	})
	if err != nil {
		return fmt.Errorf("%s failed: %w", spanName, err)
	}
	if rowCount == 0 {
		commitID, _ := params["commitID"].(string)
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

func (r *spannerRepository) MarkStaged(ctx context.Context, commitID, contentSHA256 string, at time.Time) error {
	ctx, span := telemetry.StartSpan(ctx, "reception-mark-staged", trace.WithAttributes(
		attribute.String("achgateway.commit_id", commitID),
		attribute.String("achgateway.database", "spanner"),
	))
	defer span.End()

	return r.updateReceptionStage(ctx, "mark staged",
		`UPDATE file_receptions SET stage = 'staged', staged_at = @at, content_sha256 = @hash, updated_at = @at
		 WHERE commit_id = @commitID AND stage IN ('received','staged')`,
		map[string]interface{}{"at": at, "hash": contentSHA256, "commitID": commitID},
		StageStaged,
	)
}

func (r *spannerRepository) MarkIndexed(ctx context.Context, commitID string, at time.Time) error {
	ctx, span := telemetry.StartSpan(ctx, "reception-mark-indexed", trace.WithAttributes(
		attribute.String("achgateway.commit_id", commitID),
		attribute.String("achgateway.database", "spanner"),
	))
	defer span.End()

	return r.updateReceptionStage(ctx, "mark indexed",
		`UPDATE file_receptions SET stage = 'indexed', updated_at = @at
		 WHERE commit_id = @commitID AND stage IN ('staged','indexed')`,
		map[string]interface{}{"at": at, "commitID": commitID},
		StageIndexed,
	)
}

func (r *spannerRepository) MarkPublished(ctx context.Context, commitID, fileID string, resultJSON []byte, at time.Time) error {
	ctx, span := telemetry.StartSpan(ctx, "reception-mark-published", trace.WithAttributes(
		attribute.String("achgateway.commit_id", commitID),
		attribute.String("achgateway.file_id", fileID),
		attribute.String("achgateway.database", "spanner"),
	))
	defer span.End()

	var fileRows, receptionRows int64
	_, err := r.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		n, err := txn.Update(ctx, spanner.Statement{
			SQL: `UPDATE files SET published_at = @at WHERE file_id = @fileID AND published_at IS NULL`,
			Params: map[string]interface{}{
				"at":     at,
				"fileID": fileID,
			},
		})
		if err != nil {
			return err
		}
		fileRows = n

		n, err = txn.Update(ctx, spanner.Statement{
			SQL: `UPDATE file_receptions SET stage = 'published', published_at = @at, result_json = @result, updated_at = @at
				WHERE commit_id = @commitID AND stage IN ('staged','indexed','published')`,
			Params: map[string]interface{}{
				"at":       at,
				"result":   string(resultJSON),
				"commitID": commitID,
			},
		})
		if err != nil {
			return err
		}
		receptionRows = n
		return nil
	})
	if err != nil {
		return fmt.Errorf("marking published failed: %w", err)
	}
	if fileRows == 0 {
		file, getErr := r.GetFile(ctx, fileID)
		if getErr != nil {
			return getErr
		}
		if file.PublishedAt == nil {
			return fmt.Errorf("publishing file index %s: row exists but unpublished", fileID)
		}
	}
	if receptionRows == 0 {
		current, getErr := r.GetReception(ctx, commitID)
		if getErr != nil {
			return getErr
		}
		if stageRank(current.Stage) < stageRank(StagePublished) {
			return fmt.Errorf("mark published: %w (stage=%s)", ErrInvalidStage, current.Stage)
		}
	}
	return nil
}

func (r *spannerRepository) MarkNotified(ctx context.Context, commitID string, at time.Time) error {
	ctx, span := telemetry.StartSpan(ctx, "reception-mark-notified", trace.WithAttributes(
		attribute.String("achgateway.commit_id", commitID),
		attribute.String("achgateway.database", "spanner"),
	))
	defer span.End()

	return r.updateReceptionStage(ctx, "mark notified",
		`UPDATE file_receptions SET notified_at = @at, updated_at = @at
		 WHERE commit_id = @commitID AND stage IN ('published','failed')`,
		map[string]interface{}{"at": at, "commitID": commitID},
		StagePublished,
	)
}

func (r *spannerRepository) MarkFailed(ctx context.Context, commitID, failure string, at time.Time) error {
	ctx, span := telemetry.StartSpan(ctx, "reception-mark-failed", trace.WithAttributes(
		attribute.String("achgateway.commit_id", commitID),
		attribute.String("achgateway.database", "spanner"),
	))
	defer span.End()

	return r.updateReceptionStage(ctx, "mark failed",
		`UPDATE file_receptions SET stage = 'failed', last_error = @failure, updated_at = @at
		 WHERE commit_id = @commitID AND stage <> 'failed'`,
		map[string]interface{}{"failure": failure, "at": at, "commitID": commitID},
		StageFailed,
	)
}

func (r *spannerRepository) MarkFailedWithResult(ctx context.Context, commitID, failure string, resultJSON []byte, at time.Time) error {
	ctx, span := telemetry.StartSpan(ctx, "reception-mark-failed-result", trace.WithAttributes(
		attribute.String("achgateway.commit_id", commitID),
		attribute.String("achgateway.database", "spanner"),
	))
	defer span.End()

	_, err := r.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err := txn.Update(ctx, spanner.Statement{
			SQL: `UPDATE file_receptions SET stage = 'failed', last_error = @failure, result_json = @result, updated_at = @at
				WHERE commit_id = @commitID AND stage <> 'failed'`,
			Params: map[string]interface{}{
				"failure":  failure,
				"result":   string(resultJSON),
				"at":       at,
				"commitID": commitID,
			},
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("marking reception failed with result: %w", err)
	}
	return nil
}

func (r *spannerRepository) RequestCancellation(ctx context.Context, commitID string, at time.Time) error {
	ctx, span := telemetry.StartSpan(ctx, "reception-request-cancel", trace.WithAttributes(
		attribute.String("achgateway.commit_id", commitID),
		attribute.String("achgateway.database", "spanner"),
	))
	defer span.End()

	_, err := r.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err := txn.Update(ctx, spanner.Statement{
			SQL: `UPDATE file_receptions SET cancel_requested_at = @at, updated_at = @at
				WHERE commit_id = @commitID AND cancel_requested_at IS NULL`,
			Params: map[string]interface{}{"at": at, "commitID": commitID},
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("recording cancel intent: %w", err)
	}
	return nil
}

func (r *spannerRepository) queryReceptions(ctx context.Context, spanName, where string) ([]Reception, error) {
	stmt := spanner.Statement{
		SQL: `SELECT commit_id, shard_name, shard_key, file_id, source, hostname,
			stage, accepted_at, staged_at, published_at, notified_at,
			cancel_requested_at, content_sha256, result_json, last_error, updated_at
			FROM file_receptions WHERE ` + where + ` ORDER BY accepted_at`,
	}
	iter := r.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	var out []Reception
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("%s: %w", spanName, err)
		}
		reception, err := scanSpannerReception(row)
		if err != nil {
			return nil, err
		}
		out = append(out, *reception)
	}
	return out, nil
}

func (r *spannerRepository) ListPendingNotifications(ctx context.Context) ([]Reception, error) {
	ctx, span := telemetry.StartSpan(ctx, "reception-list-pending-notify", trace.WithAttributes(
		attribute.String("achgateway.database", "spanner"),
	))
	defer span.End()

	return r.queryReceptions(ctx, "listing pending notifications",
		`stage IN ('published','failed') AND notified_at IS NULL`)
}

func (r *spannerRepository) ListUnfinishedReceptions(ctx context.Context) ([]Reception, error) {
	ctx, span := telemetry.StartSpan(ctx, "reception-list-unfinished", trace.WithAttributes(
		attribute.String("achgateway.database", "spanner"),
	))
	defer span.End()

	return r.queryReceptions(ctx, "listing unfinished receptions",
		`stage IN ('received','staged','indexed') OR (stage = 'published' AND notified_at IS NULL)`)
}
