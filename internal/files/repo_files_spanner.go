package files

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"cloud.google.com/go/spanner"
	"github.com/moov-io/base/telemetry"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

type spannerRepository struct {
	client *spanner.Client
}

func (r *spannerRepository) Record(ctx context.Context, file AcceptedFile) error {
	ctx, span := telemetry.StartSpan(ctx, "files-record", trace.WithAttributes(
		attribute.String("achgateway.file_id", file.FileID),
		attribute.String("achgateway.database", "spanner"),
	))
	defer span.End()

	m := spanner.Insert("files",
		[]string{"file_id", "shard_key", "hostname", "accepted_at", "published_at"},
		[]interface{}{file.FileID, file.ShardKey, file.Hostname, file.AcceptedAt, file.PublishedAt},
	)
	_, err := r.client.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return fmt.Errorf("recording file failed: %w", err)
	}
	return nil
}

func (r *spannerRepository) Cleanup(ctx context.Context, file AcceptedFile) error {
	ctx, span := telemetry.StartSpan(ctx, "files-cleanup", trace.WithAttributes(
		attribute.String("achgateway.file_id", file.FileID),
		attribute.String("achgateway.database", "spanner"),
	))
	defer span.End()

	m := spanner.Delete("files", spanner.Key{file.FileID})

	_, err := r.client.Apply(ctx, []*spanner.Mutation{m})
	if err != nil {
		return fmt.Errorf("cleanup of files record failed: %w", err)
	}
	return nil
}

func (r *spannerRepository) Cancel(ctx context.Context, fileID string) error {
	ctx, span := telemetry.StartSpan(ctx, "files-cancel", trace.WithAttributes(
		attribute.String("achgateway.file_id", fileID),
		attribute.String("achgateway.database", "spanner"),
	))
	defer span.End()

	// Only published files are canceled here; pre-publication cancel intent
	// lives on the reception ledger.
	_, err := r.client.ReadWriteTransaction(ctx, func(ctx context.Context, txn *spanner.ReadWriteTransaction) error {
		_, err := txn.Update(ctx, spanner.Statement{
			SQL: `UPDATE files SET canceled_at = @canceledAt
				WHERE file_id = @fileID AND canceled_at IS NULL AND published_at IS NOT NULL`,
			Params: map[string]interface{}{
				"canceledAt": time.Now().In(time.UTC),
				"fileID":     fileID,
			},
		})
		return err
	})
	if err != nil {
		return fmt.Errorf("saving file cancellation failed: %w", err)
	}
	return nil
}

func (r *spannerRepository) GetFile(ctx context.Context, fileID string) (*AcceptedFile, error) {
	ctx, span := telemetry.StartSpan(ctx, "files-get", trace.WithAttributes(
		attribute.String("achgateway.file_id", fileID),
		attribute.String("achgateway.database", "spanner"),
	))
	defer span.End()

	row, err := r.client.Single().ReadRow(ctx, "files", spanner.Key{fileID}, filesColumns)
	if err != nil {
		if spanner.ErrCode(err) == codes.NotFound {
			return nil, fmt.Errorf("getting file %s: %w", fileID, sql.ErrNoRows)
		}
		return nil, fmt.Errorf("getting file %s: %w", fileID, err)
	}

	var (
		file        AcceptedFile
		publishedAt spanner.NullTime
		canceledAt  spanner.NullTime
	)
	if err := row.Columns(&file.FileID, &file.ShardKey, &file.Hostname, &file.AcceptedAt, &publishedAt, &canceledAt); err != nil {
		return nil, fmt.Errorf("reading files row: %w", err)
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

func (r *spannerRepository) MarkFilePublished(ctx context.Context, fileID string, at time.Time) error {
	ctx, span := telemetry.StartSpan(ctx, "files-mark-published", trace.WithAttributes(
		attribute.String("achgateway.file_id", fileID),
		attribute.String("achgateway.database", "spanner"),
	))
	defer span.End()

	var rowCount int64
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
		rowCount = n
		return nil
	})
	if err != nil {
		return fmt.Errorf("marking file published failed: %w", err)
	}
	if rowCount == 0 {
		if _, getErr := r.GetFile(ctx, fileID); getErr != nil {
			return getErr
		}
	}
	return nil
}

func (r *spannerRepository) ListPublishedFileIDs(ctx context.Context) (map[string]struct{}, error) {
	ctx, span := telemetry.StartSpan(ctx, "files-list-published", trace.WithAttributes(
		attribute.String("achgateway.database", "spanner"),
	))
	defer span.End()

	stmt := spanner.Statement{SQL: `SELECT file_id FROM files WHERE published_at IS NOT NULL`}
	iter := r.client.Single().Query(ctx, stmt)
	defer iter.Stop()

	out := make(map[string]struct{})
	for {
		row, err := iter.Next()
		if err == iterator.Done {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("listing published files: %w", err)
		}
		var fileID string
		if err := row.Columns(&fileID); err != nil {
			return nil, err
		}
		out[fileID] = struct{}{}
	}
	return out, nil
}
