package files

import "time"

// AcceptedFile is the file index row. PublishedAt is nil until the staged
// file is atomically published into mergable; only published rows are
// queryable, cancellable and eligible for cutoff merging.
type AcceptedFile struct {
	FileID   string
	ShardKey string

	Hostname string

	AcceptedAt  time.Time
	PublishedAt *time.Time
	CanceledAt  time.Time
}
