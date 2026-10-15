package files

import (
	"errors"
	"time"
)

// Reception lifecycle stages. They advance monotonically until published or
// failed; a failure recorded from an intermediate stage is terminal.
const (
	// StageReceived records the durable reception fact before anything is
	// written to disk.
	StageReceived = "received"
	// StageStaged means the NACHA bytes (and ValidateOpts) are durably held
	// in the staging directory.
	StageStaged = "staged"
	// StageIndexed means the file index row exists (still unpublished).
	StageIndexed = "indexed"
	// StagePublished means the file was atomically moved into mergable, is
	// queryable/cancellable and its result was frozen.
	StagePublished = "published"
	// StageFailed means the reception could not be recovered automatically.
	// Its frozen error result is still notified exactly once.
	StageFailed = "failed"
)

// Reception origins.
const (
	// SourceHTTP labels receptions arriving through the HTTP API.
	SourceHTTP = "http"
	// SourceStream labels receptions arriving over Kafka/stream.
	SourceStream = "stream"
)

// ErrReceptionNotFound is returned when no reception exists for a commitID.
var ErrReceptionNotFound = errors.New("reception not found")

// ErrInvalidStage is returned when a stage transition would move a reception
// backwards.
var ErrInvalidStage = errors.New("invalid stage transition")

// Reception is the durable, recoverable record of one file submission. It is
// keyed by the stable commit identity carried by the submission.
type Reception struct {
	CommitID  string
	ShardName string
	ShardKey  string
	FileID    string

	Source   string
	Hostname string

	Stage string

	AcceptedAt  time.Time
	StagedAt    *time.Time
	PublishedAt *time.Time
	NotifiedAt  *time.Time

	// CancelRequestedAt is set when a cancellation (possibly arriving before
	// the file) targets this commit.
	CancelRequestedAt *time.Time

	ContentSHA256 string
	ResultJSON    []byte
	LastError     string

	UpdatedAt time.Time
}

// IsTerminal reports whether the reception reached a final stage.
func (r Reception) IsTerminal() bool {
	return r.Stage == StagePublished || r.Stage == StageFailed
}

// StageRank orders stages for monotonicity checks.
func StageRank(stage string) int {
	switch stage {
	case StageReceived:
		return 1
	case StageStaged:
		return 2
	case StageIndexed:
		return 3
	case StagePublished:
		return 4
	case StageFailed:
		return 5
	default:
		return 0
	}
}

// stageRank orders stages for monotonicity checks.
func stageRank(stage string) int {
	return StageRank(stage)
}
