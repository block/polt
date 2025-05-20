package archive

import "context"

type Archiver interface {
	Move(ctx context.Context, dryRun bool) error
	IsResumable() bool
}

type ResumableArchiver interface {
	Archiver
	GetLowWatermark() (string, error)
	NumCopiedRows() uint64
	GetProgress() string
}
