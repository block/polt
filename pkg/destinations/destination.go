package destinations

type DstType int32

const (
	ArchiveTable DstType = iota
	StageTable
	S3File
	LocalParquetFile
)

func (s DstType) String() string {
	switch s {
	case ArchiveTable:
		return "table"
	case StageTable:
		return "stage-table"
	case S3File:
		return "s3"
	case LocalParquetFile:
		return "parquet-local"
	}

	return "unknown"
}
