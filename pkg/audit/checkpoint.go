package audit

type Checkpoint struct {
	CopiedUntil string
	RowsCopied  uint64
}
