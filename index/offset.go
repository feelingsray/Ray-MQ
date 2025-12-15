package index

type MsgIndex struct {
	MsgID       string
	Topic       string
	Partition   string
	Args        map[string]string
	StartOffset MsgOffset
	EndOffset   MsgOffset
}

type MsgOffset struct {
	SegmentID int
	Offset    int64
}
