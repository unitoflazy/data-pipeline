package dto

const (
	CSVHeadOffset = int64(len("user_id,segment_type\n"))
)

type CSVRecord struct {
	UserID      string `json:"user_id"`
	SegmentType string `json:"segment_type"`
}
