package dto

import "fmt"

type Status string

const (
	StatusSuccess Status = "success"
	StatusFailure Status = "failure"
	StatusPending Status = "pending"
)

type TaskMessageLog struct {
	ParentMessageID string `bson:"parent_message_id" json:"parent_message_id"`
	TaskMessage
	TaskStatus Status `bson:"task_status" json:"task_status"`
}

func (t *TaskMessageLog) String() string {
	return fmt.Sprintf("ParentMessageID: %s, MessageID: %s, ByteFrom: %d, ByteTo: %d, BatchNumber: %d, BatchTotal: %d, Status: %s", t.ParentMessageID, t.MessageID, t.ByteFrom, t.ByteTo, t.BatchNumber, t.BatchTotal, t.TaskStatus)
}

func (t *TaskMessageLog) ToCSVFileRequest() *CSVFileRequest {
	if t == nil {
		return nil
	}
	header := make(map[string][]string)
	header["Range"] = []string{fmt.Sprintf("bytes=%d-%d", t.ByteFrom, t.ByteTo)}

	return &CSVFileRequest{
		FileName:      t.FileName,
		FileContainer: t.FileContainer,
		Headers:       header,
		ContentMD5:    t.ContentMD5,
	}
}
