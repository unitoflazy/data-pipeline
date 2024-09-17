package dto

import "fmt"

type TaskMessage struct {
	MessageID     string `bson:"_id,omitempty" json:"message_id,omitempty"`
	FileName      string `bson:"file_name" json:"file_name"`
	FileContainer string `bson:"file_container" json:"file_container"`
	ByteFrom      int64  `bson:"byte_from" json:"byte_from"`
	ByteTo        int64  `bson:"byte_to" json:"byte_to"`
	BatchNumber   int64  `bson:"batch_number" json:"batch_number"`
	BatchTotal    int64  `bson:"batch_total" json:"batch_total"`
	ContentMD5    string `bson:"content_md5" json:"content_md5"`
}

func (t *TaskMessage) String() string {
	return fmt.Sprintf("MessageID: %s, ByteFrom: %d, ByteTo: %d, BatchNumber: %d, BatchTotal: %d", t.MessageID, t.ByteFrom, t.ByteTo, t.BatchNumber, t.BatchTotal)
}

func (t *TaskMessage) ToTaskMessageLog(status Status) *TaskMessageLog {
	if t == nil {
		return nil
	}
	return &TaskMessageLog{
		TaskMessage: *t,
		TaskStatus:  status,
	}
}
