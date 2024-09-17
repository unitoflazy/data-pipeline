package dto

type ReadAndPublishDataRequest struct {
	FileName      string `json:"file_name"`
	FileContainer string `json:"file_container"`
}

type ReadAndPublishDataResponse struct {
	MessageID string `json:"message_id"`
}
