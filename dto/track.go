package dto

type TrackResponse struct {
	RootTaskMessage *TaskMessage     `json:"root_task_message,omitempty"`
	Failed          int64            `json:"failed,omitempty"`
	Pending         []TaskMessageLog `json:"pending,omitempty"`
	Success         []TaskMessageLog `json:"success,omitempty"`
}
