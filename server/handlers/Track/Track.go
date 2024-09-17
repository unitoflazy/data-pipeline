package Track

import (
	"context"
	"data-pipeline/core"
	"data-pipeline/dto"
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"sort"
)

type TrackParams struct {
	DB core.IDatabase
}

type TrackImpl struct {
	db core.IDatabase
}

func NewTrackImpl(params TrackParams) *TrackImpl {
	return &TrackImpl{
		db: params.DB,
	}
}

func (t *TrackImpl) Handle(messageID string) (*dto.TrackResponse, error) {
	if messageID == "" {
		return nil, errors.New("message id is empty")
	}

	var (
		rootTaskMessage *dto.TaskMessage = &dto.TaskMessage{}
		pending         []dto.TaskMessageLog
		success         []dto.TaskMessageLog
	)

	objID, err := primitive.ObjectIDFromHex(messageID)
	if err != nil {
		return nil, err
	}
	err = t.db.FindOne(context.Background(), bson.M{"_id": objID}, rootTaskMessage)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return nil, err
	}

	err = t.db.FindMany(context.Background(), bson.M{"parent_message_id": messageID, "task_status": dto.StatusPending}, &pending)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return nil, err
	}

	err = t.db.FindMany(context.Background(), bson.M{"parent_message_id": messageID, "task_status": dto.StatusSuccess}, &success)
	if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
		return nil, err
	}

	pending = []dto.TaskMessageLog{}
	for _, taskMessageLog := range pending {
		if indexOf(taskMessageLog.MessageID, success) == -1 {
			pending = append(pending, taskMessageLog)
		}
	}

	sort.Slice(pending, func(i, j int) bool {
		return pending[i].BatchNumber < pending[j].BatchNumber
	})

	sort.Slice(success, func(i, j int) bool {
		return success[i].BatchNumber < success[j].BatchNumber
	})

	failed := int64(0)
	if rootTaskMessage.BatchTotal > 0 && len(pending) > 0 && len(success) > 0 {
		failed = rootTaskMessage.BatchTotal - int64(len(pending)-len(success))
	}

	return &dto.TrackResponse{
		Failed:          failed,
		RootTaskMessage: rootTaskMessage,
		Pending:         pending,
		Success:         success,
	}, nil
}

func indexOf(messageID string, taskMessageLogs []dto.TaskMessageLog) int {
	for i, taskMessageLog := range taskMessageLogs {
		if taskMessageLog.MessageID == messageID {
			return i
		}
	}

	return -1
}
