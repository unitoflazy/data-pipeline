package read_and_publish_data

import (
	"context"
	"data-pipeline/core"
	"data-pipeline/dto"
	"encoding/json"
	"errors"
	"strconv"
)

type ReadAndPublishDataParams struct {
	DB             core.IDatabase
	QueuePublisher core.IPublisher
	CSVDataClient  core.ICSVDataClient
	BatchSize      int64
}

type ReadAndPublishDataImpl struct {
	db        core.IDatabase
	qu        core.IPublisher
	csv       core.ICSVDataClient
	batchSize int64
}

func NewReadAndPublishDataImpl(params ReadAndPublishDataParams) *ReadAndPublishDataImpl {
	return &ReadAndPublishDataImpl{
		db:        params.DB,
		qu:        params.QueuePublisher,
		csv:       params.CSVDataClient,
		batchSize: params.BatchSize,
	}
}

func (r *ReadAndPublishDataImpl) Handle(ctx context.Context, req *dto.ReadAndPublishDataRequest) (*dto.ReadAndPublishDataResponse, error) {
	if req == nil {
		return nil, errors.New("request is nil")
	}

	headRequest := &dto.CSVFileRequest{
		FileName:      req.FileName,
		FileContainer: req.FileContainer,
	}

	headers, err := r.csv.HeadCSVFile(ctx, headRequest)
	if err != nil {
		return nil, err
	}

	if headers == nil {
		return nil, errors.New("no headers found")
	}

	contentType := headers.Get("Content-Type")
	if contentType != "text/csv" {
		return nil, errors.New("invalid content type")
	}

	contentLength := headers.Get("Content-Length")
	if contentLength == "" {
		return nil, errors.New("content length is empty")
	}

	contentLengthInt, err := strconv.ParseInt(contentLength, 10, 64)
	if err != nil {
		return nil, err
	}

	contentMD5 := headers.Get("Content-MD5")
	if contentMD5 == "" {
		return nil, errors.New("content md5 is empty")
	}

	batches := r.calculateBatches(contentLengthInt)
	if batches == 0 {
		return nil, errors.New("no batches found")
	}

	taskMessage := &dto.TaskMessage{
		FileName:      req.FileName,
		FileContainer: req.FileContainer,
		ByteFrom:      0,
		ByteTo:        contentLengthInt,
		BatchNumber:   0,
		BatchTotal:    batches,
		ContentMD5:    contentMD5,
	}
	messageID, err := r.db.Insert(ctx, taskMessage)
	if err != nil {
		return nil, err
	}
	taskMessage.MessageID = messageID

	from := dto.CSVHeadOffset
	to := from + r.batchSize

	for i := int64(0); i < batches; i++ {
		taskMessageLog := taskMessage.ToTaskMessageLog(dto.StatusPending)
		taskMessageLog.MessageID = ""
		taskMessageLog.ParentMessageID = messageID
		taskMessageLog.BatchNumber = i + 1
		taskMessageLog.ByteFrom = from
		taskMessageLog.ByteTo = to
		if taskMessageLog.ByteTo > contentLengthInt {
			taskMessageLog.ByteTo = contentLengthInt
		}

		from = to + 1
		to = from + r.batchSize

		messageStr, err := json.Marshal(taskMessageLog)
		if err != nil {
			return nil, err
		}
		err = r.qu.PublishForAsync(ctx, messageStr)
		if err != nil {
			return nil, err
		}
	}

	return &dto.ReadAndPublishDataResponse{
		MessageID: messageID,
	}, nil
}

func (r *ReadAndPublishDataImpl) calculateBatches(contentLength int64) int64 {
	batches := contentLength / r.batchSize
	if contentLength%r.batchSize != 0 {
		batches++
	}

	return batches
}
