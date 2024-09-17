package handlers

import (
	"data-pipeline/core"
	"data-pipeline/dto"
	"data-pipeline/pkg/config"
	rpd "data-pipeline/server/handlers/ReadAndPublishData"
	"data-pipeline/server/handlers/Track"
	"encoding/json"
	"github.com/gofiber/fiber/v2"
)

type DataPipelineServiceParams struct {
	DB             core.IDatabase
	QueuePublisher core.IPublisher
	CSVDataClient  core.ICSVDataClient
}

type DataPipelineService struct {
	db  core.IDatabase
	qu  core.IPublisher
	csv core.ICSVDataClient
}

func NewDataPipelineService(params *DataPipelineServiceParams) *DataPipelineService {
	return &DataPipelineService{
		db:  params.DB,
		qu:  params.QueuePublisher,
		csv: params.CSVDataClient,
	}
}

func (d *DataPipelineService) ReadAndPublishData(ctx *fiber.Ctx) error {
	handler := rpd.NewReadAndPublishDataImpl(rpd.ReadAndPublishDataParams{
		DB:             d.db,
		QueuePublisher: d.qu,
		CSVDataClient:  d.csv,
		BatchSize:      config.GetInt64Env("BATCH_SIZE"),
	})

	rqdReq := &dto.ReadAndPublishDataRequest{}
	err := json.Unmarshal(ctx.Body(), rqdReq)
	if err != nil {
		return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	resp, err := handler.Handle(ctx.Context(), rqdReq)
	if err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return ctx.JSON(resp)
}

func (d *DataPipelineService) GetTaskLogs(ctx *fiber.Ctx) error {
	handler := Track.NewTrackImpl(Track.TrackParams{
		DB: d.db,
	})

	messageID := ctx.Query("message_id")
	if messageID == "" {
		return ctx.Status(fiber.StatusBadRequest).JSON(fiber.Map{
			"error": "message_id is empty",
		})
	}

	taskLogs, err := handler.Handle(messageID)
	if err != nil {
		return ctx.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
			"error": err.Error(),
		})
	}

	return ctx.JSON(taskLogs)
}
