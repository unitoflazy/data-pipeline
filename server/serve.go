package server

import (
	"data-pipeline/database/mongodb"
	"data-pipeline/pkg/azblob"
	"data-pipeline/pkg/config"
	"data-pipeline/pkg/log"
	"data-pipeline/queues/consumer"
	"data-pipeline/queues/rabbitmq"
	"data-pipeline/server/handlers"
	"data-pipeline/workflows"
	pca "data-pipeline/workflows/activities/process_csv_activities"
	"fmt"
	"github.com/gofiber/fiber/v2"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/worker"
	workflows2 "go.temporal.io/sdk/workflow"
	"time"
)

func Serve() {
	log.InitLogger()

	csvDataClient := azblob.NewAzBlobClient()
	db := mongodb.NewMongoDBClient()
	rmq := rabbitmq.NewRabbitMQ()

	retryPolicy := &temporal.RetryPolicy{
		InitialInterval:        time.Second,
		BackoffCoefficient:     float64(config.GetInt64Env("TEMPORAL_BACKOFF_COEFFICIENT")),
		MaximumInterval:        100 * time.Second,
		MaximumAttempts:        int32(config.GetInt64Env("TEMPORAL_MAXIMUM_ATTEMPTS")),
		NonRetryableErrorTypes: []string{azblob.NoneRetryableHTTPError.Error(), azblob.NoneRetryableHeaderError.Error(), azblob.NoneRetryableNilRespError.Error()},
	}

	// init rabbitmq consumer for async task processing
	dataTaskConsumer, err := consumer.NewDataTaskConsumer(&consumer.DataTaskConsumerParams{
		TemporalClientOptions: client.Options{
			HostPort: config.GetEnv("TEMPORAL_HOST_PORT"),
		},
		StartWorkflowOptions: client.StartWorkflowOptions{
			ID:        config.GetEnvOrDefault("TEMPORAL_WORKFLOW_ID", "data-pipeline-task"),
			TaskQueue: config.GetEnvOrDefault("TEMPORAL_TASK_QUEUE", "data-pipeline-task-queue"),
		},
		WorkflowParams: &workflows.ProcessCSVParams{
			Options: workflows2.ActivityOptions{
				ScheduleToCloseTimeout: time.Minute,
				StartToCloseTimeout:    time.Second * 30,
				RetryPolicy:            retryPolicy,
			},
		},
	})
	if err != nil {
		panic(err)
	}

	err = rmq.StartConsuming(dataTaskConsumer)
	if err != nil {
		panic(err)
	}

	service := handlers.NewDataPipelineService(&handlers.DataPipelineServiceParams{
		DB:             db,
		CSVDataClient:  csvDataClient,
		QueuePublisher: rmq,
	})

	go initWorkers()

	app := fiber.New()
	app.Get("/health", func(ctx *fiber.Ctx) error {
		return ctx.SendString("OK")
	})

	app.Post("/read-and-publish-data", service.ReadAndPublishData)
	app.Get("/track", service.GetTaskLogs)

	app.Use(func(ctx *fiber.Ctx) error {
		return ctx.Status(fiber.StatusNotFound).SendString("Not Found")
	})

	err = app.Listen(fmt.Sprintf(":%s", config.GetEnv("APP_PORT")))
	if err != nil {
		panic(err)
	}
}

func initWorkers() {
	c, err := client.Dial(client.Options{
		HostPort: config.GetEnv("TEMPORAL_HOST_PORT"),
	})
	if err != nil {
		panic(err)
	}

	taskQueue := config.GetEnv("TEMPORAL_TASK_QUEUE")

	w := worker.New(c, taskQueue, worker.Options{})
	w.RegisterWorkflow(workflows.ProcessCSVWorkflow)
	w.RegisterActivity(pca.GetCSVData)
	w.RegisterActivity(pca.InsertToDB)
	w.RegisterActivity(pca.PublishData)

	// Start listening to the Task Queue
	err = w.Run(worker.InterruptCh())
	if err != nil {
		panic(err)
	}
}
