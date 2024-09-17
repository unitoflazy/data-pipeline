package main

import (
	"data-pipeline/database/mongodb"
	"data-pipeline/pkg/azblob"
	"data-pipeline/pkg/config"
	"data-pipeline/queues/rabbitmq"
	"data-pipeline/workflows"
	pca "data-pipeline/workflows/activities/process_csv_activities"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

func main() {
	azblob.NewAzBlobClient()
	mongodb.NewMongoDBClient()
	rabbitmq.NewRabbitMQ()

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
