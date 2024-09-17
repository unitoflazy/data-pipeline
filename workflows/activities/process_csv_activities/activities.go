package process_csv_activities

import (
	"context"
	"data-pipeline/database/mongodb"
	"data-pipeline/dto"
	"data-pipeline/pkg/azblob"
	"data-pipeline/queues/rabbitmq"
	"encoding/json"
)

func InsertToDB(taskMessageLog *dto.TaskMessageLog) error {
	_, err := mongodb.MongoClient().Insert(context.Background(), taskMessageLog)
	return err
}

func GetCSVData(req *dto.CSVFileRequest) ([]dto.CSVRecord, error) {
	data, err := azblob.GetAzBlobClient().GetCSVFile(context.Background(), req)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func PublishData(data *[]dto.CSVRecord) error {
	dataBytes, err := json.Marshal(&data)
	if err != nil {
		return err
	}

	return rabbitmq.RabbitMQClient().Publish(context.Background(), dataBytes)
}
