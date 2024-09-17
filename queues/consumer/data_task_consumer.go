package consumer

import (
	"context"
	"data-pipeline/dto"
	"data-pipeline/pkg/log"
	"data-pipeline/workflows"
	"encoding/json"
	"fmt"
	"go.temporal.io/sdk/client"
)

type DataTaskConsumerParams struct {
	TemporalClientOptions client.Options
	StartWorkflowOptions  client.StartWorkflowOptions
	WorkflowParams        *workflows.ProcessCSVParams
}

type DataTaskConsumer struct {
	temporalClient       client.Client
	startWorkflowOptions client.StartWorkflowOptions
	workflowParams       *workflows.ProcessCSVParams
}

func NewDataTaskConsumer(params *DataTaskConsumerParams) (*DataTaskConsumer, error) {
	c, err := client.Dial(params.TemporalClientOptions)
	if err != nil {
		return nil, err
	}

	return &DataTaskConsumer{
		temporalClient:       c,
		startWorkflowOptions: params.StartWorkflowOptions,
		workflowParams:       params.WorkflowParams,
	}, nil
}

func (c *DataTaskConsumer) Consume(msg []byte) error {
	var taskMessage dto.TaskMessageLog
	err := json.Unmarshal(msg, &taskMessage)
	if err != nil {
		return err
	}

	log.Logger.Info().Msg(fmt.Sprintf("processing message: %s", taskMessage.String()))

	c.workflowParams.TaskMessageLog = &taskMessage
	workflowRun, err := c.temporalClient.ExecuteWorkflow(context.Background(), c.startWorkflowOptions, workflows.ProcessCSVWorkflow, c.workflowParams)
	if err != nil {
		log.Logger.Err(err).Msg("failed to start workflow")
		return err
	}

	err = workflowRun.Get(context.Background(), nil)
	if err != nil {
		log.Logger.Err(err).Msg("failed to complete workflow")
		return err
	}

	log.Logger.Info().Msg(fmt.Sprintf("completed message: %s", taskMessage.String()))
	return nil
}
