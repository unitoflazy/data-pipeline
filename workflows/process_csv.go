package workflows

import (
	"data-pipeline/dto"
	"data-pipeline/pkg/log"
	pca "data-pipeline/workflows/activities/process_csv_activities"
	"go.temporal.io/sdk/workflow"
)

type ProcessCSVParams struct {
	Options        workflow.ActivityOptions
	TaskMessageLog *dto.TaskMessageLog
}

func ProcessCSVWorkflow(ctx workflow.Context, params *ProcessCSVParams) error {
	ctx = workflow.WithActivityOptions(ctx, params.Options)

	// off set header of csv file
	if params.TaskMessageLog.BatchNumber == 0 {
		params.TaskMessageLog.ByteFrom = int64(len("user_id,segment_type\n"))
	}

	err := workflow.ExecuteActivity(ctx, pca.InsertToDB, params.TaskMessageLog).Get(ctx, nil)
	if err != nil {
		log.Logger.Err(err).Msg("workflow failed to insert task log")
		return err
	}

	var (
		data []dto.CSVRecord     = nil
		req  *dto.CSVFileRequest = params.TaskMessageLog.ToCSVFileRequest()
	)
	err = workflow.ExecuteActivity(ctx, pca.GetCSVData, req).Get(ctx, &data)
	if err != nil {
		log.Logger.Err(err).Msg("workflow failed to get csv data")
		return err
	}

	if data == nil {
		params.TaskMessageLog.TaskStatus = dto.StatusFailure
		err = workflow.ExecuteActivity(ctx, pca.InsertToDB, params.TaskMessageLog).Get(ctx, nil)
		if err != nil {
			log.Logger.Err(err).Msg("workflow failed to update task log")
			return err
		}
		return nil
	}

	err = workflow.ExecuteActivity(ctx, pca.PublishData, &data).Get(ctx, nil)
	if err != nil {
		log.Logger.Err(err).Msg("workflow failed to publish data")
		return err
	}

	params.TaskMessageLog.TaskStatus = dto.StatusSuccess
	err = workflow.ExecuteActivity(ctx, pca.InsertToDB, params.TaskMessageLog).Get(ctx, nil)
	if err != nil {
		log.Logger.Err(err).Msg("workflow failed to update task log")
		return err
	}

	return nil
}
