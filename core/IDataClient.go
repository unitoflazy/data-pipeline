package core

import (
	"context"
	"data-pipeline/dto"
	"net/http"
)

type ICSVDataClient interface {
	HeadCSVFile(ctx context.Context, req *dto.CSVFileRequest) (*http.Header, error)
	GetCSVFile(ctx context.Context, req *dto.CSVFileRequest) ([]dto.CSVRecord, error)
}
