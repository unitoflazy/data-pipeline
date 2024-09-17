package azblob

import (
	"context"
	"data-pipeline/dto"
	"data-pipeline/pkg/config"
	"data-pipeline/pkg/log"
	"encoding/csv"
	"errors"
	"fmt"
	"net/http"
)

var (
	NoneRetryableHTTPError    = errors.New("azblob none retryable: http error")
	NoneRetryableNilRespError = errors.New("azblob none retryable: nil response error")
	NoneRetryableHeaderError  = errors.New("azblob none retryable: header error")

	ContentMD5        = "Content-MD5"
	XMSBlobContentMD5 = "X-Ms-Blob-Content-Md5"
	ContentType       = "Content-Type"
	TextCSV           = "text/csv"
)

type AzBlobClient struct {
	url       string
	authToken string
}

var azBlobClient *AzBlobClient

func GetAzBlobClient() *AzBlobClient {
	if azBlobClient != nil {
		return azBlobClient
	}
	return NewAzBlobClient()
}

func NewAzBlobClient() *AzBlobClient {
	azBlobClient = &AzBlobClient{
		url:       config.GetEnv("AZBLOB_URL"),
		authToken: config.GetEnv("AZBLOB_AUTH_TOKEN"),
	}
	return azBlobClient
}

func (c *AzBlobClient) validateHeader(resp *http.Response, md5Sum string) error {
	if resp == nil {
		return NoneRetryableNilRespError
	}
	if resp.Header.Get(ContentType) != TextCSV {
		return NoneRetryableHeaderError
	}
	if resp.StatusCode >= http.StatusBadRequest && resp.StatusCode < http.StatusInternalServerError {
		return NoneRetryableHTTPError
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return errors.New("retryable http error")
	}

	if resp.Header.Get(XMSBlobContentMD5) == md5Sum {
		return nil
	}
	if resp.Header.Get(ContentMD5) == md5Sum {
		return nil
	}

	return NoneRetryableHeaderError
}

func (c *AzBlobClient) GetCSVFile(ctx context.Context, req *dto.CSVFileRequest) ([]dto.CSVRecord, error) {
	if req == nil || req.ContentMD5 == "" {
		return nil, NoneRetryableHeaderError
	}

	path := fmt.Sprintf("%s/%s/%s?%s", c.url, req.FileContainer, req.FileName, c.authToken)

	request, err := http.NewRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	request.Header = req.Headers

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	err = c.validateHeader(resp, req.ContentMD5)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	csvReader := csv.NewReader(resp.Body)
	records, err := csvReader.ReadAll()
	if err != nil {
		return nil, err
	}

	var csvRecords []dto.CSVRecord
	for _, record := range records {
		if len(record) != 2 || record[0] == "" || record[1] == "" {
			log.Logger.Warn().Msg(fmt.Sprintf("skipping invalid record: %v request %v", record, req))
			continue
		}
		csvRecords = append(csvRecords, dto.CSVRecord{
			UserID:      record[0],
			SegmentType: record[1],
		})
	}

	return csvRecords, nil
}

func (c *AzBlobClient) HeadCSVFile(ctx context.Context, req *dto.CSVFileRequest) (*http.Header, error) {
	if req == nil {
		return nil, NoneRetryableHeaderError
	}

	path := fmt.Sprintf("%s/%s/%s?%s", c.url, req.FileContainer, req.FileName, c.authToken)

	request, err := http.NewRequest(http.MethodHead, path, nil)
	if err != nil {
		return nil, err
	}
	request.Header = req.Headers

	resp, err := http.DefaultClient.Do(request)
	if err != nil {
		return nil, err
	}
	if resp == nil {
		return nil, NoneRetryableNilRespError
	}
	defer resp.Body.Close()

	return &resp.Header, nil
}
