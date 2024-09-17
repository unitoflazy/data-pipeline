package dto

import (
	"fmt"
	"net/http"
)

type CSVFileRequest struct {
	FileName      string
	FileContainer string
	Headers       http.Header
	ContentMD5    string
}

func (c *CSVFileRequest) String() string {
	return fmt.Sprintf("FileName: %s, FileContainer: %s, Headers: %v", c.FileName, c.FileContainer, c.Headers)
}
