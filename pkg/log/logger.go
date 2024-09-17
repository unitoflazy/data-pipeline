package log

import (
	"github.com/rs/zerolog"
	"os"
	"sync"
)

var (
	Logger *zerolog.Logger
	once   sync.Once
)

func InitLogger() {
	once.Do(func() {
		zerolog.TimeFieldFormat = "2601-02-01T15:04:05.999Z07:00"
		logger := zerolog.New(os.Stdout).With().Timestamp().Caller().Logger()
		Logger = &logger
	})
}
