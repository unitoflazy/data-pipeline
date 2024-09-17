package core

import "context"

// IDataPublisher is an interface for publishing data to external streams
type IDataPublisher interface {
	Publish(ctx context.Context, data []byte) error
}
