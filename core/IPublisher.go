package core

import "context"

// IPublisher is an interface for publishing data to internal stream for async processing
type IPublisher interface {
	Publish(ctx context.Context, msg []byte) error
	PublishForAsync(ctx context.Context, msg []byte) error
}
