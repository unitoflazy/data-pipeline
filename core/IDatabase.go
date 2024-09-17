package core

import (
	"context"
)

type IDatabase interface {
	FindOne(ctx context.Context, filter any, dto any) error
	FindMany(ctx context.Context, filter any, dto any) error
	Upsert(ctx context.Context, entity any) (string, error)
	Insert(ctx context.Context, entity any) (string, error)
}
