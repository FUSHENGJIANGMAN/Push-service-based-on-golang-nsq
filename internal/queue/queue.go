package queue

import "context"

type Enqueuer interface {
	Enqueue(ctx context.Context, payload []byte) error
	Close()
}

type Consumer interface {
	Run(ctx context.Context) error
	Stop()
}
