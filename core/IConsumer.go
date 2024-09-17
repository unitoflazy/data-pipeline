package core

type IConsumer interface {
	Consume(data []byte) error
}
