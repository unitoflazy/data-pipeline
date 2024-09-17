package rabbitmq

import (
	"context"
	"data-pipeline/core"
	"data-pipeline/pkg/config"
	"data-pipeline/pkg/log"
	amqp "github.com/rabbitmq/amqp091-go"
	"os"
	"os/signal"
	"syscall"
)

type rabbitMQ struct {
	consumeQ *amqp.Queue
	publishQ *amqp.Queue
	ch       *amqp.Channel
}

var (
	queueName          = config.GetEnv("RABBITMQ_QUEUE_NAME")
	publisherQueueName = config.GetEnv("RABBITMQ_PUBLISHER_QUEUE_NAME")
	durable            = config.GetEnvAsBool("RABBITMQ_QUEUE_DURABLE", false)
	deleteWhenUnused   = config.GetEnvAsBool("RABBITMQ_QUEUE_DELETE_WHEN_UNUSED", false)
	exclusive          = config.GetEnvAsBool("RABBITMQ_QUEUE_EXCLUSIVE", false)
	noWait             = config.GetEnvAsBool("RABBITMQ_QUEUE_NO_WAIT", false)

	rabbitMQClient *rabbitMQ
)

func RabbitMQClient() *rabbitMQ {
	if rabbitMQClient != nil {
		return rabbitMQClient
	}
	return NewRabbitMQ()
}

func NewRabbitMQ() *rabbitMQ {
	url := config.GetEnv("RABBITMQ_URL")
	conn, err := amqp.Dial(url)
	if err != nil {
		panic(err)
	}

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan,
			syscall.SIGINT,
			syscall.SIGTERM,
			syscall.SIGKILL,
			syscall.SIGQUIT,
			syscall.SIGABRT,
			syscall.SIGSTOP,
			syscall.SIGTSTP)
		<-sigChan
		_ = conn.Close()
	}()

	ch, err := conn.Channel()
	if err != nil {
		panic(err)
	}

	consumer, err := ch.QueueDeclare(
		queueName,
		durable,
		deleteWhenUnused,
		exclusive,
		noWait,
		nil,
	)
	if err != nil {
		panic(err)
	}

	publisher, err := ch.QueueDeclare(
		publisherQueueName,
		durable,
		deleteWhenUnused,
		exclusive,
		noWait,
		nil,
	)

	rabbitMQClient = &rabbitMQ{consumeQ: &consumer, publishQ: &publisher, ch: ch}

	return rabbitMQClient
}

func (r *rabbitMQ) Publish(ctx context.Context, msg []byte) error {
	return r.ch.PublishWithContext(ctx,
		"",
		r.publishQ.Name,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         msg,
		})
}

func (r *rabbitMQ) PublishForAsync(ctx context.Context, msg []byte) error {
	return r.ch.PublishWithContext(ctx,
		"",
		r.consumeQ.Name,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "application/json",
			Body:         msg,
		})
}

func (r *rabbitMQ) StartConsuming(processor core.IConsumer) error {
	msgs, err := r.ch.Consume(
		r.consumeQ.Name,
		"",
		durable,
		deleteWhenUnused,
		exclusive,
		noWait,
		nil,
	)
	if err != nil {
		return err
	}

	go func() {
		sigChan := make(chan os.Signal, 1)
		signal.Notify(sigChan,
			syscall.SIGINT,
			syscall.SIGTERM,
			syscall.SIGKILL,
			syscall.SIGQUIT,
			syscall.SIGABRT,
			syscall.SIGSTOP,
			syscall.SIGTSTP)
		<-sigChan
		_ = r.ch.Close()
	}()

	go func() {
		for d := range msgs {
			err := processor.Consume(d.Body)
			if err != nil {
				log.Logger.Err(err).Msg("error processing message")
			}
		}
	}()

	return nil
}
