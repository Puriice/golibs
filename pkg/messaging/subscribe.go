package messaging

import (
	"context"
	"log"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitListener struct {
	queueName string
	channel   *amqp.Channel
}

func (r *RabbitMQ) NewListener(queueName string, keys ...string) (*RabbitListener, error) {
	q, err := r.Channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return nil, err
	}

	for _, key := range keys {
		err := r.Channel.QueueBind(
			q.Name,
			key,
			r.Exchange,
			false,
			nil,
		)

		if err != nil {
			return nil, err
		}
	}

	return &RabbitListener{
		queueName: q.Name,
		channel:   r.Channel,
	}, nil
}

func (l *RabbitListener) Subscribe(context context.Context, handler func([]byte) error) error {
	err := l.channel.Qos(10, 0, false)
	if err != nil {
		return err
	}
	consumerTag := uuid.NewString()

	msgs, err := l.channel.Consume(
		l.queueName,
		consumerTag,
		false,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return err
	}

	for {
		select {
		case <-context.Done():
			_ = l.channel.Cancel(consumerTag, false)
			return nil
		case msg, ok := <-msgs:
			if !ok {
				return nil
			}

			if err := handler(msg.Body); err != nil {
				log.Println(err)

				msg.Nack(false, true)

				continue
			}
			msg.Ack(false)
		}
	}
}
