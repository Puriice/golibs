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
	config    RabbitListenerConfig
}

type RabbitListenerConfig struct {
	RabbitConfig
	QueueName     string
	Keys          []string
	PrefetchCount int
	PrefetchSize  int
	Global        bool
	Exclusive     bool
}

func NewRabbitListenerConfig(queueName string, keys ...string) RabbitListenerConfig {
	return RabbitListenerConfig{
		QueueName:     queueName,
		Keys:          keys,
		PrefetchCount: 10,
		PrefetchSize:  0,
		Global:        false,
		RabbitConfig: RabbitConfig{
			Durable:    true,
			AutoDelete: false,
			NoWait:     false,
		},
		Exclusive: false,
	}
}

func (r RabbitBroker) NewListener(queueName string, keys ...string) (*RabbitListener, error) {
	config := NewRabbitListenerConfig(queueName, keys...)

	return r.NewListenerWithConfig(config)
}

func (r RabbitBroker) NewListenerWithConfig(config RabbitListenerConfig) (*RabbitListener, error) {
	q, err := r.Channel.QueueDeclare(
		config.QueueName,
		config.Durable,
		config.AutoDelete,
		config.Exclusive,
		config.NoWait,
		nil,
	)

	if err != nil {
		return nil, err
	}

	if len(config.Keys) == 0 {
		config.Keys = []string{""}
	}

	for _, key := range config.Keys {
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
		config:    config,
	}, nil
}

func (l *RabbitListener) Subscribe(context context.Context, handler func([]byte) error) error {
	err := l.channel.Qos(l.config.PrefetchCount, l.config.PrefetchSize, false)
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

				if msg.Redelivered {
					msg.Nack(false, false)
				} else {
					msg.Nack(false, true)
				}

				continue
			}
			msg.Ack(false)
		}
	}
}
