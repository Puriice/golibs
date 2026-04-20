package messaging

import (
	"context"
	"log"
	"time"

	"github.com/google/uuid"
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitListener struct {
	rabbit    *RabbitMQ
	queueName string
	exchange  string
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
	Args          amqp.Table
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
		Args:      nil,
	}
}

func (r RabbitBroker) NewListener(queueName string, keys ...string) (*RabbitListener, error) {
	config := NewRabbitListenerConfig(queueName, keys...)

	return r.NewListenerWithConfig(config)
}

func (r RabbitBroker) NewListenerWithConfig(config RabbitListenerConfig) (*RabbitListener, error) {
	ch, err := r.RabbitMQ.getChannel()
	if err != nil {
		return nil, err
	}

	q, err := ch.QueueDeclare(
		config.QueueName,
		config.Durable,
		config.AutoDelete,
		config.Exclusive,
		config.NoWait,
		config.Args,
	)
	if err != nil {
		return nil, err
	}

	if len(config.Keys) == 0 {
		config.Keys = []string{""}
	}

	for _, key := range config.Keys {
		err := ch.QueueBind(
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
		rabbit:    r.RabbitMQ,
		queueName: q.Name,
		exchange:  r.Exchange,
		config:    config,
	}, nil
}

func (l *RabbitListener) Subscribe(ctx context.Context, handler func([]byte) error) error {
	for {
		ch, err := l.rabbit.NewChannel()

		if err != nil {
			log.Println("Get channel failed:", err)
			time.Sleep(2 * time.Second)
			continue
		}

		// 🔁 Re-declare queue after reconnect
		q, err := ch.QueueDeclare(
			l.config.QueueName,
			l.config.Durable,
			l.config.AutoDelete,
			l.config.Exclusive,
			l.config.NoWait,
			l.config.Args,
		)
		if err != nil {
			log.Println("QueueDeclare failed:", err)
			time.Sleep(2 * time.Second)
			continue
		}

		// 🔁 Re-bind
		bindFailed := false

		for _, key := range l.config.Keys {
			err := ch.QueueBind(
				q.Name,
				key,
				l.exchange,
				false,
				nil,
			)
			if err != nil {
				log.Println("QueueBind failed:", err)
				bindFailed = true
				break
			}
		}

		if bindFailed { // 👈 then retry outer loop
			ch.Close()
			time.Sleep(2 * time.Second)
			continue
		}

		err = ch.Qos(l.config.PrefetchCount, l.config.PrefetchSize, false)
		if err != nil {
			log.Println("Qos failed:", err)
			time.Sleep(2 * time.Second)
			continue
		}

		consumerTag := uuid.NewString()

		msgs, err := ch.Consume(
			q.Name,
			consumerTag,
			false,
			false,
			false,
			false,
			nil,
		)
		if err != nil {
			log.Println("Consume failed:", err)
			time.Sleep(2 * time.Second)
			continue
		}

		log.Println("📥 Consumer started")

		errChan := make(chan *amqp.Error)
		ch.NotifyClose(errChan)

	loop:
		for {
			select {
			case <-ctx.Done():
				_ = ch.Cancel(consumerTag, false)
				return nil

			case err := <-errChan:
				log.Println("❌ Channel closed:", err)
				break loop

			case msg, ok := <-msgs:
				if !ok {
					log.Println("❌ msgs channel closed")
					break loop
				}

				func() {
					defer func() {
						if r := recover(); r != nil {
							log.Println("Recovered panic:", r)
							msg.Nack(false, true)
						}
					}()

					if err := handler(msg.Body); err != nil {
						log.Println(err)

						if msg.Redelivered {
							msg.Nack(false, false)
						} else {
							msg.Nack(false, true)
						}
						return
					}

					msg.Ack(false)
				}()
			}
		}

		log.Println("🔄 Restarting consumer...")
		time.Sleep(2 * time.Second)
	}
}
