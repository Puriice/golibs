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

	Binding bool
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

		Binding: true,
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

	listener := &RabbitListener{
		rabbit:    r.RabbitMQ,
		queueName: q.Name,
		exchange:  r.Exchange,
		config:    config,
	}

	if config.Binding {
		err = listener.Bind()
	}

	return listener, err
}

func (l *RabbitListener) Bind() error {
	ch, err := l.rabbit.getChannel()
	if err != nil {
		return err
	}

	for _, key := range l.config.Keys {
		err := ch.QueueBind(
			l.queueName,
			key,
			l.exchange,
			false,
			nil,
		)
		if err != nil {
			return err
		}
	}

	return nil
}

func (l *RabbitListener) Subscribe(ctx context.Context, handler func([]byte) error) error {
	for {
		if ctx.Err() != nil {
			return nil
		}

		ch, err := l.rabbit.NewChannel()

		if err != nil {
			log.Println("Get channel failed:", err)
			if !sleepCtx(ctx, 2*time.Second) { // 👈 context-aware sleep
				return nil
			}
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
			if !sleepCtx(ctx, 2*time.Second) { // 👈 context-aware sleep
				return nil
			}
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
			if !sleepCtx(ctx, 2*time.Second) { // 👈 context-aware sleep
				return nil
			}
			continue
		}

		err = ch.Qos(l.config.PrefetchCount, l.config.PrefetchSize, false)
		if err != nil {
			log.Println("Qos failed:", err)
			if !sleepCtx(ctx, 2*time.Second) { // 👈 context-aware sleep
				return nil
			}
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
			if !sleepCtx(ctx, 2*time.Second) { // 👈 context-aware sleep
				return nil
			}
			continue
		}

		log.Println("📥 Consumer started")

		errChan := make(chan *amqp.Error)
		ch.NotifyClose(errChan)

		channelDead := false

	loop:
		for {
			select {
			case <-ctx.Done():
				_ = ch.Cancel(consumerTag, false)
				return nil

			case err := <-errChan:
				log.Println("❌ Channel closed:", err)
				channelDead = true
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

						if !channelDead { // 👈 guard every ack/nack
							if msg.Redelivered {
								msg.Nack(false, false)
							} else {
								msg.Nack(false, true)
							}
						}

						return
					}

					msg.Ack(false)
				}()
			}
		}

		log.Println("🔄 Restarting consumer...")
		if !sleepCtx(ctx, 2*time.Second) { // 👈 context-aware sleep
			return nil
		}
	}
}

func sleepCtx(ctx context.Context, d time.Duration) bool {
	select {
	case <-ctx.Done():
		return false
	case <-time.After(d):
		return true
	}
}
