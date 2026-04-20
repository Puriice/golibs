package messaging

import (
	"fmt"
	"log"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMQ struct {
	url string

	conn    *amqp.Connection
	channel *amqp.Channel

	mu sync.Mutex
}

func NewRabbitMQ(url string) (*RabbitMQ, error) {
	r := &RabbitMQ{url: url}

	if err := r.connect(); err != nil {
		return nil, err
	}

	go r.handleReconnect()

	return r, nil
}

func (r *RabbitMQ) connect() error {
	conn, err := amqp.DialConfig(r.url, amqp.Config{
		Heartbeat: 10 * time.Second,
	})
	if err != nil {
		return err
	}

	ch, err := conn.Channel()
	if err != nil {
		conn.Close()
		return err
	}

	r.conn = conn
	r.channel = ch

	log.Println("✅ Connected to RabbitMQ")

	return nil
}

func (r *RabbitMQ) handleReconnect() {
	for {
		connClose := make(chan *amqp.Error)
		chClose := make(chan *amqp.Error)

		r.conn.NotifyClose(connClose)
		r.channel.NotifyClose(chClose)

		select {
		case err := <-connClose:
			log.Println("❌ Connection closed:", err)
		case err := <-chClose:
			log.Println("❌ Channel closed:", err)
		}

		log.Println("🔄 Reconnecting...")

		for {
			time.Sleep(3 * time.Second)

			if err := r.connect(); err != nil {
				log.Println("Reconnect failed:", err)
				continue
			}

			break
		}

		log.Println("✅ Reconnected")
	}
}

func (r *RabbitMQ) getChannel() (*amqp.Channel, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.channel == nil {
		return nil, fmt.Errorf("channel not initialized")
	}

	return r.channel, nil
}

func (r *RabbitMQ) Channel() *amqp.Channel {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.channel
}

func (r *RabbitMQ) Shutdown() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.channel.Close()
	r.conn.Close()
}
