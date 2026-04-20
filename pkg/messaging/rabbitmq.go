package messaging

import (
	"context"
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

	ctx    context.Context
	cancel context.CancelFunc
}

func NewRabbitMQ(ctx context.Context, url string) (*RabbitMQ, error) {
	cctx, cancel := context.WithCancel(ctx)

	r := &RabbitMQ{
		url:    url,
		ctx:    cctx,
		cancel: cancel,
	}

	if err := r.connect(); err != nil {
		return nil, err
	}

	go r.handleReconnect()

	return r, nil
}

func (r *RabbitMQ) connect() error {
	r.mu.Lock()
	defer r.mu.Unlock()

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
		case <-r.ctx.Done():
			log.Println("🛑 Stopping reconnect loop")
			return

		case err := <-connClose:
			log.Println("❌ Connection closed:", err)

		case err := <-chClose:
			log.Println("❌ Channel closed:", err)
		}

		log.Println("🔄 Reconnecting...")

	loop:
		for {
			select {
			case <-r.ctx.Done():
				log.Println("🛑 Stop reconnecting")
				return

			case <-time.After(3 * time.Second):
				if err := r.connect(); err != nil {
					log.Println("Reconnect failed:", err)
					continue
				}

				log.Println("✅ Reconnected")
				break loop
			}
		}
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

func (r *RabbitMQ) NewChannel() (*amqp.Channel, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.conn == nil || r.conn.IsClosed() {
		return nil, fmt.Errorf("connection not available")
	}

	return r.conn.Channel()
}

func (r *RabbitMQ) Channel() *amqp.Channel {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.channel
}

func (r *RabbitMQ) Shutdown() {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.cancel()

	if r.channel != nil {
		r.channel.Close()
	}

	if r.conn != nil {
		r.conn.Close()
	}
}
