package messaging

import amqp "github.com/rabbitmq/amqp091-go"

type RabbitMQ struct {
	*amqp.Connection
	Channel *amqp.Channel
}

func NewRabbitMQ(url string) (*RabbitMQ, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	ch, err := conn.Channel()

	if err != nil {
		return nil, err
	}

	return &RabbitMQ{
		Connection: conn,
		Channel:    ch,
	}, nil
}

func (r RabbitMQ) Shutdown() {
	r.Channel.Close()
	r.Close()
}
