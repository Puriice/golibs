package messaging

import amqp "github.com/rabbitmq/amqp091-go"

type RabbitMQ struct {
	*amqp.Connection
	Channel *amqp.Channel
}

type RabbitBroker struct {
	*RabbitMQ
	Exchange string
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

func (r RabbitMQ) Broker(exchange string) (*RabbitBroker, error) {
	err := r.Channel.ExchangeDeclare(
		exchange,
		"direct",
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		return nil, err
	}

	return &RabbitBroker{
		RabbitMQ: &r,
		Exchange: exchange,
	}, nil
}
