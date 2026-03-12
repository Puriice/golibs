package messaging

type RabbitBroker struct {
	*RabbitMQ
	Exchange string
}

func (r RabbitMQ) NewBroker(exchange string) (*RabbitBroker, error) {
	err := r.Channel.ExchangeDeclare(
		exchange,
		"topic",
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
