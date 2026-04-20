package messaging

type RabbitBroker struct {
	RabbitMQ *RabbitMQ
	Exchange string
}

func (r *RabbitMQ) NewBroker(exchange string) (*RabbitBroker, error) {
	return r.NewBrokerWithConfig(exchange, NewConfig())
}

func (r *RabbitMQ) NewBrokerWithConfig(exchange string, config RabbitConfig) (*RabbitBroker, error) {
	ch, err := r.getChannel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchange,
		"topic",
		config.Durable,
		config.AutoDelete,
		false,
		config.NoWait,
		nil,
	)
	if err != nil {
		return nil, err
	}

	return &RabbitBroker{
		RabbitMQ: r,
		Exchange: exchange,
	}, nil
}
