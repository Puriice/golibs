package messaging

type RabbitBroker struct {
	*RabbitMQ
	Exchange string
}

func (r RabbitMQ) NewBroker(exchange string) (*RabbitBroker, error) {
	return r.NewBrokerWithConfig(exchange, NewConfig())
}

func (r RabbitMQ) NewBrokerWithConfig(exchange string, config RabbitConfig) (*RabbitBroker, error) {
	err := r.Channel.ExchangeDeclare(
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
		RabbitMQ: &r,
		Exchange: exchange,
	}, nil
}

func (r RabbitMQ) NewBroadcastBroker(exchange string) (*RabbitBroker, error) {
	return r.NewBroadcastBrokerWithConfig(exchange, NewConfig())
}

func (r RabbitMQ) NewBroadcastBrokerWithConfig(exchange string, config RabbitConfig) (*RabbitBroker, error) {
	err := r.Channel.ExchangeDeclare(
		exchange,
		"fanout",
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
		RabbitMQ: &r,
		Exchange: exchange,
	}, nil
}
