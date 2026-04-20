package messaging

import "github.com/rabbitmq/amqp091-go"

type BrokerKind string

const (
	Fanout BrokerKind = "fanout"
	Direct BrokerKind = "direct"
	Topic  BrokerKind = "topic"
)

type BrokerConfig struct {
	RabbitConfig
	Kind BrokerKind
	Args amqp091.Table
}

type RabbitBroker struct {
	RabbitMQ *RabbitMQ
	Exchange string
}

func NewBrokerConfig() BrokerConfig {
	return BrokerConfig{
		RabbitConfig: NewConfig(),
		Kind:         Topic,
		Args:         nil,
	}
}

func (r *RabbitMQ) NewBroker(exchange string) (*RabbitBroker, error) {
	return r.NewBrokerWithConfig(exchange, NewBrokerConfig())
}

func (r *RabbitMQ) NewBrokerWithConfig(exchange string, config BrokerConfig) (*RabbitBroker, error) {
	ch, err := r.getChannel()
	if err != nil {
		return nil, err
	}

	err = ch.ExchangeDeclare(
		exchange,
		string(config.Kind),
		config.Durable,
		config.AutoDelete,
		false,
		config.NoWait,
		config.Args,
	)
	if err != nil {
		return nil, err
	}

	return &RabbitBroker{
		RabbitMQ: r,
		Exchange: exchange,
	}, nil
}
