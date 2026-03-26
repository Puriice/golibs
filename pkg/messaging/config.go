package messaging

type RabbitConfig struct {
	Durable    bool
	AutoDelete bool
	NoWait     bool
}

func NewConfig() RabbitConfig {
	return RabbitConfig{
		Durable:    true,
		AutoDelete: false,
		NoWait:     false,
	}
}
