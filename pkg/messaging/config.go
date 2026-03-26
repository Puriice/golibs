package messaging

type RabbitConfig struct {
	Durable    bool
	AutoDelete bool
	NoWait     bool
}
