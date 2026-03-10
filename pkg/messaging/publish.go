package messaging

import (
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (r RabbitBroker) Publish(key string, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	return r.Channel.Publish(
		r.Exchange,
		key,
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
}
