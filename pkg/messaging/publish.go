package messaging

import (
	"encoding/json"

	amqp "github.com/rabbitmq/amqp091-go"
)

func publish(channel *amqp.Channel, exchange string, key string, mandatory bool, payload any) error {
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}

	immediate := false

	return channel.Publish(
		exchange,
		key,
		mandatory,
		immediate,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)

}

func (r RabbitBroker) Publish(key string, payload any) error {
	mandatory := false

	return publish(r.Channel, r.Exchange, key, mandatory, payload)
}

func (r RabbitBroker) MandatoryPublish(key string, payload any) error {
	mandatory := true

	return publish(r.Channel, r.Exchange, key, mandatory, payload)
}
