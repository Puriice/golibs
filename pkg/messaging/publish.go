package messaging

import (
	"encoding/json"
	"fmt"
	"time"

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

func (r RabbitBroker) publishWithRetry(
	key string,
	payload any,
	mandatory bool,
	maxAttempts int,
) error {
	var lastErr error

	for attempt := 1; attempt <= maxAttempts; attempt++ {
		conn := r.RabbitMQ.conn
		if conn == nil {
			lastErr = fmt.Errorf("no connection")
			time.Sleep(2 * time.Second)
			continue
		}

		ch, err := conn.Channel()
		if err != nil {
			lastErr = err
			time.Sleep(2 * time.Second)
			continue
		}

		err = publish(ch, r.Exchange, key, mandatory, payload)
		ch.Close()

		if err == nil {
			return nil
		}

		lastErr = err
		time.Sleep(time.Duration(attempt) * time.Second)
	}

	return fmt.Errorf("publish failed after %d attempts: %w", maxAttempts, lastErr)
}

func (r RabbitBroker) Publish(key string, payload any) error {
	return r.publishWithRetry(key, payload, false, 5)
}

func (r RabbitBroker) MandatoryPublish(key string, payload any) error {
	return r.publishWithRetry(key, payload, true, 5)
}
