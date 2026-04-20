package messaging

import (
	"encoding/json"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

func (r RabbitBroker) publishWithRetry(
	key string,
	payload amqp.Publishing,
	mandatory bool,
	immediate bool,
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

		err = ch.Publish(
			r.Exchange,
			key,
			mandatory,
			immediate,
			payload,
		)
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
	body, err := json.Marshal(payload)

	if err != nil {
		return err
	}

	publishing := amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	}

	mandatory := false
	immediate := false

	return r.publishWithRetry(key, publishing, mandatory, immediate, 5)
}

func (r RabbitBroker) PublishRaw(
	key string,
	mandatory bool,
	immediate bool,
	payload amqp.Publishing,
) error {
	return r.publishWithRetry(key, payload, mandatory, immediate, 5)
}

func (r RabbitBroker) MandatoryPublish(key string, payload any) error {
	body, err := json.Marshal(payload)

	if err != nil {
		return err
	}

	publishing := amqp.Publishing{
		ContentType: "application/json",
		Body:        body,
	}

	mandatory := true
	immediate := false

	return r.publishWithRetry(key, publishing, mandatory, immediate, 5)
}
