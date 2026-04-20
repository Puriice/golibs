package messaging

import (
	"encoding/json"
	"fmt"
	"log"
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
		ch, err := r.RabbitMQ.getChannel()
		if err != nil {
			lastErr = err
			log.Printf("❌ [%d/%d] Get channel failed: %v\n", attempt, maxAttempts, err)
		} else {
			err = publish(ch, r.Exchange, key, mandatory, payload)
			if err == nil {
				return nil // ✅ success
			}

			lastErr = err
			log.Printf("❌ [%d/%d] Publish failed: %v\n", attempt, maxAttempts, err)
		}

		time.Sleep(time.Duration(attempt*2) * time.Second)
	}

	return fmt.Errorf("publish failed after %d attempts: %w", maxAttempts, lastErr)
}

func (r RabbitBroker) Publish(key string, payload any) error {
	return r.publishWithRetry(key, payload, false, 5)
}

func (r RabbitBroker) MandatoryPublish(key string, payload any) error {
	return r.publishWithRetry(key, payload, true, 5)
}
