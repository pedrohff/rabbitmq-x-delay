package rabbitmq_x_delay

import (
	"errors"
	"github.com/google/uuid"
	"github.com/streadway/amqp"
	"math"
	"time"
)

const RetryLimit = 12

type DelayFunc func(retryCounter int32) (delayMillis int64)

type RescheduleOptions struct {
	Channel      *amqp.Channel
	ExchangeName string
	RoutingKey   string
}

/*
Increments delays exponentially
*/
func DefaultDelayFunc(retryCounter int32) int64 {
	return int64(math.Pow(2, float64(retryCounter)) * 1000)
}

/*
Auxiliary method for rescheduling using the DefaultDelayFunc
*/
func RescheduleMessage(delivery amqp.Delivery, opts *RescheduleOptions) error {
	return RescheduleMessageWithDelayFunc(DefaultDelayFunc, delivery, opts)
}

/*
Behaves the same as RescheduleMessageDeclaringDelay, working with a DelayFunc
*/
func RescheduleMessageWithDelayFunc(delayFunc DelayFunc, delivery amqp.Delivery, opts *RescheduleOptions) error {
	var retryCounter int32
	if mapVal, ok := delivery.Headers["retries"]; ok {
		retryCounter = mapVal.(int32)
	}
	return RescheduleMessageDeclaringDelay(delayFunc(retryCounter), delivery, opts)
}

/*
Copies a message body and headers, updating the headers used by the library: "x-delay", "retries" and "message-uuid".
Once setup, the message is encapsulated in an amqp.Publishing and resent to the exchange.

A message will be available again in the queue after the given delayMillis

Delivery represents the message that should be rescheduled

Channels must be active for rescheduling

The exchangeName must follow the one given on the CreateDefaultDelayExchange method

The routingKey is present inside the amqp.Delivery structure, still it does not always seem to match. This will also
allow custom routing
*/
func RescheduleMessageDeclaringDelay(delayMillis int64, delivery amqp.Delivery, opts *RescheduleOptions) error {
	if opts == nil {
		return errors.New("reschedule options are invalid")
	}

	var retries int32

	if delivery.Headers != nil {
		if mapVal, ok := delivery.Headers["retries"]; ok {
			retries = mapVal.(int32)
			retries = retries + 1
		}
	}

	if retries > RetryLimit {
		return errors.New("retry limit")
	}

	headers := setupHeaders(delayMillis, delivery.Headers, retries)

	publishing := amqp.Publishing{
		DeliveryMode: amqp.Persistent,
		Timestamp:    time.Now(),
		ContentType:  delivery.ContentType,
		Body:         delivery.Body,
		Headers:      headers,
	}

	if opts.Channel == nil {
		return errors.New("RabbitMQ not available")
	}

	// Both flags bellow set as false helps guaranteeing the message will be delivered
	return opts.Channel.Publish(
		opts.ExchangeName,
		opts.RoutingKey,
		false,
		false,
		publishing,
	)
}

func setupHeaders(delayMillis int64, deliveryHeaders amqp.Table, retries int32) amqp.Table {
	var headers amqp.Table
	if deliveryHeaders != nil && len(deliveryHeaders) != 0 {
		headers = deliveryHeaders
	} else {
		headers = make(amqp.Table)
	}

	headers["x-delay"] = delayMillis
	headers["retries"] = retries

	if _, ok := headers["message-uuid"]; !ok {
		headers["message-uuid"] = uuid.New().String()
	}
	return headers
}
