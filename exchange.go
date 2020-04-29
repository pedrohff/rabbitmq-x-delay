package rabbitmq_x_delay

import "github.com/streadway/amqp"

/*
Declares an exchange following the necessary pattern for a delay exchange.
The exchange kind must be "x-delayed-message" so the plugin recognizes it as one of its type,
still there's the need to declare how the messages key will behave, so there's the need to declare
the key type as part of the extra args as in "x-delayed-type".

The remaining properties were left fixed, as it's highly recommended that they're left this way,
explore the channel.ExchangeDeclare method docs for better understanding
*/
func CreateDefaultDelayExchange(exchangeName string, exchangeType string, channel *amqp.Channel) error {
	return channel.ExchangeDeclare(
		exchangeName,
		"x-delayed-message",
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-delayed-type": exchangeType,
		},
	)
}
