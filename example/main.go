package main

import (
	"fmt"
	"github.com/kardianos/service"
	rabbitmq_x_delay "github.com/pedrohff/rabbitmq-x-delay"
	"github.com/streadway/amqp"
	"log"
	"os"
	"time"
)

const (
	exchangename = "sample-default-exchange-delay"
	queuename    = "sample-queue"
	keyname      = "sample-queue-key"
)

func main() {
	svcConfig := &service.Config{
		Name:        "RabbitMQ tester",
		DisplayName: "RabbitMQTester",
		Description: "Service for testing rabbitmq",
	}

	prg := &program{}
	s, err := service.New(prg, svcConfig)
	if err != nil {
		log.Fatal(err)
	}
	logger, err := s.Logger(nil)
	if err != nil {
		log.Fatal(err)
	}
	err = s.Run()
	if err != nil {
		logger.Error(err)
	}
	fmt.Println("app ended")
}

func createRabbit() {
	rabbitConnection, err := amqp.DialConfig(
		"amqp://guest:guest@localhost:5672",
		amqp.Config{
			Heartbeat: 5 * time.Second,
			Vhost:     "/",
		},
	)

	if err != nil {
		fmt.Println("could not open connection")
		fmt.Println(err)
		return
	}
	go closeListener("connection closed", rabbitConnection.NotifyClose)

	rabbitChannel, err := rabbitConnection.Channel()
	if err != nil {
		fmt.Println("could not open channel", err)
		return
	}

	go closeListener("channel closed", rabbitChannel.NotifyClose)

	err = rabbitmq_x_delay.CreateDefaultDelayExchange(exchangename, "direct", rabbitChannel)
	if err != nil {
		fmt.Println("could not declare default exchange for delays", err)
		return
	}

	_, err = rabbitChannel.QueueDeclare(
		queuename,
		true,
		false,
		false,
		false,
		nil,
	)

	if err != nil {
		fmt.Println("could not declare queue", err, true)
		return
	}

	err = rabbitChannel.QueueBind(
		queuename,
		keyname,
		exchangename,
		false,
		nil,
	)

	if err != nil {
		fmt.Println("could not bind queue to exchange", err)
		return
	}

	hostname, _ := os.Hostname()
	deliveries, err := rabbitChannel.Consume(
		queuename,
		hostname,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		fmt.Println("could not start consumer", err)
		return
	}

	go func(deliveries <-chan amqp.Delivery) {
		for message := range deliveries {
			fmt.Println("new message!")
			for s, i := range message.Headers {
				fmt.Printf("\t%s: %v\n", s, i)
			}
			fmt.Println(string(message.Body))
			err = rabbitmq_x_delay.RescheduleMessage(message, &rabbitmq_x_delay.RescheduleOptions{
				Channel:      rabbitChannel,
				ExchangeName: exchangename,
				RoutingKey:   keyname,
			})
			if err != nil {
				fmt.Println("read error", err)
				message.Reject(false)
			} else {
				message.Ack(false)
			}
		}
	}(deliveries)
}

func closeListener(errMsg string, notifier func(chan *amqp.Error) chan *amqp.Error) {
	func() {
		errChan := make(chan *amqp.Error)
		for {
			reason, ok := <-notifier(errChan)
			if ok {
				fmt.Println(errMsg)
				fmt.Printf("%s\n", reason)
				fmt.Printf("\t[DynamicRabbitConnErr]%v\n", reason)
				return
			}
		}
	}()
}
