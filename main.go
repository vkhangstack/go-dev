package main

import (
	"encoding/json"
	"log"

	"github.com/streadway/amqp"
)

type RideRequest struct {
	PassengerName string
	PickupAddress string
	Destination   string
}

type RideOffer struct {
	DriverName string
	PickupETA  int
}

type RabbitMQ struct {
	conn    *amqp.Connection
	channel *amqp.Channel
}

func NewRabbitMQ(url string) (*RabbitMQ, error) {
	conn, err := amqp.Dial(url)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()

	if err != nil {
		return nil, err
	}

	return &RabbitMQ{conn: conn, channel: channel}, nil
}

func (rmq *RabbitMQ) PublishRideRequest(request *RideRequest, exchange string) error {
	body, err := json.Marshal(request)
	if err != nil {
		return err
	}

	err = rmq.channel.Publish(
		exchange,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		return nil
	}

	log.Printf("RabbitMQ publish request: %s", body)

	return nil
}
func (rmq *RabbitMQ) ConsumeRideRequest(queueName string, callback func(*RideOffer) error) error {
	msgs, err := rmq.channel.Consume(
		queueName,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}

	go func() {
		for msg := range msgs {
			offer := &RideOffer{}
			err := json.Unmarshal(msg.Body, offer)
			if err != nil {
				log.Printf("Error parsing ride offer: %s", err)
			} else {
				err = callback(offer)
				if err != nil {
					log.Printf("Error processing ride offer: %s", err)
				}
			}
		}
	}()

	return nil
}

func (rmq *RabbitMQ) PublishRideOffer(offer *RideOffer, exchange string) error {
	body, err := json.Marshal(offer)
	if err != nil {
		return err
	}

	err = rmq.channel.Publish(
		exchange,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json",
			Body:        body,
		},
	)
	if err != nil {
		return err
	}

	log.Printf("Ride offer sent: %s", body)

	return nil
}

func main() {
	// connect rabbitmq
	rmq, err := NewRabbitMQ("amqps://khangdev:khangdev@192.168.23.1:5672/")

	if err != nil {
		log.Fatalf("Failed to connect to RabbitMQ: %s", err)
	}

	request := &RideRequest{
		PassengerName: "khangdev",
		PickupAddress: "Vietnam",
		Destination:   "Khangdev",
	}

	err = rmq.PublishRideRequest(request, "ride_requests")
	if err != nil {
		log.Fatalf("Failed to publish ride request: %s", err)
	}

	// Consume rider offer
	err = rmq.ConsumeRideRequest("ride_offers", func(offer *RideOffer) error {
		log.Printf("Received ride offer from %s with ETA %d", offer.DriverName, offer.PickupETA)
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to consume ride offer: $s", err)
	}

	// Publish a ride offer

	offer := &RideOffer{
		DriverName: "Bob",
		PickupETA:  5,
	}
	err = rmq.PublishRideOffer(offer, "ride_offers")
	if err != nil {
		log.Fatalf("Failed to publish ride offer: %s", err)
	}
	// awaiting
	forever := make(chan bool)
	log.Printf("Waiting for ride requests and offers...")
	<-forever
}
