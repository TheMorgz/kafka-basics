package main

import (
	"log"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	config := &kafka.ConfigMap{
		"bootstrap.servers": "localhost:9092",
		"acks":              "all",
	}

	producer, err := kafka.NewProducer(config)
	if err != nil {
		log.Fatalf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	// Create delivery channel for async production
	deliveryChan := make(chan kafka.Event, 10000)
	topic := "test"

	err = producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     &topic,
			Partition: kafka.PartitionAny,
		},
		Value: []byte("hello world!"),
	}, deliveryChan)

	if err != nil {
		log.Fatalf("Failed to produce message: %v", err)
	}

	// Wait for message delivery
	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		log.Printf("Failed to deliver message: %v\n", m.TopicPartition.Error)
	} else {
		log.Printf("Produced message to topic %s partition [%d] @ offset %v\n",
			*m.TopicPartition.Topic,
			m.TopicPartition.Partition,
			m.TopicPartition.Offset)
	}

	// Flush any remaining messages
	producer.Flush(15 * 1000)
}
