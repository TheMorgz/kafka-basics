package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
)

func main() {
	config := &kafka.ConfigMap{
		"bootstrap.servers":       "localhost:9092",
		"group.id":                "test-group",
		"auto.offset.reset":       "earliest",
		"enable.auto.commit":      true,
		"auto.commit.interval.ms": 5000,
	}

	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		log.Fatalf("Error creating consumer: %s\n", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	topics := []string{"test"}
	err = consumer.SubscribeTopics(topics, func(c *kafka.Consumer, event kafka.Event) error {
		switch e := event.(type) {
		case kafka.AssignedPartitions:
			log.Printf("Assigned partitions: %v\n", e.Partitions)
			return c.Assign(e.Partitions)
		case kafka.RevokedPartitions:
			log.Printf("Revoked partitions: %v\n", e.Partitions)
			return c.Unassign()
		}
		return nil
	})

	if err != nil {
		log.Fatalf("Failed to subscribe: %v\n", err)
	}

	go func() {
		<-sigChan
		log.Println("Initiating shutdown...")
		cancel()
	}()

	for {
		select {
		case <-ctx.Done():
			log.Println("Shutting down consumer gracefully...")
			consumer.Close()
			return
		default:
			msg, err := consumer.ReadMessage(time.Second)
			if err != nil {
				if !err.(kafka.Error).IsTimeout() {
					log.Printf("Error reading message: %v\n", err)
				}
				continue
			}

			if msg != nil {
				log.Printf("Received message: Topic=%s Partition=%d Offset=%d Key=%s Value=%s\n",
					*msg.TopicPartition.Topic,
					msg.TopicPartition.Partition,
					msg.TopicPartition.Offset,
					string(msg.Key),
					string(msg.Value))

				_, err = consumer.CommitMessage(msg)
				if err != nil {
					log.Printf("Failed to commit message: %v\n", err)
				}
			}
		}
	}
}
