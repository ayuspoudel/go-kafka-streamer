package worker

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/IBM/sarama"
)

func worker() {
	topic := "comments"
	consumerClient, err := connectConsumer([]string{"localhost:29092"})
	if err != nil {
		panic(err)
	}

	partitionConsumer, err := consumerClient.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	fmt.Println("Consumer started")
	sigChain := make(chan os.Signal, 1)
	signal.Notify(sigChain, syscall.SIGINT, syscall.SIGTERM)
	msgCount := 0
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-partitionConsumer.Errors():
				fmt.Println("Error:", err)
			case msg := <-partitionConsumer.Messages():
				msgCount++
				fmt.Printf(
					"Received message\n Count: %d | Topic: (%s) | Message: (%s)\n",
					msgCount, msg.Topic, string(msg.Value),
				)
			case <-sigChain:
				fmt.Println("Interrupt detected")
				doneCh <- struct{}{}
				return
			}
		}
	}()

	<-doneCh

	fmt.Println("Processed", msgCount, "messages")

	err = partitionConsumer.Close()
	if err != nil {
		panic(err)
	}

	err = consumerClient.Close()
	if err != nil {
		panic(err)
	}
}

func connectConsumer(brokersUrl []string) (sarama.Consumer, error) {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true
	conn, err := sarama.NewConsumer(brokersUrl, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}
