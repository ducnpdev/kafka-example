package segmentio

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/segmentio/kafka-go"
)

type KafkaReaderInstance struct {
	Reader         *kafka.Reader
	FetchAndCommit bool
}

func NewReaderInstance(cfg KafkaReaderConfig) *kafka.Reader {
	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: cfg.Brokers,
		Topic:   cfg.Topic,
		Logger:  cfg.Log,
		GroupID: Group,
	})
}

func (k *KafkaReaderInstance) Consume(ctx context.Context) {
	if k.Reader == nil {
		return
	}
	if k.FetchAndCommit {
		k.ConsumeFetch(ctx)
		return
	}
	k.ConsumeRead(ctx)
}

// waiting commit single message
func (k *KafkaReaderInstance) ConsumeFetch(ctx context.Context) {
	k.Reader.Config().Logger.Printf("fetch message and single commit")

	for {
		m, err := k.Reader.FetchMessage(ctx)

		if err != nil {
			k.Reader.Config().Logger.Printf("FetchMessage err: %s", err.Error())
		}
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		if err := k.Reader.CommitMessages(ctx, m); err != nil {
			k.Reader.Config().Logger.Printf("failed to commit messages: %s", err.Error())
		}
	}
}

// block until commit mesage
func (k *KafkaReaderInstance) ConsumeRead(ctx context.Context) {
	k.Reader.Config().Logger.Printf("consume read all message")

	for {
		msg, err := k.Reader.ReadMessage(ctx)
		if err != nil {
			k.Reader.Config().Logger.Printf("could not read message %s", err.Error())
		} else {
			k.Reader.Config().Logger.Printf("received %s", string(msg.Value))
		}
	}
}

func MainConsume(ctx context.Context) {
	readerIns := NewReaderInstance(KafkaReaderConfig{
		Brokers: Brokers,
		Topic:   Topic,
		Log:     log.New(os.Stdout, "kafka-reader: ", 0),
		// Partition: 2,
	})
	kafkaReader := KafkaReaderInstance{
		Reader:         readerIns,
		FetchAndCommit: false,
	}
	kafkaReader.Consume(ctx)

}
