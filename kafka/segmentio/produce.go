package segmentio

import (
	"context"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/segmentio/kafka-go"
)

type KafkaWriteInstance struct {
	Write *kafka.Writer
}

func NewWriteInstance(cfg KafkaWriteConfig) *kafka.Writer {
	return kafka.NewWriter(kafka.WriterConfig{
		Brokers: cfg.Brokers,
		Topic:   cfg.Topic,
		Logger:  cfg.Log,
	})
}

func (k *KafkaWriteInstance) Produce(ctx context.Context, preStr string) {
	if k.Write == nil {
		return
	}
	i := 1

	for {
		key := []byte(strconv.Itoa(i))
		message := []byte(preStr + strconv.Itoa(i))
		err := k.Write.WriteMessages(ctx, kafka.Message{
			Key:   key,
			Value: message,
		})
		k.Write.Logger.Printf(" key %d, message %s", i, message)

		if err != nil {
			k.Write.Logger.Printf("error key %d, message %s", i, message)
		}
		i++
		time.Sleep(time.Second * 10)
	}
}

func MainProduce(ctx context.Context) {
	write := NewWriteInstance(KafkaWriteConfig{
		Brokers: Brokers,
		Topic:   Topic,
		Log:     log.New(os.Stdout, "kafka-writer: ", 0),
	})
	kafkaWrite := KafkaWriteInstance{
		Write: write,
	}
	for i := 0; i < 1; i++ {
		preStr := fmt.Sprintf("this is channel %d : value=", i)
		go kafkaWrite.Produce(ctx, preStr)
	}
}
