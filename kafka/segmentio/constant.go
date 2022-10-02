package segmentio

import "log"

var (
	Topic     = "test4"
	Group     = "group-test4"
	Partition = 3
	Brokers   = []string{"localhost:9092"}
)

type KafkaWriteConfig struct {
	Brokers []string
	Topic   string
	Log     *log.Logger
}

type KafkaReaderConfig struct {
	Brokers   []string
	Topic     string
	Log       *log.Logger
	Partition int
}
