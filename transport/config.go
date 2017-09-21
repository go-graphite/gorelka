package transport

import (
	"sync"
	"time"

	"github.com/go-graphite/g2mt/distribution"
	"github.com/Shopify/sarama"
)

type Config struct {
	Name                  string                 `yaml:"Name"`
	Compression           string                 `yaml:"Compression"`
	Brokers               []string               `yaml:"Brokers"`
	RequiredAcks          sarama.RequiredAcks    `yaml:"RequiredAcks"`
	RetryMax              int                    `yaml:"RetryMax"`
	Topic                 string                 `yaml:"Topic"`
	Shards                int                    `yaml:"Shards"`
	DistributionAlgorithm distribution.Algorithm `yaml:"DistributionAlgorithm"`
	FlushFrequency        time.Duration          `yaml:"FlushFrequency"`
	ChannelBufferSize     int                    `yaml:"ChannelBufferSize"`
	Partition             int32                  `yaml:"Partition"`
	//	OrganisationID    int
}

var ProducerMessagePool = sync.Pool{
	New: func() interface{} {
		return &sarama.ProducerMessage{}
	},
}

func getSaramaProducer(topic string, partition int32, value []byte) *sarama.ProducerMessage {
	msg := ProducerMessagePool.Get().(*sarama.ProducerMessage)
	msg.Topic = topic
	msg.Partition = partition
	msg.Value = sarama.ByteEncoder(value)

	return msg
}
