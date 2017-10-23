package transport

import (
	"sync"
	"time"

	"github.com/go-graphite/g2mt/distribution"
	"github.com/Shopify/sarama"
	"encoding/json"
	"strings"
	"fmt"
)

type OutputEncoding int

const (
	JsonEncoding OutputEncoding = iota
	ProtobufEncoding
	GraphiteLineEncoding
)

var supportedEncodings = map[string]OutputEncoding{
	"json": JsonEncoding,
	"protobuf": ProtobufEncoding,
	"graphite": GraphiteLineEncoding,
}


func (e *OutputEncoding) MarshalJSON() ([]byte, error) {
	for k, v := range supportedEncodings {
		if v == *e {
			return json.Marshal(k)
		}
	}
	return nil, fmt.Errorf("unknown encoding")
}

func (e *OutputEncoding) UnmarshalJSON(data []byte) error {
	var encoding string
	var ok bool
	err := json.Unmarshal(data, &encoding)
	if err != nil {
		return err
	}

	if *e, ok = supportedEncodings[strings.ToLower(encoding)]; !ok {
		return fmt.Errorf("unknown encoding '%s', supported encodings: %v", encoding, supportedEncodings)
	}

	return nil
}

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
	Encoding              OutputEncoding         `yaml:"OutputEncoding"`
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
