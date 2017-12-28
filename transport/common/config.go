package common

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
type Transport int

const (
	JsonEncoding OutputEncoding = iota
	ProtobufEncoding
	GraphiteLineEncoding

	TCP   Transport = iota
	UDP
	Kafka
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

var supportedTransports = map[string]Transport{
	"tcp":   TCP,
	"udp":   UDP,
	"kafka": Kafka,
}


func (e *Transport) MarshalJSON() ([]byte, error) {
	for k, v := range supportedTransports {
		if v == *e {
			return json.Marshal(k)
		}
	}
	return nil, fmt.Errorf("unknown transport")
}

func (e *Transport) UnmarshalJSON(data []byte) error {
	var transport string
	var ok bool
	err := json.Unmarshal(data, &transport)
	if err != nil {
		return err
	}

	if *e, ok = supportedTransports[strings.ToLower(transport)]; !ok {
		return fmt.Errorf("unknown transport '%s', supported transport: %v", transport, supportedTransports)
	}

	return nil
}

func (e Transport) String() string {
	for k, v := range supportedTransports {
		if e == v {
			return k
		}
	}

	return "unknown"
}

type TLSConfig struct {
	Enabled bool `yaml:"Enabled"`
	SkipInsecureCerts bool `yaml:"SkipInsecureCerts"`
}

type Config struct {
	// Common
	Name                  string                 `yaml:"Name"`
	Type                  Transport              `yaml:"TransportType"`
	Compression           string                 `yaml:"Compression"`
	DistributionAlgorithm distribution.Algorithm `yaml:"DistributionAlgorithm"`
	FlushFrequency        time.Duration          `yaml:"FlushFrequency"`
	Encoding              OutputEncoding         `yaml:"OutputEncoding"`
	ChannelBufferSize     int                    `yaml:"ChannelBufferSize"`
	TLS                   TLSConfig              `yaml:"TLS"`
	Buffered              bool                   `yaml:"Buffered"`
	// Kafka Transport
	Brokers               []string               `yaml:"Brokers"`
	RequiredAcks          sarama.RequiredAcks    `yaml:"RequiredAcks"`
	RetryMax              int                    `yaml:"RetryMax"`
	Topic                 string                 `yaml:"Topic"`
	Shards                int                    `yaml:"Shards"`
	Partition             int32                  `yaml:"Partition"`
	//	OrganisationID    int
	// TCP Transport
	Servers	[]string
	CompressionLevel int `yaml:"CompressionLevel"`
}

var ProducerMessagePool = sync.Pool{
	New: func() interface{} {
		return &sarama.ProducerMessage{}
	},
}

func GetSaramaProducer(topic string, partition int32, value []byte) *sarama.ProducerMessage {
	msg := ProducerMessagePool.Get().(*sarama.ProducerMessage)
	msg.Topic = topic
	msg.Partition = partition
	msg.Value = sarama.ByteEncoder(value)

	return msg
}
