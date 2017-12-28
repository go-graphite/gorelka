package common

import (
	"sync"
	"time"

	"encoding/json"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/go-graphite/g2mt/distribution"
	"strings"
)

type OutputEncoding int

const (
	GraphiteLineEncoding OutputEncoding = iota
	ProtobufEncoding
	JsonEncoding
)

var supportedEncodings = map[string]OutputEncoding{
	"json":     JsonEncoding,
	"protobuf": ProtobufEncoding,
	"graphite": GraphiteLineEncoding,
}

func (e *OutputEncoding) MarshalJSON() ([]byte, error) {
	for k, v := range supportedEncodings {
		if v == *e {
			return json.Marshal(k)
		}
	}
	return nil, fmt.Errorf("unknown encoding '%s', supported encodings: %v", *e, supportedEncodings)
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

func (e OutputEncoding) String() string {
	for k, v := range supportedEncodings {
		if e == v {
			return k
		}
	}

	return "unknown"
}

type Transport int

const (
	TCP Transport = iota
	UDP
	Kafka
)

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
	return nil, fmt.Errorf("unknown transport '%v', supported transports: %v", *e, supportedTransports)
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
	Enabled           bool `json:"Enabled"`
	SkipInsecureCerts bool `json:"SkipInsecureCerts"`
}

type Config struct {
	// Common
	Name                  string                 `json:"Name"`
	Type                  Transport              `json:"Type"`
	Compression           string                 `json:"Compression"`
	DistributionAlgorithm distribution.Algorithm `json:"DistributionAlgorithm"`
	FlushFrequency        time.Duration          `json:"FlushFrequency"`
	Encoding              OutputEncoding         `json:"Encoding"`
	ChannelBufferSize     int                    `json:"ChannelBufferSize"`
	TLS                   TLSConfig              `json:"TLS"`
	Buffered              bool                   `json:"Buffered"`
	// Kafka Transport
	Brokers      []string            `json:"Brokers"`
	RequiredAcks sarama.RequiredAcks `json:"RequiredAcks"`
	RetryMax     int                 `json:"RetryMax"`
	Topic        string              `json:"Topic"`
	Shards       int                 `json:"Shards"`
	Partition    int32               `json:"Partition"`
	//	OrganisationID    int
	// TCP Transport
	Servers          []string
	CompressionLevel int `json:"CompressionLevel"`
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
