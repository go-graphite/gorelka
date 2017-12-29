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

func (e *OutputEncoding) FromString(s string) error {
	var ok bool
	if *e, ok = supportedEncodings[strings.ToLower(s)]; !ok {
		return fmt.Errorf("unknown transport '%s', supported transport: %v", s, supportedEncodings)
	}

	return nil
}

func (e *OutputEncoding) UnmarshalJSON(data []byte) error {
	var encoding string
	err := json.Unmarshal(data, &encoding)
	if err != nil {
		return err
	}

	return e.FromString(encoding)
}

func (e *OutputEncoding) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var encoding string
	err := unmarshal(&encoding)
	if err != nil {
		return err
	}

	return e.FromString(encoding)
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
	Blackhole
)

var supportedTransports = map[string]Transport{
	"tcp":       TCP,
	"udp":       UDP,
	"kafka":     Kafka,
	"blackhole": Blackhole,
}

func (e *Transport) MarshalJSON() ([]byte, error) {
	for k, v := range supportedTransports {
		if v == *e {
			return json.Marshal(k)
		}
	}
	return nil, fmt.Errorf("unknown transport '%v', supported transports: %v", *e, supportedTransports)
}

func (e *Transport) FromString(s string) error {
	var ok bool
	if *e, ok = supportedTransports[strings.ToLower(s)]; !ok {
		return fmt.Errorf("unknown transport '%s', supported transport: %v", s, supportedTransports)
	}

	return nil
}

func (e *Transport) UnmarshalJSON(data []byte) error {
	var transport string
	err := json.Unmarshal(data, &transport)
	if err != nil {
		return err
	}

	return e.FromString(transport)
}

func (e *Transport) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var transport string
	err := unmarshal(&transport)
	if err != nil {
		return err
	}

	return e.FromString(transport)
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

type ConfigForFile struct {
	// Common
	Type                  string        `json:"Type"`
	Compression           string        `json:"Compression"`
	DistributionAlgorithm string        `json:"DistributionAlgorithm"`
	Encoding              string        `json:"Encoding"`
	FlushFrequency        time.Duration `json:"FlushFrequency"`
	ChannelBufferSize     int           `json:"ChannelBufferSize"`
	TLS                   TLSConfig     `json:"TLS"`
	Buffered              bool          `json:"Buffered"`
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

// TODO: Find out reason why Viper ignores custom unmarshal functions and remove workaround from below
type Config struct {
	// Common
	Name                  string `json:"Name"`
	Type                  Transport
	Compression           string `json:"Compression"`
	DistributionAlgorithm distribution.Algorithm
	Encoding              OutputEncoding
	FlushFrequency        time.Duration `json:"FlushFrequency"`
	ChannelBufferSize     int           `json:"ChannelBufferSize"`
	TLS                   TLSConfig     `json:"TLS"`
	Buffered              bool          `json:"Buffered"`
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

func (c *Config) FromParsed(cfg ConfigForFile) error {
	err := c.Type.FromString(cfg.Type)
	if err != nil {
		return err
	}
	err = c.Encoding.FromString(cfg.Encoding)
	if err != nil {
		return err
	}
	err = c.DistributionAlgorithm.FromString(cfg.DistributionAlgorithm)
	if err != nil {
		return err
	}

	c.Compression = cfg.Compression
	c.FlushFrequency = cfg.FlushFrequency
	c.ChannelBufferSize = cfg.ChannelBufferSize
	c.TLS = cfg.TLS
	c.Buffered = cfg.Buffered
	c.Brokers = cfg.Brokers
	c.RequiredAcks = cfg.RequiredAcks
	c.RetryMax = cfg.RetryMax
	c.Topic = cfg.Topic
	c.Shards = cfg.Shards
	c.Partition = cfg.Partition
	c.Servers = cfg.Servers
	c.CompressionLevel = cfg.CompressionLevel
	return nil
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
