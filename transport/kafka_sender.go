package transport

import (
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"

	"fmt"
	"github.com/go-graphite/g2mt/carbon"
	"github.com/go-graphite/g2mt/distribution"
	"sync"
)

type KafkaSender struct {
	Config

	senderID int

	kafka        sarama.AsyncProducer
	exitChan     <-chan struct{}
	queues       []chan *carbon.Metric
	maxBatchSize int
	workers      int
	sendInterval time.Duration

	kafkaConfig *sarama.Config

	mightHaveDataToProcess chan struct{}
	logger                 *zap.Logger
	data                   metricsMap
	distributionFunc       distribution.Distribute
}

type metricsMap struct {
	sync.RWMutex
	data map[string]*carbon.Metric
}

func NewKafkaSender(c Config, exitChan <-chan struct{}, workers, maxBatchSize int, sendInterval time.Duration) (*KafkaSender, error) {
	if c.Shards <= 0 {
		return nil, fmt.Errorf("Invalid amount of shards (<=0), should be at least 1")
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = c.RequiredAcks
	config.Producer.Retry.Max = c.RetryMax
	config.Producer.Flush.Frequency = c.FlushFrequency
	config.ChannelBufferSize = c.ChannelBufferSize
	config.Producer.Partitioner = sarama.NewManualPartitioner

	switch strings.ToLower(c.Compression) {
	case "snappy":
		config.Producer.Compression = sarama.CompressionSnappy
	case "gzip":
		config.Producer.Compression = sarama.CompressionGZIP
	default:
		config.Producer.Compression = sarama.CompressionNone
	}

	var distributionFunc distribution.Distribute
	switch c.DistributionAlgorithm {
	case distribution.All:
		distributionFunc = distribution.NewAllDistribution(c.Topic)
	case distribution.JumpFNV1a:
		distributionFunc = distribution.NewJumpFNV1aDistribution(c.Topic, c.Shards)
	case distribution.FNV1a:
		distributionFunc = distribution.NewFNV1aDistribution(c.Topic, c.Shards)
	}

	sender := &KafkaSender{
		Config: c,

		kafkaConfig:            config,
		kafka:                  nil,
		exitChan:               exitChan,
		maxBatchSize:           maxBatchSize,
		sendInterval:           sendInterval,
		workers:                workers,
		mightHaveDataToProcess: make(chan struct{}),
		distributionFunc:       distributionFunc,

		logger: zapwriter.Logger("kafka"),
	}

	for i := 0; i < c.Shards; i++ {
		sender.queues = append(sender.queues, make(chan *carbon.Metric))
	}

	return sender, nil

}

func (k *KafkaSender) returnMessagesToPool() {
	select {
	case <-k.exitChan:
		return
	case p := <-k.kafka.Successes():
		ProducerMessagePool.Put(p)
	}
}

func (k *KafkaSender) GetName() string {
	return k.Name
}

func (k *KafkaSender) sendToKafka(payload *carbon.Payload) {
	if len(payload.Metrics) == 0 {
		return
	}

	k.logger.Info("Got some data to send")

	t0 := time.Now()
	data, err := payload.Marshal()
	if err != nil {
		k.logger.Error("Failed to marshal message",
			zap.Error(err),
		)
	}

	msg := getSaramaProducer(k.Topic, k.Partition, data)
	k.kafka.Input() <- msg

	l := 0
	for i := range payload.Metrics {
		l += len(payload.Metrics[i].Points)
	}
	speed := float64(l) / time.Since(t0).Seconds()
	k.logger.Info("finished sending data data",
		zap.Duration("runtime", time.Since(t0)),
		zap.Int("metrics", l),
		zap.Float64("speed", speed),
	)
}

func (k *KafkaSender) worker(queue chan *carbon.Metric) {
	metricsMap := make(map[string]*carbon.Metric)
	data := &carbon.Payload{}
	ticker := time.NewTicker(k.sendInterval)
	k.logger.Info("Worker started")
	for {
		select {
		case <-k.exitChan:
			return
		case newMetric := <-queue:
			k.logger.Info("kafka sender got metric")
			if m, ok := metricsMap[newMetric.Metric]; ok {
				m.Points = append(m.Points, newMetric.Points...)
			} else {
				metricsMap[newMetric.Metric] = newMetric
				data.Metrics = append(data.Metrics, newMetric)
			}
		case <-ticker.C:
			if k.kafka != nil {
				k.sendToKafka(data)
				data = &carbon.Payload{}
				metricsMap = make(map[string]*carbon.Metric)
			}
		}

	}
}

func (k *KafkaSender) Send(metric *carbon.Metric) {
	queueId := k.distributionFunc.MetricToShard(metric)
	if queueId == -1 {
		for i := range k.queues {
			k.queues[i] <- metric
		}
		return
	}
	k.queues[queueId] <- metric
}

func (k *KafkaSender) tryToConnect() {
	var err error
	var previousError error
	var producer sarama.AsyncProducer
	for {
		select {
		case <-k.exitChan:
			return
		default:
		}
		if k.kafka == nil {
			producer, err = sarama.NewAsyncProducer(k.Brokers, k.kafkaConfig)
			if err == nil {
				k.kafka = producer
			} else {
				if previousError == nil || previousError.Error() != err.Error() {
					k.logger.Error("error connecting to all kafka servers, will retry again (message will appear only once for each unsuccessful series of connects)",
						zap.Error(err),
					)
					previousError = err
				}
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
}

func (k *KafkaSender) Start() {
	go k.tryToConnect()

	for i := 0; i < k.Shards; i++ {
		go k.worker(k.queues[i])
	}
}