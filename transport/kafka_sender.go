package transport

import (
	"crypto/tls"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/go-graphite/gorelka/carbon"
	"github.com/go-graphite/gorelka/distribution"
	"github.com/go-graphite/gorelka/encoders/graphite"
	"github.com/go-graphite/gorelka/transport/common"

	"github.com/Shopify/sarama"
	"github.com/go-graphite/gorelka/queue"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

type KafkaSender struct {
	common.Config

	senderID int

	kafka        sarama.AsyncProducer
	exitChan     <-chan struct{}
	queues       []*queue.SingleDeliveryQueue
	maxBatchSize int
	workers      int
	sendInterval time.Duration

	kafkaConfig *sarama.Config

	mightHaveDataToProcess chan struct{}
	logger                 *zap.Logger
	data                   metricsMap
	distributionFunc       distribution.Distribute
}

func NewKafkaSender(c common.Config, exitChan <-chan struct{}, workers, maxBatchSize int) (Sender, error) {
	if c.Shards <= 0 {
		return nil, fmt.Errorf("invalid amount of shards (%v), should be at least 1", c.Shards)
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = c.RequiredAcks
	config.Producer.Retry.Max = c.RetryMax
	config.Producer.Flush.Frequency = c.FlushFrequency
	config.ChannelBufferSize = c.ChannelBufferSize
	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Version = c.Version

	switch strings.ToLower(c.Compression) {
	case "lz4":
		config.Producer.Compression = sarama.CompressionLZ4
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

	if c.TLS.Enabled {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = &tls.Config{
			InsecureSkipVerify: true,
		}
	}

	sender := &KafkaSender{
		Config: c,

		kafkaConfig:            config,
		kafka:                  nil,
		exitChan:               exitChan,
		maxBatchSize:           maxBatchSize,
		sendInterval:           c.SendInterval,
		workers:                workers,
		mightHaveDataToProcess: make(chan struct{}),
		distributionFunc:       distributionFunc,

		logger: zapwriter.Logger("receiver").With(zap.String("name", c.Name), zap.String("protocol", c.Type.String())),
	}

	for i := 0; i < c.Shards; i++ {
		q := queue.NewSingleDeliveryQueue(int64(maxBatchSize))
		sender.queues = append(sender.queues, q)
	}

	return sender, nil

}

func (k *KafkaSender) returnMessagesToPool() {
	select {
	case <-k.exitChan:
		return
	case p := <-k.kafka.Successes():
		common.ProducerMessagePool.Put(p)
	}
}

func (k *KafkaSender) GetName() string {
	return k.Name
}

func (k *KafkaSender) sendToKafka(payload *carbon.Payload) {
	if len(payload.Metrics) == 0 {
		return
	}

	k.logger.Debug("got some data to send")

	t0 := time.Now()
	var data []byte
	var err error
	switch k.Encoding {
	case common.JsonEncoding:
		data, err = json.Marshal(payload)
	case common.ProtobufEncoding:
		data, err = payload.Marshal()
	case common.GraphiteLineEncoding:
		data, err = graphite.CarbonPayloadMarshaller(payload)
	}
	if err != nil {
		k.logger.Error("failed to marshal message",
			zap.Error(err),
		)
	}

	msg := common.GetSaramaProducer(k.Topic, k.Partition, data)
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

func (k *KafkaSender) worker(queue *queue.SingleDeliveryQueue) {
	metricsMap := make(map[string]*carbon.Metric)
	data := &carbon.Payload{}
	ticker := time.NewTicker(k.sendInterval)
	k.logger.Info("Worker started")
	for {
		select {
		case <-k.exitChan:
			return
		case <-ticker.C:
			if k.kafka != nil {
				k.sendToKafka(data)
				data = &carbon.Payload{}
				metricsMap = make(map[string]*carbon.Metric)
			}
		default:
		}

		payload, ok := queue.DequeueAllNB()
		if !ok {
			time.Sleep(k.sendInterval / 10)
			continue
		}
		for _, newMetric := range payload.Metrics {
			k.logger.Info("kafka sender got metric")
			if m, ok := metricsMap[newMetric.Metric]; ok {
				m.Points = append(m.Points, newMetric.Points...)
			} else {
				metricsMap[newMetric.Metric] = newMetric
				data.Metrics = append(data.Metrics, newMetric)
			}
		}
	}
}

func (k *KafkaSender) Send(metric *carbon.Payload) {
	payloads := make([]*carbon.Payload, len(k.queues))
	if k.distributionFunc.IsAll() {
		for i := range k.queues {
			k.queues[i].Enqueue(metric)
		}
		return
	}

	for _, m := range metric.Metrics {
		queueId := k.distributionFunc.MetricToShard(m)
		payloads[queueId].Metrics = append(payloads[queueId].Metrics, m)
	}

	for queueId, payload := range payloads {
		k.logger.Debug("got data to send",
			zap.Int("queue_id", queueId),
		)
		k.queues[queueId].Enqueue(payload)
	}
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
