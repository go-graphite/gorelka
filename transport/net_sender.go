package transport

import (
	"fmt"
	"strings"
	"time"

	"github.com/go-graphite/g2mt/carbon"
	"github.com/go-graphite/g2mt/distribution"

	"github.com/Shopify/sarama"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"

	"github.com/go-graphite/g2mt/transport/common"
	"github.com/go-graphite/g2mt/transport/workers"
	asyncWorker "github.com/go-graphite/g2mt/transport/workers/async"
	syncWorker "github.com/go-graphite/g2mt/transport/workers/sync"
)

type NetSender struct {
	common.Config

	senderID int

	kafka        sarama.AsyncProducer
	exitChan     <-chan struct{}
	queues       []chan *carbon.Metric
	maxBatchSize int
	workers      int
	sendInterval time.Duration

	kafkaConfig *sarama.Config

	logger           *zap.Logger
	distributionFunc distribution.Distribute
}

func serverToPortAddr(server string) (string, string) {
	idx := strings.LastIndex(server, ":")
	if idx != -1 || strings.HasSuffix(server, "]") {
		return server, ""
	}

	return server[0:idx], server[idx:]
}

func NewNetSender(c common.Config, exitChan <-chan struct{}, workers, maxBatchSize int, sendInterval time.Duration) (Sender, error) {
	if len(c.Servers) <= 0 {
		return nil, fmt.Errorf("invalid amount of servers (%v), should be at least 1", len(c.Servers))
	}

	var distributionFunc distribution.Distribute
	switch c.DistributionAlgorithm {
	case distribution.All:
		distributionFunc = distribution.NewAllDistribution(c.Name)
	case distribution.JumpFNV1a:
		distributionFunc = distribution.NewJumpFNV1aDistribution(c.Name, len(c.Servers))
	case distribution.FNV1a:
		distributionFunc = distribution.NewFNV1aDistribution(c.Name, len(c.Servers))
	}

	sender := &NetSender{
		Config: c,

		kafka:            nil,
		exitChan:         exitChan,
		maxBatchSize:     maxBatchSize,
		sendInterval:     sendInterval,
		workers:          workers,
		distributionFunc: distributionFunc,

		logger: zapwriter.Logger("receiver").With(zap.String("name", c.Name), zap.String("protocol", c.Type.String())),
	}

	for i := 0; i < len(c.Servers); i++ {
		sender.queues = append(sender.queues, make(chan *carbon.Metric, c.ChannelBufferSize))
	}

	return sender, nil
}

func (k *NetSender) GetName() string {
	return k.Name
}

func (k *NetSender) Send(metric *carbon.Metric) {
	queueId := k.distributionFunc.MetricToShard(metric)
	k.logger.Debug("got data to send",
		zap.Int("queue_id", queueId),
	)
	if queueId == -1 {
		for i := range k.queues {
			k.queues[i] <- metric
		}
		return
	}
	k.queues[queueId] <- metric
}

func (k *NetSender) Start() {
	for i := 0; i < len(k.Servers); i++ {
		k.logger.Debug("starting worker",
			zap.Int("worker_id", i),
			zap.Bool("buffered", k.Config.Buffered),
			zap.String("server", k.Config.Servers[i]),
		)
		var worker workers.NetWorker
		if k.Config.Buffered {
			worker = asyncWorker.NewAsyncWorker(i, k.Config, k.queues[i], k.exitChan)
		} else {
			worker = syncWorker.NewSyncWorker(i, k.Config, k.queues[i], k.exitChan)
		}
		_ = worker
	}
}
