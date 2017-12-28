package transport

import (
	"time"
	"github.com/go-graphite/g2mt/carbon"
	"github.com/go-graphite/g2mt/transport/common"
)

type Sender interface {
	Start()
	Send(metric *carbon.Metric)
	GetName() string
}

type SenderInitFunc func(c common.Config, exitChan <-chan struct{}, workers, maxBatchSize int, sendInterval time.Duration) (Sender, error)