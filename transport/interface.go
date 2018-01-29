package transport

import (
	"github.com/go-graphite/gorelka/carbon"
	"github.com/go-graphite/gorelka/transport/common"
	"time"
)

type Sender interface {
	Start()
	Send(metric *carbon.Metric)
	GetName() string
}

type SenderInitFunc func(c common.Config, exitChan <-chan struct{}, workers, maxBatchSize int, sendInterval time.Duration) (Sender, error)
