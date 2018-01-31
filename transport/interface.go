package transport

import (
	"github.com/go-graphite/gorelka/carbon"
	"github.com/go-graphite/gorelka/transport/common"
)

type Sender interface {
	Start()
	Send(payload *carbon.Payload)
	GetName() string
}

type SenderInitFunc func(c common.Config, exitChan <-chan struct{}, workers, maxBatchSize int) (Sender, error)
