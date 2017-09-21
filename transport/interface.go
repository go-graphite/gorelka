package transport

import (
	"github.com/go-graphite/g2mt/carbon"
)

type Sender interface {
	Start()
	Send(metric *carbon.Metric)
	GetName() string
}
