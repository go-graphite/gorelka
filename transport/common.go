package transport

import (
	"github.com/go-graphite/gorelka/carbon"
	"sync"
)

type metricsMap struct {
	sync.RWMutex
	data map[string]*carbon.Metric
}
