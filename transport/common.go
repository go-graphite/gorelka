package transport

import (
	"github.com/go-graphite/g2mt/carbon"
	"sync"
)

type metricsMap struct {
	sync.RWMutex
	data map[string]*carbon.Metric
}
