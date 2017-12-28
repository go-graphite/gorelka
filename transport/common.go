package transport

import (
	"sync"
	"github.com/go-graphite/g2mt/carbon"
)

type metricsMap struct {
	sync.RWMutex
	data map[string]*carbon.Metric
}