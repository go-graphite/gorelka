package types

import (
	"sync"

	"github.com/go-graphite/g2mt/carbon"
)

// Points
var PointPool = sync.Pool{
	New: func() interface{} {
		return &carbon.Point{}
	},
}

var MetricPool = sync.Pool{
	New: func() interface{} {
		return &carbon.Metric{}
	},
}
var PayloadPool = sync.Pool{
	New: func() interface{} {
		return &carbon.Payload{}
	},
}
