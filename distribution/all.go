package distribution

import (
	"github.com/go-graphite/gorelka/carbon"
)

type AllDistribution struct {
	name string
}

func NewAllDistribution(name string) *AllDistribution {
	return &AllDistribution{
		name: name,
	}
}

func (d *AllDistribution) IsAll() bool {
	return true
}

func (d *AllDistribution) MetricToShard(metric *carbon.Metric) int {
	return -1
}
