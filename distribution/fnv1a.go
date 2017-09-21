package distribution

import (
	"hash/fnv"

	"github.com/go-graphite/g2mt/carbon"
)

type FNV1aDistribution struct {
	name string

	shards uint64
}

func NewFNV1aDistribution(name string, shards int) *FNV1aDistribution {
	return &FNV1aDistribution{
		name:   name,
		shards: uint64(shards),
	}
}

func (d *FNV1aDistribution) MetricToShard(metric *carbon.Metric) int {
	hf := fnv.New64a()
	_, _ = hf.Write([]byte(metric.Metric))
	return int(hf.Sum64() % d.shards)
}
