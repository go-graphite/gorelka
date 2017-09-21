package distribution

import (
	"github.com/dgryski/go-jump"
	"hash/fnv"

	"github.com/go-graphite/g2mt/carbon"
)

type JumpFNV1aDistribution struct {
	name string

	shards int
}

func NewJumpFNV1aDistribution(name string, shards int) *JumpFNV1aDistribution {
	return &JumpFNV1aDistribution{
		name:   name,
		shards: shards,
	}
}

func (d *JumpFNV1aDistribution) MetricToShard(metric *carbon.Metric) int {
	hf := fnv.New64a()
	_, _ = hf.Write([]byte(metric.Metric))
	return int(jump.Hash(hf.Sum64(), d.shards))
}
