package receiver

import (
	"github.com/go-graphite/g2mt/carbon"
)

type Receiver interface {
	Start()
	Parse(line []byte) (*carbon.Metric, error)
}
