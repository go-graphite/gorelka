package receiver

import (
	"github.com/go-graphite/gorelka/carbon"
)

type Receiver interface {
	Start()
	Parse(line []byte) (*carbon.Metric, error)
}
