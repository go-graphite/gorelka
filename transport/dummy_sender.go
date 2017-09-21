package transport

import (
	"github.com/go-graphite/g2mt/carbon"
)

// DummySender should only be used for testing purposes
type DummySender struct {
	Name string

	metricsReceived []*carbon.Metric
}

// Start should start the dummy sender, but actually it doesn't do anything
func (s *DummySender) Start() {}

func (s *DummySender) Send(metric *carbon.Metric) {
	s.metricsReceived = append(s.metricsReceived, metric)
}

func (s *DummySender) GetName() string {
	return s.Name
}

func (s *DummySender) GetReceivedMetrics() []*carbon.Metric {
	m := s.metricsReceived
	s.metricsReceived = make([]*carbon.Metric, 0)
	return m
}

func (s *DummySender) String() string {
	return "{sender: " + s.Name + "}"
}

func NewDummySender(name string) *DummySender {
	return &DummySender{
		metricsReceived: make([]*carbon.Metric, 0),
		Name:            name,
	}
}
