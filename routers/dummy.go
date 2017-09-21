package routers

import (
	"github.com/go-graphite/g2mt/carbon"
	"github.com/go-graphite/g2mt/transport"
	"sync"
)

// DummyRouter implements simple router
// That will just store everything in a map (with proper locking)
// It's only useful for unit tests and shouldn't be used in Production
type DummyRouter struct {
	sync.RWMutex
	data map[string]*carbon.Metric
}

// NewDummyRouter will initialize router
func NewDummyRouter() *DummyRouter {
	return &DummyRouter{
		data: make(map[string]*carbon.Metric),
	}
}

// Route is the endpoint that should receive list of metrics
func (r *DummyRouter) Route(payload carbon.Payload) {
	r.Lock()
	defer r.Unlock()

	for _, m := range payload.Metrics {
		if v, ok := r.data[m.Metric]; ok {
			v.Points = append(v.Points, m.Points...)
		} else {
			r.data[m.Metric] = m
		}
	}
}

// Reload will try to reload configuration of router. Returns true on success.
func (r *DummyRouter) Reload(senders []transport.Sender, config Config) bool {
	return false
}

// GetData is a special function in DummyRouter that returns all internal data and resets the state
func (r *DummyRouter) GetData() map[string]*carbon.Metric {
	r.Lock()
	defer r.Unlock()

	v := r.data
	r.data = make(map[string]*carbon.Metric)
	return v
}
