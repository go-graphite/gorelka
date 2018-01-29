package routers

import (
	"github.com/go-graphite/gorelka/carbon"
	"github.com/go-graphite/gorelka/transport"
)

// Router is an interface for all possible routers
type Router interface {
	Route(payload carbon.Payload)
	Reload(senders []transport.Sender, config Config) bool
}
