package transport

import (
	"github.com/go-graphite/gorelka/carbon"
)

type BlackholeSender struct {
	Name string
}

func (s *BlackholeSender) Start()                      {}
func (s *BlackholeSender) Send(metric *carbon.Payload) {}
func (s *BlackholeSender) GetName() string {
	return s.Name
}

func NewBlackholeSender() *BlackholeSender {
	return &BlackholeSender{
		Name: "blackhole",
	}
}
