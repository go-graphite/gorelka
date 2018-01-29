package transport

import (
	"regexp"

	"github.com/go-graphite/gorelka/carbon"
)

type RewritingSender struct {
	name string

	regex *regexp.Regexp
}

func (s *RewritingSender) Start()                     {}
func (s *RewritingSender) Send(metric *carbon.Metric) {}
func (s *RewritingSender) GetName() string {
	return s.name
}

func NewRewritingSender(name, re, to string) *RewritingSender {
	return &RewritingSender{
		name: "blackhole",
	}
}
