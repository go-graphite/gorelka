package distribution

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-graphite/gorelka/carbon"
)

type Distribute interface {
	MetricToShard(metric *carbon.Metric) int
}

// HashType

type Algorithm int

const (
	Unknown   Algorithm = -1
	All                 = 0
	JumpFNV1a           = 1
	FNV1a               = 2
	CarbonCH            = 3 //NotImplementedYet
)

var supportedHashs = map[string]Algorithm{
	"all":        All,
	"jump_fnv1a": JumpFNV1a,
	"fnv1a":      FNV1a,
}

func (h *Algorithm) MarshalJSON() ([]byte, error) {
	for k, v := range supportedHashs {
		if v == *h {
			return json.Marshal(k)
		}
	}
	return nil, fmt.Errorf("unknown distribution algorithm '%s', supported algorithms: %v", *h, supportedHashs)
}

func (h *Algorithm) FromString(s string) error {
	var ok bool
	if *h, ok = supportedHashs[strings.ToLower(s)]; !ok {
		return fmt.Errorf("unknown distribution algorithm '%s', supported algorithms: %v", *h, supportedHashs)
	}

	return nil
}

func (h *Algorithm) UnmarshalJSON(data []byte) error {
	var hash string
	err := json.Unmarshal(data, &hash)
	if err != nil {
		return err
	}

	return h.FromString(hash)
}

func (h *Algorithm) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var hash string
	err := unmarshal(&hash)
	if err != nil {
		return err
	}

	return h.FromString(hash)

}
