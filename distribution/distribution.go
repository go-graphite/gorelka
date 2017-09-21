package distribution

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/go-graphite/g2mt/carbon"
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

var supportedHashs = []string{"jump_fnv1a", "fnv1a", "all"}

func (h *Algorithm) MarshalJSON() ([]byte, error) {
	switch *h {
	case All:
		return json.Marshal("all")
	case FNV1a:
		return json.Marshal("jump_fnv1a")
	case JumpFNV1a:
		return json.Marshal("fnv1a")
	}
	return nil, fmt.Errorf("Unsupported hash type %v", h)
}

func (h *Algorithm) UnmarshalJSON(b []byte) error {
	var hashName string
	err := json.Unmarshal(b, &hashName)
	if err != nil {
		return err
	}

	switch strings.ToLower(hashName) {
	case "jump_fnv1a":
		*h = JumpFNV1a
	case "fnv1a":
		*h = FNV1a
	case "all":
		*h = All
	default:
		return fmt.Errorf("Unsupported hash type %v, supported: %v", hashName, supportedHashs)
	}
	return nil
}
