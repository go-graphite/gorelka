package graphite

import (
	"bytes"
	"encoding/json"
	"strconv"

	"github.com/go-graphite/g2mt/carbon"
)

func CarbonPayloadMarshaller(payload *carbon.Payload) ([]byte, error) {
	out := new(bytes.Buffer)
	for _, m := range payload.Metrics {
		(*metric)(m).write(out)
	}

	return out.Bytes(), nil
}

func PBMarshaler(payload *carbon.Payload) ([]byte, error) {
	return payload.Marshal()
}

func JSONMarshaler(payload *carbon.Payload) ([]byte, error) {
	return json.Marshal(payload)
}

type metric carbon.Metric

func (m *metric) write(w *bytes.Buffer) {
	metricTags := m.Metric
	for k, v := range m.Tags {
		metricTags += ";"
		metricTags += k
		metricTags += "="
		metricTags += v
	}
	metricTags += " "

	for _, point := range m.Points {
		w.WriteString(metricTags)
		w.WriteString(strconv.FormatFloat(point.Value, 'f', -1, 64))
		w.WriteString(" ")
		w.WriteString(strconv.FormatUint(uint64(point.Timestamp), 10))
		w.WriteString("\n")
	}
}
