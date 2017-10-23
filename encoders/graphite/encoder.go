package graphite

import (
	"bytes"

	"github.com/go-graphite/g2mt/carbon"
	"strconv"
)

func CarbonPayloadMarshaller(payload *carbon.Payload) ([]byte, error) {
	out := new(bytes.Buffer)
	for _, metric := range payload.Metrics {
		for _, point := range metric.Points {
			tmp := metric.Metric + " " + strconv.FormatFloat(point.Value, 'f', -1, 64) + " " + strconv.FormatUint(uint64(point.Timestamp), 10) + "\n"
			_, err := out.WriteString(tmp)
			if err != nil {
				return nil, err
			}
		}
	}

	return out.Bytes(), nil
}