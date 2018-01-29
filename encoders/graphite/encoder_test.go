package graphite

import (
	"bytes"
	"testing"

	"github.com/go-graphite/gorelka/carbon"
)

func TestGraphiteLineEncoder(t *testing.T) {
	payload := &carbon.Payload{
		Metrics: []*carbon.Metric{
			{
				Metric: "foo.bar",
				Points: []carbon.Point{
					{
						Timestamp: 1508759656,
						Value:     0.1,
					},
					{
						Timestamp: 1508759657,
						Value:     0.2,
					},
				},
			},
			{
				Metric: "boo.bar",
				Tags: map[string]string{
					"rename_enable": "1",
				},
				Points: []carbon.Point{
					{
						Timestamp: 1508759656,
						Value:     1.1,
					},
					{
						Timestamp: 1508759657,
						Value:     1.2,
					},
				},
			},
		},
	}

	expectedOutput := []byte("foo.bar 0.1 1508759656\nfoo.bar 0.2 1508759657\nboo.bar;rename_enable=1 1.1 1508759656\nboo.bar;rename_enable=1 1.2 1508759657\n")

	out, err := CarbonPayloadMarshaller(payload)
	if err != nil {
		t.Errorf("err is not nil, but it should be, %v", err)
	}

	if out == nil {
		t.Errorf("out is nil, but it shouldn't be")
	}

	if bytes.Compare(out, expectedOutput) != 0 {
		t.Errorf("output is different\nout:\n%v\n\nexpected:\n%v", string(out), string(expectedOutput))
	}
}

func BenchmarkGraphiteLineEncoderSingleMetricSinglePoint(b *testing.B) {
	payload := &carbon.Payload{
		Metrics: []*carbon.Metric{
			{
				Metric: "foo.bar",
				Points: []carbon.Point{
					{
						Timestamp: 1508759656,
						Value:     0.1,
					},
				},
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CarbonPayloadMarshaller(payload)
	}
}

func BenchmarkGraphiteLineEncoderSingleMetricFivePoints(b *testing.B) {
	payload := &carbon.Payload{
		Metrics: []*carbon.Metric{
			{
				Metric: "foo.bar",
				Points: []carbon.Point{
					{
						Timestamp: 1508759656,
						Value:     0.1,
					},
					{
						Timestamp: 1508759657,
						Value:     0.2,
					},
					{
						Timestamp: 1508759658,
						Value:     0.3,
					},
					{
						Timestamp: 1508759659,
						Value:     0.4,
					},
					{
						Timestamp: 1508759660,
						Value:     0.5,
					},
				},
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CarbonPayloadMarshaller(payload)
	}
}

func BenchmarkGraphiteLineEncoderFiveMetricsSinglePoint(b *testing.B) {
	payload := &carbon.Payload{
		Metrics: []*carbon.Metric{
			{
				Metric: "foo.bar1",
				Points: []carbon.Point{
					{
						Timestamp: 1508759656,
						Value:     0.1,
					},
				},
			},
			{
				Metric: "foo.bar2",
				Points: []carbon.Point{
					{
						Timestamp: 1508759657,
						Value:     0.2,
					},
				},
			},
			{
				Metric: "foo.bar3",
				Points: []carbon.Point{
					{
						Timestamp: 1508759658,
						Value:     0.3,
					},
				},
			},
			{
				Metric: "foo.bar4",
				Points: []carbon.Point{
					{
						Timestamp: 1508759659,
						Value:     0.4,
					},
				},
			},
			{
				Metric: "foo.bar5",
				Points: []carbon.Point{
					{
						Timestamp: 1508759660,
						Value:     0.5,
					},
				},
			},
		},
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		CarbonPayloadMarshaller(payload)
	}
}
