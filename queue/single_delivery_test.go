package queue

import (
	"testing"

	"bytes"
	"github.com/go-graphite/gorelka/carbon"
)

/*
func payloadsEqual(first *carbon.Payload, second *carbon.Payload) bool {
	if first.Metrics == nil {
		if second.Metrics == nil {
			return true
		}
		return false
	}
	if second.Metrics == nil || len(first.Metrics) != len(second.Metrics) {
		return false
	}

	for i, m1 := range first.Metrics {
		m2 := second.Metrics[i]
		if m1.Metric != m2.Metric || len(m1.Points) != len(m2.Points) {
			return false
		}
		for j, point1 := range m1.Points {
			point2 := m2.Points[j]
			if point1.Value != point2.Value || point1.Timestamp != point2.Timestamp {
				return false
			}
		}
	}

	return true
}

func TestSingleDeliveryQueueCarbon(t *testing.T) {
	q := NewSingleDeliveryQueue(100)

	testPayload := &carbon.Payload{
		Metrics: []*carbon.Metric{
			{
				Metric: "foo",
				Points: []carbon.Point{
					{
						Value:     1.2,
						Timestamp: 123.0,
					},
				},
			},
			{
				Metric: "bar",
				Points: []carbon.Point{
					{
						Value:     2.4,
						Timestamp: 124.0,
					},
				},
			},
		}}

	q.Enqueue(testPayload)
	l := q.Len()
	if l != 1 {
		t.Errorf("Queue have wrong amount of data: %v, expected %v", l, 1)
	}

	if !q.HaveData() {
		t.Error("Queue doesn't have data, but should")
	}

	p, ok := q.DequeueAll()
	if !ok {
		t.Error("Can't dequeue")
	}

	if !payloadsEqual(testPayload, p) {
		t.Errorf("Malformed point. Got: %+v, expected %+v", p, testPayload)
	}

	p, ok = q.DequeueAll()
	if ok {
		t.Errorf("Dequeued point again: %+v", p)
	}

	if q.Len() != 0 {
		t.Errorf("Wrong len: %v, exepcted %v", q.Len(), 0)
	}

	if q.HaveData() {
		t.Error("Queue have data, but shouldn't")
	}

	checkPayload := &carbon.Payload{
		Metrics: []*carbon.Metric{
			{
				Metric: "foo",
				Points: []carbon.Point{
					{
						Value:     1.2,
						Timestamp: 123.0,
					},
					{
						Value:     1.2,
						Timestamp: 123.0,
					},
				},
			},
			{
				Metric: "bar",
				Points: []carbon.Point{
					{
						Value:     2.4,
						Timestamp: 124.0,
					},
					{
						Value:     2.4,
						Timestamp: 124.0,
					},
				},
			},
		}}

	q.Enqueue(testPayload)
	q.Enqueue(testPayload)

	p, ok = q.DequeueAll()
	if !ok {
		t.Error("Can't dequeue")
	}

	if !payloadsEqual(checkPayload, p) {
		t.Errorf("Malformed point. Got: %+v, expected %+v", p, checkPayload)
	}
}
*/

func TestSingleDeliveryQueueByte(t *testing.T) {
	q := NewSingleDeliveryQueueByte(100)

	testPayload1 := []byte("foo")
	testPayload2 := []byte("bar")

	q.Enqueue(testPayload1)
	l := q.Len()
	if l != 1 {
		t.Errorf("Queue have wrong amount of data: %v, expected %v", l, 1)
	}

	q.Enqueue(testPayload2)
	l = q.Len()
	if l != 2 {
		t.Errorf("Queue have wrong amount of data: %v, expected %v", l, 2)
	}

	if !q.HaveData() {
		t.Error("Queue doesn't have data, but should")
	}

	p, ok := q.Dequeue()
	if !ok {
		t.Error("Can't dequeue")
	}

	if bytes.Compare(testPayload1, p) != 0 {
		t.Errorf("Malformed point. Got: %+v, expected %+v", p, testPayload1)
	}

	l = q.Len()
	if l != 1 {
		t.Errorf("Queue have wrong amount of data: %v, expected %v", l, 1)
	}

	p, ok = q.Dequeue()
	if !ok {
		t.Error("Can't dequeue")
	}

	if bytes.Compare(testPayload2, p) != 0 {
		t.Errorf("Malformed point. Got: %+v, expected %+v", p, testPayload2)
	}

	_, ok = q.Dequeue()
	if ok {
		t.Error("Can dequeue, but shouldn't be able to")
	}

	_, ok = q.DequeueAll()
	if ok {
		t.Error("Can dequeue, but shouldn't be able to")
	}

	testPayloadMany := [][]byte{
		testPayload1,
		testPayload2,
	}

	q.EnqueueMany(testPayloadMany)
	l = q.Len()
	if l != 2 {
		t.Errorf("Queue have wrong amount of data: %v, expected %v", l, 2)
	}

	pMany, ok := q.DequeueAll()
	if !ok {
		t.Error("Can't dequeue")
	}

	if len(pMany) != len(testPayloadMany) {
		t.Fatalf("Wrong len: %v, exepcted %v", q.Len(), 0)
	}

	for i := range pMany {
		if bytes.Compare(pMany[i], testPayloadMany[i]) != 0 {
			t.Errorf("Malformed point. Got: %+v, expected %+v", pMany[i], testPayloadMany[i])
		}
	}

	if q.HaveData() {
		t.Error("Queue have data, but shouldn't")
	}
}
