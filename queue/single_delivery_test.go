package queue

import (
	"strconv"
	"sync"
	"testing"

	"time"

	"bytes"
)

const (
	payloads    = 1000
	queueFactor = 10000
)

func BenchmarkSingleDeliveryQueueByteEnqueueDequeueSingleThread(b *testing.B) {
	payload := make([][]byte, 0, 1000)
	for i := 0; i < payloads; i++ {
		payload = append(payload, []byte("some.random.payload"+strconv.Itoa(i)))
	}
	q := NewSingleDeliveryQueueByte(payloads * queueFactor)
	b.ResetTimer()
	var err error
	firstRun := true
	for i := 0; i < b.N; i++ {
		firstRun = true
		for firstRun || err != nil {
			firstRun = false
			err = q.EnqueueMany(payload)
			if err != nil {
				_, _ = q.DequeueAll()
			}
		}
	}

}

func BenchmarkSingleDeliveryQueueByteEnqueueDequeueMultiThread(b *testing.B) {
	payload := make([][]byte, 0, payloads)
	for i := 0; i < payloads; i++ {
		payload = append(payload, []byte("some.random.payload"+strconv.Itoa(i)))
	}

	q := NewSingleDeliveryQueueByte(payloads * queueFactor)

	syncClose := make(chan struct{})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-syncClose:
				return
			default:
			}
			_, _ = q.DequeueAll()
			time.Sleep(20 * time.Millisecond)
		}
	}()

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		var err error
		firstRun := true
		for pb.Next() {
			firstRun = true
			for firstRun || err != nil {
				firstRun = false
				err = q.EnqueueMany(payload)
			}
		}
	})
	close(syncClose)

	wg.Wait()
}

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
