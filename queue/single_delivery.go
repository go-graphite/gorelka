package queue

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/go-graphite/gorelka/carbon"
)

type SingleDeliveryQueue struct {
	sync.Mutex
	data         *carbon.Payload
	nameToMetric map[string]*carbon.Metric

	currentElement int
	maxSize        int64
	size           int64
}

func NewSingleDeliveryQueue(size int64) *SingleDeliveryQueue {
	return &SingleDeliveryQueue{
		data:         &carbon.Payload{},
		maxSize:      size,
		nameToMetric: make(map[string]*carbon.Metric),
	}
}

func (q *SingleDeliveryQueue) Len() int64 {
	s := atomic.LoadInt64(&q.size)
	return s
}

func (q *SingleDeliveryQueue) HaveData() bool {
	return q.Len() > 0
}

func (q *SingleDeliveryQueue) Enqueue(data *carbon.Payload) {
	q.Lock()
	for i, metric := range data.Metrics {
		if m, ok := q.nameToMetric[data.Metrics[i].Metric]; ok {
			m.Points = append(m.Points, metric.Points...)
		} else {
			q.data.Metrics = append(q.data.Metrics, metric)
			q.nameToMetric[data.Metrics[i].Metric] = metric
		}
	}
	q.size++
	q.Unlock()
}

func (q *SingleDeliveryQueue) DequeueAll() (*carbon.Payload, bool) {
	q.Lock()
	if q.size == 0 {
		q.Unlock()
		return nil, false
	}
	e := q.data
	q.data = &carbon.Payload{Metrics: make([]*carbon.Metric, 0, len(q.data.Metrics))}
	q.nameToMetric = make(map[string]*carbon.Metric, len(q.nameToMetric))
	q.size = 0
	q.Unlock()
	return e, true
}

var errBufferOverflow = fmt.Errorf("buffer overflow for per/connection input queue")

type SingleDeliveryQueueByte struct {
	sync.Mutex
	data [][]byte

	currentElement int64
	maxSize        int64
	size           int64
}

func NewSingleDeliveryQueueByte(size int64) *SingleDeliveryQueueByte {
	return &SingleDeliveryQueueByte{
		data:    make([][]byte, 0, size),
		maxSize: size,
	}
}

func (q *SingleDeliveryQueueByte) HaveData() bool {
	return q.Len() > 0
}

func (q *SingleDeliveryQueueByte) Len() int64 {
	s := atomic.LoadInt64(&q.size)
	return s
}

func (q *SingleDeliveryQueueByte) Enqueue(data []byte) error {
	q.Lock()
	if q.size >= q.maxSize {
		q.Unlock()
		return errBufferOverflow
	}
	q.data = append(q.data, data)
	atomic.AddInt64(&q.size, 1)
	q.Unlock()
	return nil
}

func (q *SingleDeliveryQueueByte) EnqueueMany(data [][]byte) error {
	q.Lock()
	if q.size >= q.maxSize {
		q.Unlock()
		return errBufferOverflow
	}

	q.data = append(q.data, data...)
	atomic.AddInt64(&q.size, int64(len(data)))
	q.Unlock()

	return nil
}

func (q *SingleDeliveryQueueByte) Dequeue() ([]byte, bool) {
	q.Lock()
	if len(q.data) == 0 {
		q.Unlock()
		return nil, false
	}
	e := q.data[0]
	q.data = q.data[1:]
	atomic.AddInt64(&q.size, -1)
	q.Unlock()
	return e, true
}

func (q *SingleDeliveryQueueByte) DequeueAll() ([][]byte, bool) {
	q.Lock()
	if len(q.data) == 0 {
		q.Unlock()
		return nil, false
	}
	e := q.data
	q.data = make([][]byte, 0, len(q.data))
	atomic.StoreInt64(&q.size, 0)
	q.Unlock()
	return e, true
}
