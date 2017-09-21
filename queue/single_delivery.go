package queue

import (
	"sync"
	"sync/atomic"

	"github.com/go-graphite/g2mt/carbon"
)

type SingleDeliveryQueue struct {
	sync.Mutex
	data         *carbon.Payload
	nameToMetric map[string]*carbon.Metric

	currentElement int
	maxSize        int
	size           uint64
}

func NewSingleDeliveryQueue(size int) *SingleDeliveryQueue {
	return &SingleDeliveryQueue{
		data:         &carbon.Payload{},
		maxSize:      size,
		nameToMetric: make(map[string]*carbon.Metric),
	}
}

func (q *SingleDeliveryQueue) Len() uint64 {
	s := atomic.LoadUint64(&q.size)
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
	q.data = &carbon.Payload{}
	q.nameToMetric = make(map[string]*carbon.Metric)
	q.size = 0
	q.Unlock()
	return e, true
}

type SingleDeliveryQueueByte struct {
	sync.Mutex
	data [][]byte

	currentElement int
	maxSize        int
	size           uint64
}

func NewSingleDeliveryQueueByte(size int) *SingleDeliveryQueueByte {
	return &SingleDeliveryQueueByte{
		data:    make([][]byte, 0, size),
		maxSize: size,
	}
}

func (q *SingleDeliveryQueueByte) HaveData() bool {
	s := atomic.LoadUint64(&q.size)
	return s > 0
}

func (q *SingleDeliveryQueueByte) Len() uint64 {
	s := atomic.LoadUint64(&q.size)
	return s
}

func (q *SingleDeliveryQueueByte) Enqueue(data []byte) {
	q.Lock()
	q.data = append(q.data, data)
	q.size++
	q.Unlock()
}

func (q *SingleDeliveryQueueByte) EnqueueMany(data [][]byte) {
	q.Lock()
	q.data = append(q.data, data...)
	q.size += uint64(len(data))
	q.Unlock()
}

func (q *SingleDeliveryQueueByte) Dequeue() ([]byte, bool) {
	q.Lock()
	if len(q.data) == 0 {
		q.Unlock()
		return nil, false
	}
	e := q.data[0]
	q.data = q.data[1:]
	q.size--
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
	q.data = make([][]byte, 0, q.maxSize)
	atomic.StoreUint64(&q.size, 0)
	q.Unlock()
	return e, true
}
