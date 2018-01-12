package async

import (
	"crypto/tls"
	"fmt"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-graphite/g2mt/carbon"
	"github.com/go-graphite/g2mt/encoders/graphite"
	transport "github.com/go-graphite/g2mt/transport/common"
	"github.com/go-graphite/g2mt/transport/workers"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

type asyncWorker struct {
	id    int
	alive int64

	sendInterval time.Duration
	tls          *transport.TLSConfig
	server       string
	proto        string

	compressor func(w net.Conn) (io.ReadWriteCloser, error)
	marshaller func(payload *carbon.Payload) ([]byte, error)
	writer     io.ReadWriteCloser
	exitChan   <-chan struct{}
	queue      chan *carbon.Metric
	stats      workers.WorkerStats

	logger *zap.Logger
}

func (w asyncWorker) IsAlive() bool {
	alive := atomic.LoadInt64(&w.alive)

	return alive == 1
}

func (w *asyncWorker) GetStats() *workers.WorkerStats {
	stats := &workers.WorkerStats{
		SpentTime:  atomic.LoadInt64(&w.stats.SpentTime),
		SentPoints: atomic.LoadInt64(&w.stats.SentPoints),
		SendErrors: atomic.LoadInt64(&w.stats.SendErrors),
	}

	return stats
}

var errWorkerIsNotAlive = fmt.Errorf("connection is not established")

func (w *asyncWorker) TryConnect() {
	for {
		time.Sleep(250 * time.Millisecond)
		if w.IsAlive() {
			continue
		}

		var conn net.Conn
		var err error
		if w.tls.Enabled {
			// srv, port := serverToPortAddr(s)
			tlsConfig := &tls.Config{
				InsecureSkipVerify: w.tls.SkipInsecureCerts,
				ServerName:         w.server,
			}
			conn, err = tls.Dial(w.proto, w.server, tlsConfig)
		} else {
			conn, err = net.Dial(w.proto, w.server)
		}

		if err != nil {
			// TODO: log error
			atomic.AddInt64(&w.stats.ConnectErrors, 1)
			continue
		}

		w.writer, err = w.compressor(conn)
		if err != nil {
			// TODO: log error
			atomic.AddInt64(&w.stats.ConnectErrors, 1)
			conn.Close()
			continue
		}
		atomic.StoreInt64(&w.alive, 1)
	}
}

func (w *asyncWorker) send(payload *carbon.Payload) error {
	if !w.IsAlive() {
		return errWorkerIsNotAlive
	}
	if len(payload.Metrics) == 0 {
		return nil
	}

	t0 := time.Now()
	data, err := w.marshaller(payload)
	if err != nil {
		atomic.AddInt64(&w.stats.MarshalErrors, 1)
		return err
	}

	_, err = w.writer.Write(data)
	if err != nil {
		atomic.AddInt64(&w.stats.SendErrors, 1)
		return err
	}

	l := int64(0)
	for i := range payload.Metrics {
		l += int64(len(payload.Metrics[i].Points))
	}

	spentTime := time.Since(t0).Nanoseconds()
	atomic.AddInt64(&w.stats.SpentTime, spentTime)
	atomic.AddInt64(&w.stats.SentPoints, l)

	return nil
}

func (w *asyncWorker) Loop() {
	metricsMap := make(map[string]*carbon.Metric)
	data := &carbon.Payload{}
	ticker := time.NewTicker(w.sendInterval)
	for {
		select {
		case <-w.exitChan:
			return
		case newMetric := <-w.queue:
			if m, ok := metricsMap[newMetric.Metric]; ok {
				m.Points = append(m.Points, newMetric.Points...)
			} else {
				metricsMap[newMetric.Metric] = newMetric
				data.Metrics = append(data.Metrics, newMetric)
			}
		case <-ticker.C:
			err := w.send(data)
			if err == nil {
				data = &carbon.Payload{Metrics: make([]*carbon.Metric, 0, len(data.Metrics))}
				metricsMap = make(map[string]*carbon.Metric)
			}
		}

	}
}

func NewAsyncWorker(id int, config transport.Config, queue chan *carbon.Metric, exitChan <-chan struct{}) *asyncWorker {
	l := zapwriter.Logger("worker").With(
		zap.Int("id", id),
		zap.String("server", config.Servers[id]),
	)
	w := &asyncWorker{
		id:           id,
		exitChan:     exitChan,
		queue:        queue,
		sendInterval: config.FlushFrequency,
		server:       config.Servers[id],
		tls:          &config.TLS,
		proto:        config.Type.String(),
		logger:       l,
	}

	switch config.Encoding {
	case transport.JsonEncoding:
		w.marshaller = graphite.JSONMarshaler
	case transport.ProtobufEncoding:
		w.marshaller = graphite.PBMarshaler
	case transport.GraphiteLineEncoding:
		w.marshaller = graphite.CarbonPayloadMarshaller
	}

	switch strings.ToLower(config.Compression) {
	case "snappy":
		w.compressor = workers.SnappyCompressor
	case "gzip":
		compressor := workers.NewGzipLeveledCompressor(config.CompressionLevel)
		w.compressor = compressor.NewWriter
	default:
		w.compressor = workers.NoopCompressor
	}

	go w.Loop()

	return w
}
