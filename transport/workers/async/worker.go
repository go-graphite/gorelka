package async

import (
	"crypto/tls"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-graphite/gorelka/carbon"
	"github.com/go-graphite/gorelka/encoders/graphite"
	"github.com/go-graphite/gorelka/queue"
	transport "github.com/go-graphite/gorelka/transport/common"
	"github.com/go-graphite/gorelka/transport/workers"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

type errorSignals struct {
	connectionError bool
	compressorError bool
}

type asyncWorker struct {
	id    int
	alive int64

	tls    *transport.TLSConfig
	server string
	proto  string

	compressor    func(w net.Conn) (io.ReadWriteCloser, error)
	marshaller    func(payload *carbon.Payload) ([]byte, error)
	writer        io.ReadWriteCloser
	exitChan      <-chan struct{}
	reconnectChan chan struct{}
	queue         *queue.SingleDeliveryQueue
	stats         workers.WorkerStats
	sendInterval  time.Duration
	maxBufferSize int

	logger       *zap.Logger
	errorSignals errorSignals
}

func (w *asyncWorker) TryConnect() {
	for {
		select {
		case <-w.exitChan:
			return
		case <-w.reconnectChan:
			for {
				w.logger.Debug("trying to connect")

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
					if !w.errorSignals.connectionError {
						w.logger.Error("error while connecting to upstream",
							zap.Error(err),
						)
						w.errorSignals.connectionError = true
					}
					atomic.AddInt64(&w.stats.ConnectErrors, 1)
					time.Sleep(250 * time.Millisecond)
					continue
				}

				if w.errorSignals.connectionError {
					w.logger.Info("connection restored")
					w.errorSignals.connectionError = false
				}

				w.writer, err = w.compressor(conn)
				if err != nil {
					if !w.errorSignals.compressorError {
						w.logger.Error("error initializing compressor",
							zap.Error(err),
						)
						w.errorSignals.compressorError = true
					}
					atomic.AddInt64(&w.stats.ConnectErrors, 1)
					conn.Close()
					time.Sleep(250 * time.Millisecond)
					continue
				}

				if w.errorSignals.compressorError {
					w.logger.Info("compression initialized")
					w.errorSignals.compressorError = false
				}
				atomic.StoreInt64(&w.alive, 1)
				go w.Loop()
				w.logger.Debug("connection established")
				break
			}
		}

	}
}

func (w asyncWorker) IsAlive() bool {
	alive := atomic.LoadInt64(&w.alive)

	return alive == 1
}

func (w *asyncWorker) GetStats() *workers.WorkerStats {
	stats := &workers.WorkerStats{
		SpentTime:   atomic.LoadInt64(&w.stats.SpentTime),
		SentPoints:  atomic.LoadInt64(&w.stats.SentPoints),
		SentMetrics: atomic.LoadInt64(&w.stats.SentMetrics),
		SendErrors:  atomic.LoadInt64(&w.stats.SendErrors),
	}

	return stats
}

func (w *asyncWorker) send(payload *carbon.Payload) error {
	t0 := time.Now()
	pointsToSend := int64(0)
	for m := range payload.Metrics {
		pointsToSend += int64(len(payload.Metrics[m].Points))
	}
	metricsToSend := int64(len(payload.Metrics))
	w.logger.Debug("marshaled some data",
		zap.Int64("metrics", metricsToSend),
		zap.Int64("points", pointsToSend),
	)
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

	spentTime := time.Since(t0).Nanoseconds()
	atomic.AddInt64(&w.stats.SpentTime, spentTime)
	atomic.AddInt64(&w.stats.SentPoints, pointsToSend)
	atomic.AddInt64(&w.stats.SentMetrics, metricsToSend)

	return nil
}

func (w *asyncWorker) Loop() {
	seenMetrics := make(map[string]*carbon.Metric)
	metrics := &carbon.Payload{Metrics: make([]*carbon.Metric, 0)}
	metricsLen := 0
	lastSend := time.Now()
	for {
		select {
		case <-w.exitChan:
			return
		default:
		}
		payloads, ok := w.queue.DequeueAllNB()
		if !ok {
			time.Sleep(w.sendInterval / 10)
			continue
		}
		for _, metric := range payloads.Metrics {
			metricsLen++
			if m, ok := seenMetrics[metric.Metric]; ok {
				m.Points = append(m.Points, metric.Points...)
			} else {
				seenMetrics[metric.Metric] = metric
				metrics.Metrics = append(metrics.Metrics, metric)
			}

			timeSpent := time.Since(lastSend)
			if timeSpent > w.sendInterval || metricsLen > w.maxBufferSize {
				w.logger.Debug("will send some data",
					zap.Duration("timePassed", timeSpent),
					zap.Duration("sendInterval", w.sendInterval),
					zap.Int("metricsBuffered", metricsLen),
					zap.Int("maxBufferSize", w.maxBufferSize),
				)
				err := w.send(metrics)
				if err != nil {
					w.logger.Error("error while sending data",
						zap.Error(err),
					)
					// TODO: Drop points if overflow
					w.writer.Close()
					w.reconnectChan <- struct{}{}
					continue
				}
				lastSend = time.Now()
				seenMetrics = make(map[string]*carbon.Metric, len(seenMetrics))
				metricsLen = 0
				metrics = &carbon.Payload{Metrics: make([]*carbon.Metric, 0, len(metrics.Metrics))}
			}
		}
	}
}

func NewAsyncWorker(id int, config transport.Config, queue *queue.SingleDeliveryQueue, exitChan <-chan struct{}) *asyncWorker {
	l := zapwriter.Logger("worker").With(
		zap.Int("id", id),
	)

	w := &asyncWorker{
		id:            id,
		exitChan:      exitChan,
		queue:         queue,
		server:        config.Servers[id],
		tls:           &config.TLS,
		proto:         config.Type.String(),
		logger:        l,
		reconnectChan: make(chan struct{}),
		sendInterval:  config.SendInterval,
		maxBufferSize: config.ChannelBufferSize,
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

	go w.TryConnect()
	w.reconnectChan <- struct{}{}

	l.Info("async worker started")

	return w
}
