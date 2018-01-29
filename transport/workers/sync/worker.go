package sync

import (
	"crypto/tls"
	"io"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-graphite/gorelka/carbon"
	"github.com/go-graphite/gorelka/encoders/graphite"
	transport "github.com/go-graphite/gorelka/transport/common"
	"github.com/go-graphite/gorelka/transport/workers"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

type syncWorker struct {
	id    int
	alive int64

	tls    *transport.TLSConfig
	server string
	proto  string

	compressor     func(w net.Conn) (io.ReadWriteCloser, error)
	marshaller     func(payload *carbon.Payload) ([]byte, error)
	writer         io.ReadWriteCloser
	exitChan       <-chan struct{}
	reconnectChan  chan struct{}
	leftoversQueue chan []byte
	queue          chan *carbon.Metric
	stats          workers.WorkerStats

	logger *zap.Logger
}

func (w *syncWorker) TryConnect() {
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
					w.logger.Error("error while connecting to upstream",
						zap.Error(err),
					)
					atomic.AddInt64(&w.stats.ConnectErrors, 1)
					time.Sleep(250 * time.Millisecond)
					continue
				}

				w.writer, err = w.compressor(conn)
				if err != nil {
					w.logger.Error("error initializing compressor",
						zap.Error(err),
					)
					atomic.AddInt64(&w.stats.ConnectErrors, 1)
					conn.Close()
					time.Sleep(250 * time.Millisecond)
					continue
				}
				atomic.StoreInt64(&w.alive, 1)
				go w.Loop()
				w.logger.Debug("connection established")
				break
			}
		}

	}
}

func (w syncWorker) IsAlive() bool {
	alive := atomic.LoadInt64(&w.alive)

	return alive == 1
}

func (w *syncWorker) GetStats() *workers.WorkerStats {
	stats := &workers.WorkerStats{
		SpentTime:  atomic.LoadInt64(&w.stats.SpentTime),
		SentPoints: atomic.LoadInt64(&w.stats.SentPoints),
		SendErrors: atomic.LoadInt64(&w.stats.SendErrors),
	}

	return stats
}

func (w *syncWorker) send(metric *carbon.Metric) error {
	t0 := time.Now()
	data, err := w.marshaller(&carbon.Payload{Metrics: []*carbon.Metric{metric}})
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
	atomic.AddInt64(&w.stats.SentPoints, 1)

	return nil
}

func (w *syncWorker) Loop() {
	w.logger.Debug("worker started")
	for {
		select {
		case <-w.exitChan:
			return
		case data := <-w.leftoversQueue:
			w.logger.Debug("got some leftovers")
			w.writer.Write(data)
		case metric := <-w.queue:
			w.logger.Debug("will send some data")
			err := w.send(metric)
			if err != nil {
				w.logger.Error("error while sending data",
					zap.Error(err),
				)
				leftovers := make([]byte, 0)
				n, _ := w.writer.Read(leftovers)
				if n != 0 {
					select {
					case w.leftoversQueue <- leftovers:
					default:
						atomic.AddInt64(&w.stats.DroppedPoints, 1)
						w.logger.Debug("queue is full, point dropped")
					}

				}
				w.writer.Close()
				select {
				case w.queue <- metric:
				default:
					atomic.AddInt64(&w.stats.DroppedPoints, 1)
					w.logger.Debug("queue is full, point dropped")
				}
				w.reconnectChan <- struct{}{}
				return
			}
		}
	}
}

func NewSyncWorker(id int, config transport.Config, queue chan *carbon.Metric, exitChan <-chan struct{}) *syncWorker {
	l := zapwriter.Logger("worker").With(
		zap.Int("id", id),
		zap.String("server", config.Servers[id]),
	)

	w := &syncWorker{
		id:            id,
		exitChan:      exitChan,
		queue:         queue,
		server:        config.Servers[id],
		tls:           &config.TLS,
		proto:         config.Type.String(),
		logger:        l,
		reconnectChan: make(chan struct{}),
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

	return w
}
