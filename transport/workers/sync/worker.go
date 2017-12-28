package sync

import (
	"time"
	"sync/atomic"
	"io"
	"strings"
	"net"
	"crypto/tls"

	"github.com/go-graphite/g2mt/carbon"
	transport "github.com/go-graphite/g2mt/transport/common"
	"github.com/go-graphite/g2mt/encoders/graphite"
	"github.com/go-graphite/g2mt/transport/workers"
)

type syncWorker struct {
	id int
	alive int64

	tls *transport.TLSConfig
	server string
	proto string

	compressor func(w net.Conn) (io.WriteCloser, error)
	marshaller func(payload *carbon.Payload) ([]byte, error)
	writer     io.WriteCloser
	exitChan   <-chan struct{}
	queue      chan *carbon.Metric
	stats      workers.WorkerStats
}

func (w *syncWorker) TryConnect() {
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
				ServerName: w.server,
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
	atomic.AddInt64(&w.stats.SentPoints,1)

	return nil
}


func (w *syncWorker) Loop() {
	for {
		select {
		case <-w.exitChan:
			return
		case metric := <-w.queue:
			err := w.send(metric)
			if err != nil {
				atomic.StoreInt64(&w.alive, 0)
				return
			}
		}
	}
}

func NewSyncWorker(id int, config transport.Config, queue chan *carbon.Metric, exitChan <-chan struct{}) *syncWorker {
	w := &syncWorker{
		id: id,
		exitChan: exitChan,
		queue: queue,
		server: config.Servers[id],
		tls: &config.TLS,
		proto: config.Type.String(),
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

	return w
}
