package receiver

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-graphite/g2mt/carbon"
	"github.com/go-graphite/g2mt/hacks"
	"github.com/go-graphite/g2mt/queue"
	"github.com/go-graphite/g2mt/routers"
	"github.com/go-graphite/g2mt/transport/workers"

	"github.com/lomik/zapwriter"
	"github.com/pkg/errors"
	"go.uber.org/zap"
)

type Metrics struct {
	ProcessedMetrics uint64
	ProcessingTimeNS uint64
}

type Config struct {
	Listen   string
	Protocol string
	Workers  int
	Strict   bool

	Decompression string `json:"Decompression"`

	Tags Tags // atomic.Value
}

type listenerType int

const (
	tcpListener  listenerType = 0
	udpListener  listenerType = 1
	unixListener listenerType = 2
)

type GraphiteLineReceiver struct {
	Config

	sendInterval  time.Duration
	acceptTimeout time.Duration
	maxBatchSize  int
	exitChan      <-chan struct{}
	processQueue  []*queue.SingleDeliveryQueueByte

	listener interface{}
	lType    listenerType
	logger   *zap.Logger
	router   routers.Router

	Metrics Metrics

	decompressor workers.Decompressor
}

const (
	GraphiteLineReceiverMaxLineSize = 32 * 1024
)

var errFmtParseError = errors.New("parse failed")

func NewGraphiteLineReceiver(config Config, router routers.Router, exitChan <-chan struct{}, maxBatchSize, queueSize int, sendInterval, acceptTimeout time.Duration) (*GraphiteLineReceiver, error) {
	lType := tcpListener
	var listener interface{}
	switch strings.ToLower(config.Protocol) {
	case "tcp", "tcp4", "tcp6":
		lType = tcpListener
		n := strings.ToLower(config.Protocol)
		addr, err := net.ResolveTCPAddr(n, config.Listen)
		if err != nil {
			return nil, err
		}

		l, err := net.ListenTCP(n, addr)
		if err != nil {
			return nil, err
		}

		listener = l
	case "unix":
		lType = unixListener
		n := strings.ToLower(config.Protocol)
		addr, err := net.ResolveUnixAddr(n, config.Listen)
		if err != nil {
			return nil, err
		}

		l, err := net.ListenUnix(n, addr)
		if err != nil {
			return nil, err
		}

		listener = l
	case "udp":
		lType = udpListener
		n := strings.ToLower(config.Protocol)
		addr, err := net.ResolveUDPAddr(n, config.Listen)
		if err != nil {
			return nil, err
		}

		listener = addr
	default:
		return nil, fmt.Errorf("Unknown protocol %v", config.Protocol)
	}

	if router == nil {
		return nil, fmt.Errorf("Router is not defined")
	}

	if sendInterval == 0 {
		return nil, fmt.Errorf("SendInterval must be >0")
	}

	return graphiteLineReceiverInit(listener, lType, config, router, exitChan, maxBatchSize, queueSize, sendInterval, acceptTimeout), nil
}

func graphiteLineReceiverInit(listener interface{}, lType listenerType, config Config, router routers.Router, exitChan <-chan struct{}, maxBatchSize, queueSize int, sendInterval, acceptTimeout time.Duration) *GraphiteLineReceiver {
	r := &GraphiteLineReceiver{
		Config:        config,
		listener:      listener,
		lType:         lType,
		maxBatchSize:  maxBatchSize,
		exitChan:      exitChan,
		router:        router,
		sendInterval:  sendInterval,
		acceptTimeout: acceptTimeout,

		logger: zapwriter.Logger("graphite"),
	}

	for i := 0; i < config.Workers; i++ {
		r.processQueue = append(r.processQueue, queue.NewSingleDeliveryQueueByte(int64(queueSize)))
	}

	r.decompressor = workers.NewDecompressor(config.Decompression)
	return r
}

func (l *GraphiteLineReceiver) Start() {
	for i := 0; i < l.Workers; i++ {
		go l.validateAndParse(i)
	}

	l.logger.Info("started")

	var lastRcvDeadline time.Time
	for {
		select {
		case <-l.exitChan:
			switch l.lType {
			case tcpListener:
				listener := l.listener.(*net.TCPListener)
				listener.Close()
			case unixListener:
				listener := l.listener.(*net.UnixListener)
				listener.Close()
			default:
			}
			return
		default:
			var conn net.Conn
			var err error
			switch l.lType {
			case tcpListener:
				listener := l.listener.(*net.TCPListener)
				now := time.Now()
				if now.Sub(lastRcvDeadline) > (l.acceptTimeout >> 2) {
					err = listener.SetDeadline(now.Add(l.acceptTimeout))
					if err != nil {
						l.logger.Error("failed to update deadline for connection",
							zap.Error(err),
						)
						continue
					}
					lastRcvDeadline = now
				}
				conn, err = listener.Accept()
			case unixListener:
				listener := l.listener.(*net.UnixListener)
				now := time.Now()
				if now.Sub(lastRcvDeadline) > (l.acceptTimeout >> 2) {
					err = listener.SetDeadline(now.Add(l.acceptTimeout))
					if err != nil {
						l.logger.Error("failed to update deadline for connection",
							zap.Error(err),
						)
						continue
					}
					lastRcvDeadline = now
				}
				conn, err = listener.Accept()
			case udpListener:
				addr := l.listener.(*net.UDPAddr)
				conn, err = net.ListenUDP("udp", addr)

				now := time.Now()
				if now.Sub(lastRcvDeadline) > (l.acceptTimeout >> 2) {
					err = conn.SetDeadline(now.Add(l.acceptTimeout))
					if err != nil {
						l.logger.Error("failed to update deadline for connection",
							zap.Error(err),
						)
						continue
					}
					lastRcvDeadline = now
				}
			}

			if err != nil {
				if opError, ok := err.(*net.OpError); ok && opError.Timeout() {
					// Silently ignore timeout
					continue
				}
				l.logger.Error("failed to accept connection",
					zap.Error(err),
				)
				continue
			}
			l.logger.Debug("received connection")

			go l.processGraphiteConnection(conn)
		}
	}
}

func (l *GraphiteLineReceiver) validateAndParse(id int) {
	processTicker := time.NewTicker(5 * time.Millisecond)
	var err error
	var metric *carbon.Metric
	var parse func(line []byte) (*carbon.Metric, error)
	if l.Strict {
		l.logger.Debug("will use strict parser")
		parse = l.Parse
	} else {
		l.logger.Debug("will use relaxed parser")
		parse = l.parseRelaxed
	}
	for {
		select {
		case <-l.exitChan:
			processTicker.Stop()
			return
		case <-processTicker.C:
			d, ok := l.processQueue[id].DequeueAll()
			if !ok {
				continue
			}
			t0 := time.Now()
			payload := carbon.Payload{}
			for _, line := range d {
				metric, err = parse(line)
				if err != nil {
					l.logger.Error("error parsing line protocol",
						zap.String("line", hacks.UnsafeString(line)),
						zap.Error(err),
					)
					continue
				}
				payload.Metrics = append(payload.Metrics, metric)
			}
			if len(payload.Metrics) > 0 {

				dt := time.Since(t0).Nanoseconds()
				atomic.AddUint64(&l.Metrics.ProcessingTimeNS, uint64(dt))
				atomic.AddUint64(&l.Metrics.ProcessedMetrics, uint64(len(d)))
				speed := float64(len(d)) / float64(dt) * 1000000000
				l.logger.Debug("Parsing done",
					zap.Float64("speed_metrics_per_second", speed),
				)
				go l.router.Route(payload)
			}
		}
	}
}

func (l *GraphiteLineReceiver) parseRelaxed(line []byte) (data *carbon.Metric, err error) {
	var s1, s2, s3 int

	defer func() {
		if r := recover(); r != nil {
			l.logger.Error("panic occurred while parsing the string",
				zap.Int("s2", s2),
				zap.Int("s3", s3),
				zap.String("last byte", string(line[s3])),
				zap.Int("len", len(line)),
				zap.Any("recovered panic", r),
				zap.String("line", hacks.UnsafeString(line)),
			)
			data = nil
			err = errors.Wrap(errFmtParseError, "unknown error occurred")
		}
	}()
	s1 = bytes.IndexByte(line, ' ')
	// Some sane limit
	if s1 < 1 || s1 > GraphiteLineReceiverMaxLineSize {
		return nil, errors.WithMessage(errFmtParseError, "line is too large or malformed")
	}
	s1skipped := s1
	for line[s1skipped+1] == ' ' && s1skipped < len(line)-1 {
		s1skipped++
	}

	s2 = bytes.IndexByte(line[s1skipped+1:], ' ')
	if s2 < 1 {
		return nil, errors.WithMessage(errFmtParseError, "no value field")
	}
	s2 += s1skipped + 1

	value, err := strconv.ParseFloat(hacks.UnsafeString(line[s1skipped+1:s2]), 64)
	if err != nil || math.IsNaN(value) {
		return nil, errors.WithMessage(errFmtParseError, "invalid value")
	}
	s3 = len(line) - 1
	for {
		if (line[s3-1] == '\r' || line[s3-1] == '\n' || line[s3-1] == 0) && s3 > s2 {
			s3--
		} else {
			break
		}
	}
	for line[s2+1] == ' ' {
		s2++
	}

	if s2+1 >= s3 {
		return nil, errors.WithMessage(errFmtParseError, "invalid timestamp")
	}

	ts, err := strconv.ParseFloat(hacks.UnsafeString(line[s2+1:s3]), 64)
	if err != nil || math.IsNaN(ts) || math.IsInf(ts, 0) {
		return nil, errors.WithMessage(errFmtParseError, "invalid timestamp")
	}

	p := &carbon.Metric{
		Metric: hacks.UnsafeString(line[:s1]),
		Points: []carbon.Point{{
			Value:     value,
			Timestamp: uint32(ts),
		}},
	}

	return p, nil
}

func (l *GraphiteLineReceiver) Parse(line []byte) (*carbon.Metric, error) {
	s1 := bytes.IndexByte(line, ' ')
	// Some sane limit
	if s1 < 1 || s1 > GraphiteLineReceiverMaxLineSize {
		return nil, errors.WithMessage(errFmtParseError, "line is too large or malformed")
	}

	s2 := bytes.IndexByte(line[s1+1:], ' ')
	if s2 < 1 {
		return nil, errors.WithMessage(errFmtParseError, "no value field")
	}
	s2 += s1 + 1

	value, err := strconv.ParseFloat(hacks.UnsafeString(line[s1+1:s2]), 64)
	if err != nil || math.IsNaN(value) {
		return nil, errors.WithMessage(errFmtParseError, "invalid value")
	}
	s3 := len(line) - 1

	ts, err := strconv.ParseFloat(hacks.UnsafeString(line[s2+1:s3]), 64)
	if err != nil || math.IsNaN(ts) || math.IsInf(ts, 0) {
		return nil, errors.WithMessage(errFmtParseError, "invalid timestamp")
	}

	p := &carbon.Metric{
		Metric: hacks.UnsafeString(line[:s1]),
		Points: []carbon.Point{{
			Value:     value,
			Timestamp: uint32(ts),
		}},
	}

	return p, nil
}

func (l *GraphiteLineReceiver) processGraphiteConnection(c net.Conn) {
	defer func() {
		l.logger.Debug("Finished processing of connection")
		err := c.Close()
		if err != nil {
			l.logger.Error("failed to close connection",
				zap.Error(err),
			)
		}
	}()

	dc, err := l.decompressor(c)
	if err != nil {
		l.logger.Error("failed to create decompressor",
			zap.String("decompression", l.Decompression),
			zap.Error(err),
		)
		return
	}
	reader := bufio.NewReaderSize(dc, GraphiteLineReceiverMaxLineSize)

	lastRcvDeadline := time.Now()
	if err := c.SetReadDeadline(lastRcvDeadline.Add(l.sendInterval)); err != nil {
		l.logger.Error("failed to set deadline",
			zap.Error(err),
		)
		return
	}

	sentMetrics := 0
	buffer := make([][]byte, 0)
	forceChan := make(chan struct{}, 1)
	cnt := 0
	lastSentTime := time.Now()
	for {
		select {
		case <-forceChan:
			for {
				err = l.processQueue[cnt].EnqueueMany(buffer)
				if err == nil {
					break
				}
				time.Sleep(l.sendInterval / 10)
			}

			cnt++
			if cnt >= l.Workers {
				cnt = 0
			}
			buffer = make([][]byte, 0)
			sentMetrics = 0
			lastSentTime = time.Now()
		case <-l.exitChan:
			return
		default:
		}

		now := time.Now()
		if now.Sub(lastRcvDeadline) > (l.sendInterval >> 2) {
			err = c.SetDeadline(now.Add(l.sendInterval))
			if err != nil {
				l.logger.Error("failed to update deadline for connection",
					zap.Error(err),
				)
				continue
			}
			lastRcvDeadline = now
		}

		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err != io.EOF {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					// Silently ignore read timeouts - that might mean that connection is idle
					continue
				}
				l.logger.Error("failed to read from connection",
					zap.Error(err),
				)
			}
			// Connection is now closed, exiting
			break
		}

		if len(line) > GraphiteLineReceiverMaxLineSize {
			l.logger.Error("faild to parse protocol",
				zap.String("line", hacks.UnsafeString(line)),
				zap.Error(errors.Wrap(errFmtParseError, "line is too large or malformed")),
			)
			break
		}

		idx := bytes.IndexByte(line, ' ')
		if idx == -1 {
			l.logger.Error("failed to parse protocol",
				zap.String("line", hacks.UnsafeString(line)),
				zap.Error(err),
			)
			continue
		}
		buffer = append(buffer, line)
		sentMetrics++
		if len(buffer) >= l.maxBatchSize || time.Since(lastSentTime) > l.sendInterval {
			forceChan <- struct{}{}
		}
	}

	l.logger.Debug("Connection closed. Flushing buffer")
	if len(buffer) > 0 {
		for {
			err = l.processQueue[0].EnqueueMany(buffer)
			if err == nil {
				break
			}
			time.Sleep(l.sendInterval / 10)
		}
	}
}
