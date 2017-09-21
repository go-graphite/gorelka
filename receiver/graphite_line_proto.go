package receiver

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"math"
	"net"
	"strconv"
	"time"

	"github.com/pkg/errors"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"

	"github.com/go-graphite/g2mt/carbon"
	"github.com/go-graphite/g2mt/hacks"
	"github.com/go-graphite/g2mt/queue"
	"github.com/go-graphite/g2mt/routers"
	"strings"
	"sync/atomic"
)

type Metrics struct {
	ProcessedMetrics uint64
	ProcessingTimeNS uint64
}

type Config struct {
	Listen   string
	Protocol string
	Workers  int
}

type listenerType int

const (
	tcpListener  listenerType = 0
	udpListener  listenerType = 1
	unixListener listenerType = 2
)

type GraphiteLineReceiver struct {
	Config

	sendInterval time.Duration
	maxBatchSize int
	exitChan     <-chan struct{}
	processQueue []*queue.SingleDeliveryQueueByte

	listener interface{}
	lType    listenerType
	logger   *zap.Logger
	router   routers.Router

	Metrics Metrics
}

const (
	GraphiteLineReceiverMaxLineSize = 32 * 1024
)

var errFmtParseError = errors.New("parse failed")

func NewGraphiteLineReceiver(config Config, router routers.Router, exitChan <-chan struct{}, maxBatchSize int, sendInterval time.Duration) (*GraphiteLineReceiver, error) {
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

	return graphiteLineReceiverInit(listener, lType, config, router, exitChan, maxBatchSize, sendInterval), nil
}

func graphiteLineReceiverInit(listener interface{}, lType listenerType, config Config, router routers.Router, exitChan <-chan struct{}, maxBatchSize int, sendInterval time.Duration) *GraphiteLineReceiver {
	r := &GraphiteLineReceiver{
		Config:       config,
		listener:     listener,
		lType:        lType,
		maxBatchSize: maxBatchSize,
		exitChan:     exitChan,
		router:       router,
		sendInterval: sendInterval,

		logger: zapwriter.Logger("graphite"),
	}

	for i := 0; i < config.Workers; i++ {
		r.processQueue = append(r.processQueue, queue.NewSingleDeliveryQueueByte(1024))
	}
	return r
}

func (l *GraphiteLineReceiver) Start() {
	for i := 0; i < l.Workers; i++ {
		go l.validateAndParse(i)
	}

	l.logger.Info("started")

	acceptTimeout := 50 * time.Millisecond
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
				if now.Sub(lastRcvDeadline) > (acceptTimeout >> 2) {
					err = listener.SetDeadline(now.Add(acceptTimeout))
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
				if now.Sub(lastRcvDeadline) > (acceptTimeout >> 2) {
					err = listener.SetDeadline(now.Add(acceptTimeout))
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
				if now.Sub(lastRcvDeadline) > (acceptTimeout >> 2) {
					err = conn.SetDeadline(now.Add(acceptTimeout))
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
			l.logger.Info("received connection")

			go l.processGraphiteConnection(conn)
		}
	}
}

func (l *GraphiteLineReceiver) validateAndParse(id int) {
	processTicker := time.NewTicker(5 * time.Millisecond)
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
				metric, err := l.Parse(line)
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

func (l *GraphiteLineReceiver) Parse(line []byte) (*carbon.Metric, error) {
	s1 := bytes.IndexByte(line, ' ')
	// Some sane limit
	if s1 < 1 || s1 > GraphiteLineReceiverMaxLineSize {
		return nil, errors.Wrap(errFmtParseError, "line is too large or malformed")
	}

	s2 := bytes.IndexByte(line[s1+1:], ' ')
	if s2 < 1 {
		return nil, errors.Wrap(errFmtParseError, "no value field")
	}
	s2 += s1 + 1

	value, err := strconv.ParseFloat(hacks.UnsafeString(line[s1+1:s2]), 64)
	if err != nil || math.IsNaN(value) {
		return nil, errors.Wrap(errFmtParseError, "invalid value")
	}
	s3 := len(line) - 1

	ts, err := strconv.ParseFloat(hacks.UnsafeString(line[s2+1:s3]), 64)
	if err != nil || math.IsNaN(ts) || math.IsInf(ts, 0) {
		return nil, errors.Wrap(errFmtParseError, "invalid timestamp")
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
		l.logger.Info("Finished processing of connection")
		err := c.Close()
		if err != nil {
			l.logger.Error("failed to close connection",
				zap.Error(err),
			)
		}
	}()

	reader := bufio.NewReaderSize(c, GraphiteLineReceiverMaxLineSize)

	lastRcvDeadline := time.Now()
	readTimeout := l.sendInterval
	err := c.SetReadDeadline(lastRcvDeadline.Add(readTimeout))
	if err != nil {
		l.logger.Error("failed to set deadline",
			zap.Error(err),
		)
		return
	}

	sentMetrics := 0
	sendTicker := time.NewTicker(l.sendInterval)
	buffer := make([][]byte, 0)
	forceChan := make(chan struct{}, 1)
	cnt := 0
	for {
		select {
		case <-forceChan:
			l.processQueue[cnt].EnqueueMany(buffer)
			cnt++
			if cnt >= l.Workers {
				cnt = 0
			}
			buffer = make([][]byte, 0)
			/*
				l.logger.Debug("sent metrics",
					zap.Bool("send_ticker", false),
					zap.Int("metrics", sentMetrics),
				)
			*/
			sentMetrics = 0
		case <-sendTicker.C:
			l.processQueue[cnt].EnqueueMany(buffer)
			cnt++
			if cnt >= l.Workers {
				cnt = 0
			}
			buffer = make([][]byte, 0)
			/*
				l.logger.Debug("sent metrics",
					zap.Bool("send_ticker", true),
					zap.Int("metrics", sentMetrics),
				)
			*/
			sentMetrics = 0
		case <-l.exitChan:
			sendTicker.Stop()
			return
		default:

		}

		now := time.Now()
		if now.Sub(lastRcvDeadline) > (readTimeout >> 2) {
			err = c.SetDeadline(now.Add(readTimeout))
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
					continue
				}
				l.logger.Error("failed to read from connection",
					zap.Error(err),
				)
			}
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
		if len(buffer) >= l.maxBatchSize {
			forceChan <- struct{}{}
		}
	}

	l.logger.Info("Shutting down... Flushing buffer")
	if len(buffer) > 0 {
		l.processQueue[0].EnqueueMany(buffer)
	}
	sendTicker.Stop()
}
