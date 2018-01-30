package main

import (
	"bufio"
	"bytes"
	"io"
	"math"
	"net"
	"strconv"
	"time"

	"github.com/go-graphite/gorelka/hacks"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
	"sync/atomic"
)

type ReceiverStats struct {
	PointsReceived int64
	ParseErrors    int64
}

type Receiver struct {
	stats    ReceiverStats
	listener net.Listener
	exitChan <-chan struct{}

	logger *zap.Logger
}

func NewReceiver(t, listen string, exitChan <-chan struct{}) (*Receiver, error) {
	l, err := net.Listen(t, listen)
	if err != nil {
		return nil, err
	}

	r := &Receiver{
		listener: l,
		exitChan: exitChan,
		logger:   zapwriter.Logger("receiver"),
	}

	go r.handler()

	return r, nil
}

func (r *Receiver) handler() {
	for {
		select {
		case <-r.exitChan:
			r.listener.Close()
			return
		default:
		}

		conn, err := r.listener.Accept()
		if err != nil {
			r.logger.Error("error accepting connection",
				zap.Error(err),
			)
			continue
		}

		go r.processConnection(conn)
	}
}

func (r *Receiver) GetStats() ReceiverStats {
	stats := ReceiverStats{}

	stats.PointsReceived = atomic.LoadInt64(&r.stats.PointsReceived)
	stats.ParseErrors = atomic.LoadInt64(&r.stats.ParseErrors)

	return stats
}

func (r *Receiver) processConnection(conn net.Conn) {
	defer conn.Close()
	var err error

	reader := bufio.NewReader(conn)
	lastRcvDeadline := time.Now()
	timeout := 100 * time.Millisecond
	if err := conn.SetReadDeadline(lastRcvDeadline.Add(timeout)); err != nil {
		r.logger.Error("failed to set deadline",
			zap.Error(err),
		)
		return
	}

	for {
		now := time.Now()
		if now.Sub(lastRcvDeadline) > (timeout >> 2) {
			err = conn.SetDeadline(now.Add(timeout))
			if err != nil {
				r.logger.Error("failed to update deadline for connection",
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
				r.logger.Error("failed to read from connection",
					zap.Error(err),
				)
			}
			// Connection is now closed, exiting
			break
		}

		valid := r.validate(line)
		if valid {
			atomic.AddInt64(&r.stats.PointsReceived, 1)
		} else {
			atomic.AddInt64(&r.stats.ParseErrors, 1)
			/*
				r.logger.Warn("parse error",
					zap.String("line", hacks.UnsafeString(line)),
				)
			*/
		}
	}
}

func (r *Receiver) validate(line []byte) bool {
	s1 := bytes.IndexByte(line, ' ')
	// Some sane limit
	if s1 < 1 || s1 > GraphiteLineReceiverMaxLineSize {
		return false
	}

	s2 := bytes.IndexByte(line[s1+1:], ' ')
	if s2 < 1 {
		return false
	}
	s2 += s1 + 1

	value, err := strconv.ParseFloat(hacks.UnsafeString(line[s1+1:s2]), 64)
	if err != nil || math.IsNaN(value) {
		return false
	}
	s3 := len(line) - 1

	ts, err := strconv.ParseFloat(hacks.UnsafeString(line[s2+1:s3]), 64)
	if err != nil || math.IsNaN(ts) || math.IsInf(ts, 0) {
		return false
	}

	return true
}
