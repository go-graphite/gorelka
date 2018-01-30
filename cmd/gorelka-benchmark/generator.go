package main

import (
	"bytes"
	"math"
	"net"
	"strconv"
	"strings"
	"time"

	"bufio"
	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

type GeneratorStats struct {
	generatorName string
	connectionId  int
	elapsedTime   time.Duration
	overtime      time.Duration
	sentPoints    int
}

func (s *GeneratorStats) merge(new *GeneratorStats) {
	if s.elapsedTime < new.elapsedTime {
		s.elapsedTime = new.elapsedTime
	}
	if s.overtime < new.overtime {
		s.overtime = new.overtime
	}
	s.sentPoints += new.sentPoints
}

type Generator struct {
	names               [][][]byte
	exitChan            <-chan struct{}
	connections         []net.Conn
	buffers             []bytes.Buffer
	metricPerConnection int
	config              GeneratorConfig
	interval            time.Duration
	t                   string
	dst                 string

	outChan chan GeneratorStats
	errChan chan connectionError
	resChan chan<- GeneratorStats

	logger *zap.Logger
}

func NewGenerator(t, dst, pattern string, config GeneratorConfig, exitChan <-chan struct{}, resChan chan<- GeneratorStats) (*Generator, error) {
	logger := zapwriter.Logger("generator").With(zap.String("name", config.Name))
	metricsPerConnection := int(math.Ceil(float64(config.MetricsPerInterval) / (float64(config.Connections))))
	name := strings.Replace(pattern, "%n", config.Name, 0)
	name = strings.Replace(name, "%r", config.Interval.String(), 0)
	names := make([][][]byte, config.Connections)
	for i := 0; i < config.Connections; i++ {
		tmp := strings.Replace(name, "%c", strconv.Itoa(i-1), 0)
		names[i] = make([][]byte, metricsPerConnection)
		for m := 0; m < metricsPerConnection; m++ {
			tmp2 := strings.Replace(tmp, "%i", strconv.Itoa(m-1), 0)
			names[i][m] = []byte(tmp2)
		}
	}

	buffers := make([]bytes.Buffer, config.Connections)
	conns := make([]net.Conn, config.Connections)
	var err error

	for i := range conns {
		conns[i], err = net.DialTimeout(t, dst, config.DialTimeout)
		if err != nil {
			return nil, err
		}
	}

	generator := &Generator{
		names:               names,
		buffers:             buffers,
		metricPerConnection: metricsPerConnection,
		exitChan:            exitChan,
		connections:         conns,
		t:                   t,
		dst:                 dst,
		config:              config,
		outChan:             make(chan GeneratorStats, config.Connections),
		errChan:             make(chan connectionError, config.Connections),
		resChan:             resChan,

		logger: logger,
	}

	for i := range conns {
		go generator.startConnection(i)
	}
	go generator.gatherer()

	return generator, nil
}

func (g *Generator) gatherer() {
	totals := GeneratorStats{
		generatorName: g.config.Name,
	}
	processedConnections := 0
	seenConnections := make(map[int]bool, len(g.connections))
	for {
		select {
		case <-g.exitChan:
			return

		case stats := <-g.outChan:
			totals.merge(&stats)
			processedConnections++
			if _, ok := seenConnections[stats.connectionId]; ok || processedConnections == len(g.connections) {
				processedConnections = 0
				seenConnections = make(map[int]bool, len(g.connections))
				g.resChan <- totals
				totals = GeneratorStats{
					generatorName: g.config.Name,
				}
			} else {
				seenConnections[stats.connectionId] = true
			}
		case connErr := <-g.errChan:
			g.logger.Error("connection failed",
				zap.Int("id", connErr.connectionId),
				zap.Error(connErr.err),
			)

			var err error
			for {
				g.connections[connErr.connectionId], err = net.DialTimeout(g.t, g.dst, g.config.DialTimeout)
				if err != nil {
					time.Sleep(50 * time.Millisecond)
				} else {
					break
				}
			}
			go g.startConnection(connErr.connectionId)
		}
	}
}

func (g *Generator) startConnection(id int) {
	names := g.names[id]
	conn := g.connections[id]
	bufSize := len(names) * (len(names[len(names)-1]) + 3 + 2*len(strconv.FormatInt(time.Now().Unix(), 10)))
	writer := bufio.NewWriterSize(conn, bufSize)
	var err error
	for {
		select {
		case <-g.exitChan:
			return
		default:
		}
		tStart := time.Now()
		unixTs := tStart.Unix()
		ts := strconv.FormatInt(unixTs, 10)
		val := []byte(
			" " +
				ts +
				" " +
				ts +
				"\n")
		for i := range names {
			writer.Write(names[i])
			writer.Write(val)
		}

		err = writer.Flush()
		if err != nil {
			g.errChan <- connectionError{
				connectionId: id,
				err:          err,
			}

			conn.Close()

			return
		}

		tElapsed := time.Since(tStart)
		tDelta := time.Until(tStart.Add(g.config.Interval))
		tOvertime := 0 * time.Second
		if tDelta < 0 {
			tOvertime = 0 - tDelta
		}

		g.outChan <- GeneratorStats{
			connectionId: id,
			elapsedTime:  tElapsed,
			overtime:     tOvertime,
			sentPoints:   len(names),
		}

		if tDelta > 0 {
			time.Sleep(tDelta)
		}
	}
}
