package metrics

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"time"

	"expvar"
	"github.com/go-graphite/carbonzipper/mstats"
	"github.com/peterbourgon/g2g"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
)

type AggregationType int

const (
	Avg AggregationType = iota
	Sum
	Min
	Max
	Count
	Last
)

type Config struct {
	Type                string
	GraphiteHost        string
	MetricPrefixPattern string
	Prefix              string
	FQDN                string
	Tags                map[string]string
	Interval            time.Duration
	LogMetrics          bool
}

type Collector interface {
	Register(name string, value expvar.Var)
	RegisterWithAggregation(name string, value expvar.Var, t AggregationType)
	GetWithPrefix(prefix string) Collector
}

type graphiteState struct {
	metricsMutex sync.Mutex
	metrics      map[string]expvar.Var
}

type Graphite struct {
	prefix   string
	tags     string
	g        *g2g.Graphite
	interval time.Duration

	state *graphiteState
}

func mapToGraphiteTags(tags map[string]string) string {
	if tags == nil || len(tags) == 0 {
		return ""
	}

	res := make([]byte, 0)

	for k, v := range tags {
		tkv := []byte(fmt.Sprintf("%s=%s", k, v))
		if len(res) == 0 {
			res = append(res, ';')
		}
		res = append(res, tkv...)
	}

	return string(res)
}

var instance Collector
var once sync.Once

var supportedCollectors = map[string]func(*Config) Collector{
	"":          NewBlackhole,
	"graphite":  NewGraphite,
	"dummy":     NewBlackhole,
	"blackhole": NewBlackhole,
	"none":      NewBlackhole,
}

func NewCollector(config *Config) Collector {
	logger := zapwriter.Logger("collector")
	once.Do(func() {
		if init, ok := supportedCollectors[strings.ToLower(config.Type)]; ok {
			instance = init(config)
		} else {
			logger.Fatal("unsupported collector")
		}
	})
	return instance
}

func GetCollector() Collector {
	return NewCollector(&Config{})
}

func GetCollectorPrefixed(prefix string) Collector {
	return NewCollector(&Config{}).GetWithPrefix(prefix)
}

// NewGraphite creates a new instance of Graphite Sender
func NewGraphite(config *Config) Collector {
	logger := zapwriter.Logger("stats").With(zap.String("type", "graphite"))
	if config.GraphiteHost == "" || config.Interval == 0 {
		return nil
	}
	var fqdn string
	var err error
	if config.FQDN == "" {
		fqdn, err = os.Hostname()
		if err != nil {
			logger.Error("failed to get fqdn",
				zap.Error(err),
			)
			fqdn = "UNKNOWN"
		}
		fqdn = strings.Replace(fqdn, ".", "_", -1)
	}

	pattern := config.MetricPrefixPattern
	pattern = strings.Replace(pattern, "{prefix}", config.Prefix, -1)
	pattern = strings.Replace(pattern, "{fqdn}", fqdn, -1)

	g := &Graphite{
		g:        g2g.NewGraphite(config.GraphiteHost, config.Interval, 10*time.Second),
		prefix:   pattern,
		tags:     mapToGraphiteTags(config.Tags),
		interval: config.Interval,
		state: &graphiteState{
			metrics: make(map[string]expvar.Var),
		},
	}

	go mstats.Start(config.Interval)

	g.RegisterWithAggregation("memstats.alloc", &mstats.Alloc, Last)
	g.RegisterWithAggregation("memstats.total_alloc", &mstats.TotalAlloc, Last)
	g.RegisterWithAggregation("memstats.num_gc", &mstats.NumGC, Last)
	g.RegisterWithAggregation("memstats.pause_ns", &mstats.PauseNS, Last)

	if config.LogMetrics {
		go g.logMetricsToStdout()
	}

	return g
}

func (g *Graphite) logMetricsToStdout() {
	logger := zapwriter.Logger("stats")
	for {
		time.Sleep(g.interval)
		g.state.metricsMutex.Lock()
		tmp := make(map[string]string, len(g.state.metrics))
		for k, v := range g.state.metrics {
			tmp[k] = v.String()
		}
		logger.Info("stats logger",
			zap.Any("stats", tmp),
		)
		g.state.metricsMutex.Unlock()
	}
}

func (g Graphite) formatName(name string) string {
	return fmt.Sprintf("%s.%s%s", g.prefix, name, g.tags)
}

func (g Graphite) formatNameWithPostfix(name, postfix string) string {
	return fmt.Sprintf("%s.%s%s%s", g.prefix, name, postfix, g.tags)
}

// Register creates new graphite metric with name based on Prefix + specified name. Value is taken from expvars every Interval.
func (g *Graphite) Register(name string, value expvar.Var) {
	newName := g.formatName(name)
	g.state.metricsMutex.Lock()
	g.state.metrics[newName] = value
	g.state.metricsMutex.Unlock()
	g.g.Register(newName, value)
}

func (g *Graphite) RegisterWithAggregation(name string, value expvar.Var, t AggregationType) {
	postfix := "_avg"
	switch t {
	case Avg:
	case Count:
		postfix = "_count"
	case Min:
		postfix = "_min"
	case Max:
		postfix = "_max"
	case Last:
		postfix = "_last"
	case Sum:
		postfix = "_sum"
	}
	newName := g.formatNameWithPostfix(name, postfix)
	g.state.metricsMutex.Lock()
	g.state.metrics[newName] = value
	g.state.metricsMutex.Unlock()
	g.g.Register(newName, value)
}

func (g *Graphite) GetWithPrefix(prefix string) Collector {
	newG := &Graphite{
		prefix: g.prefix + "." + prefix,
		tags:   g.tags,
		g:      g.g,
		state:  g.state,
	}

	return newG
}

type Blackhole struct{}

func NewBlackhole(config *Config) Collector {
	return &Blackhole{}
}

func (g *Blackhole) Register(name string, value expvar.Var) {}

func (g *Blackhole) RegisterWithAggregation(name string, value expvar.Var, t AggregationType) {}

func (g *Blackhole) GetWithPrefix(prefix string) Collector {
	return g
}
