package main

import (
	"flag"
	"io/ioutil"
	"log"
	"time"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
	"gopkg.in/yaml.v2"
)

const (
	GraphiteLineReceiverMaxLineSize = 32 * 1024
)

var defaultLoggerConfig = zapwriter.Config{
	Logger:           "",
	File:             "stdout",
	Level:            "debug",
	Encoding:         "json",
	EncodingTime:     "iso8601",
	EncodingDuration: "seconds",
}

type SenderConfig struct {
	Type          string            `yaml:"type"`
	Destination   string            `yaml:"destination"`
	Runs          int               `yaml:"runs"` // 0 - unlimited
	MetricPattern string            `yaml:"metricPattern"`
	Generators    []GeneratorConfig `yaml:"generators"`
}

type GeneratorConfig struct {
	Name               string        `yaml:"name"`
	Connections        int           `yaml:"connections"`
	MetricsPerInterval int           `yaml:"metricsPerInterval"`
	Interval           time.Duration `yaml:"interval"`
	DialTimeout        time.Duration `yaml:"dialTimeout"`
	SendTimeout        time.Duration `yaml:"sendTimeout"`
}

type ReceiverConfig struct {
	Enabled       bool          `yaml:"enabled"`
	StatsInterval time.Duration `yaml:"statsInterval"`
	Listen        string        `yaml:"listen"`
	Type          string        `yaml:"type"`
}

var config = struct {
	Sender   SenderConfig   `yaml:"sender"`
	Receiver ReceiverConfig `yaml:"receiver"`
}{
	Sender: SenderConfig{
		Type:          "tcp",
		Destination:   "127.0.0.1:2003",
		Runs:          0,
		MetricPattern: "test.%n.%r.conn%c.metric%i", //  %n - generatorName, %r - resolution, %c - connectionNumber, %i metricNumber
		Generators: []GeneratorConfig{
			{
				Name:               "secondly",
				Connections:        50,
				MetricsPerInterval: 100000,
				Interval:           1 * time.Second,
				DialTimeout:        100 * time.Millisecond,
				SendTimeout:        1 * time.Second,
			},
		},
	},
	Receiver: ReceiverConfig{
		Enabled: false,
		Listen:  "127.0.0.1:2004",
		Type:    "tcp",
	},
}

type connectionError struct {
	err error

	connectionId int
}

func printReceiverStats(r *Receiver, interval time.Duration, exitChan <-chan struct{}) {
	logger := zapwriter.Logger("receiver").With(zap.String("type", "GeneratorStats"))
	statsPrinter := time.Tick(interval)
	oldStats := ReceiverStats{}
	for {
		select {
		case <-exitChan:
			return
		case <-statsPrinter:
			stats := r.GetStats()
			pointsPerSecond := float64(stats.PointsReceived-oldStats.PointsReceived) / interval.Seconds()
			errorsPerSecond := float64(stats.ParseErrors-oldStats.ParseErrors) / interval.Seconds()

			logger.Info("GeneratorStats",
				zap.Float64("pointsPerSecond", pointsPerSecond),
				zap.Float64("errorsPerSecond", errorsPerSecond),
				zap.Int64("pointsReceived", stats.PointsReceived),
				zap.Int64("parseErrors", stats.ParseErrors),
			)
			oldStats = stats
		}

	}

}

func main() {
	err := zapwriter.ApplyConfig([]zapwriter.Config{defaultLoggerConfig})
	if err != nil {
		log.Fatal("failed to initialize logger with default configuration")

	}
	logger := zapwriter.Logger("main")

	configFile := flag.String("config", "", "config file (yaml)")
	flag.Parse()

	if *configFile == "" {
		logger.Fatal("missing config file option")
	}

	cfg, err := ioutil.ReadFile(*configFile)
	if err != nil {
		logger.Fatal("unable to load config file:",
			zap.Error(err),
		)
	}

	err = yaml.Unmarshal(cfg, &config)
	if err != nil {
		logger.Fatal("failed to parse config",
			zap.String("config_path", *configFile),
			zap.Error(err),
		)
	}

	run := 0
	actualRun := 0
	exitChan := make(chan struct{})
	resChan := make(chan GeneratorStats)

	if config.Receiver.Enabled {
		logger.Info("initializing receiver")
		r, err := NewReceiver(config.Receiver.Type, config.Receiver.Listen, exitChan)
		if err != nil {
			logger.Fatal("failed to initialize receiver",
				zap.Error(err),
				zap.Any("config", config.Receiver),
			)
		}

		go printReceiverStats(r, config.Receiver.StatsInterval, exitChan)
	}

	for _, generatorConfig := range config.Sender.Generators {
		_, err := NewGenerator(config.Sender.Type, config.Sender.Destination, config.Sender.MetricPattern, generatorConfig, exitChan, resChan)
		if err != nil {
			logger.Fatal("failed to initialize generator",
				zap.Error(err),
				zap.Any("config", generatorConfig),
			)
		}
	}

	logger.Info("started",
		zap.Any("config", config),
	)

	for run <= config.Sender.Runs {
		t0 := time.Now()
		actualRun++
		if config.Sender.Runs != 0 {
			run++
		}

		results := 0
		s := GeneratorStats{}
		for {
			select {
			case r := <-resChan:
				s.sentPoints += r.sentPoints
				results++
				s.merge(&r)

				speed := float64(r.sentPoints) / r.elapsedTime.Seconds()

				logger.Info("generator done",
					zap.Duration("runtime", time.Since(t0)),
					zap.Duration("elapsedTime", s.elapsedTime),
					zap.String("generator", r.generatorName),
					zap.Int("sentPoints", r.sentPoints),
					zap.Float64("speed", speed),
					zap.Duration("overtime", r.overtime),
				)
			}

			if results > len(config.Sender.Generators) {
				break
			}
		}

		avgSpeed := float64(s.sentPoints) / s.elapsedTime.Seconds()
		logger.Info("run done",
			zap.Duration("runtime", time.Since(t0)),
			zap.Int("sentPoints", s.sentPoints),
			zap.Float64("averagePointsPerSecond", avgSpeed),
			zap.Duration("overtime", s.overtime),
		)
	}

	close(exitChan)
}
