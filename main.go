package main

import (
	"expvar"
	"flag"
	"log"
	"runtime"

	"github.com/go-graphite/g2mt/receiver"
	"github.com/go-graphite/g2mt/transport"
	"github.com/lomik/zapwriter"
	"github.com/spf13/viper"
	"go.uber.org/zap"
	"net/http"
	_ "net/http/pprof"

	"github.com/go-graphite/g2mt/distribution"
	"github.com/go-graphite/g2mt/routers"
	"github.com/Shopify/sarama"
	"time"
)

var defaultLoggerConfig = zapwriter.Config{
	Logger:           "",
	File:             "stdout",
	Level:            "debug",
	Encoding:         "json",
	EncodingTime:     "iso8601",
	EncodingDuration: "seconds",
}

type debugConfig struct {
	Listen string
}

type receiverConfig struct {
	Type         string
	Router       string
	SendInterval time.Duration
	Config       []receiver.Config
}

type transportConfig struct {
	Type   string
	Router string
	Config []transport.Config
}

type routerConfig struct {
	Type   string
	Config routers.Config
}

type listenerConf struct {
	Port                  int
	Transports            map[string]transportConfig
	Receivers             map[string]receiverConfig
	Routers               map[string]routerConfig
	MaxBatchSize          int
	TransportWorkers      int
	TransportChanCapacity int
	SendInterval          time.Duration
	QueueSize             int
}

var config = struct {
	Logger    []zapwriter.Config `yaml:"logger"`
	Listeners []listenerConf

	Debug debugConfig
}{
	Listeners: []listenerConf{{
		MaxBatchSize:          500000,
		SendInterval:          200 * time.Millisecond,
		TransportWorkers:      4,
		TransportChanCapacity: 64 * 1024,
		Receivers: map[string]receiverConfig{
			"graphite": {
				Type:         "graphite",
				Router:       "default_relay",
				SendInterval: 100 * time.Millisecond,
				Config: []receiver.Config{{
					Listen:   ":2003",
					Protocol: "tcp",
					Workers:  6,
				}},
			},
		},
		Routers: map[string]routerConfig{
			"default_relay": {
				Type: "relay",
				Config: routers.Config{
					Rules: []routers.Rule{
						{
							Regex:         "^(rewrite_me)\\.(.*)",
							RewriteTo:     "carbon.$2",
							LastIfMatched: true,
							LogOnReceive:  true,
							Blackhole:     true,
						},
						{
							StartsWith:    "carbon.DONT_SEND_ME",
							LastIfMatched: true,
							Blackhole:     true,
						},
						{
							StartsWith:    "carbon.",
							LastIfMatched: true,
							Destinations: []string{
								"kafka-carbon-ams4",
								"kafka-carbon-lhr4",
							},
						},
					},
				},
			},
		},
		Transports: map[string]transportConfig{
			"kafka": {
				Type:   "kafka",
				Router: "default_relay",
				Config: []transport.Config{
					{
						Name:                  "carbon-ams4",
						Shards:                1,
						DistributionAlgorithm: distribution.JumpFNV1a,
						Compression:           "snappy",
						Brokers:               []string{"localhost:9092"},
						RequiredAcks:          sarama.NoResponse,
						RetryMax:              3,
						Topic:                 "graphite-carbon-metrics-ams4",
						FlushFrequency:        200 * time.Millisecond,
						ChannelBufferSize:     100000,
					},
					{
						Name:                  "carbon-lhr4",
						Shards:                1,
						DistributionAlgorithm: distribution.JumpFNV1a,
						Compression:           "snappy",
						Brokers:               []string{"localhost:9092"},
						RequiredAcks:          sarama.NoResponse,
						RetryMax:              3,
						Topic:                 "graphite-carbon-metrics-lhr4",
						FlushFrequency:        200 * time.Millisecond,
						ChannelBufferSize:     100000,
					},
				},
			},
		},
	}},
	Debug: debugConfig{
		Listen: ":6060",
	},
	Logger: []zapwriter.Config{defaultLoggerConfig},
}

func errorPrinter(exitChan <-chan struct{}, errChan <-chan error) {
	logger := zapwriter.Logger("errorLogger")
	select {
	case <-exitChan:
		return
	case err := <-errChan:
		logger.Error("error occured",
			zap.Error(err),
			zap.Stack("stack"),
		)
	}
}

// BuildVersion contains version and/or commit of current build. Defaults to "Development"
var BuildVersion = "Development"

func main() {
	err := zapwriter.ApplyConfig([]zapwriter.Config{defaultLoggerConfig})
	if err != nil {
		log.Fatal("Failed to initialize logger with default configuration")

	}
	logger := zapwriter.Logger("main")

	configFile := flag.String("config", "", "config file (yaml)")

	flag.Parse()
	expvar.NewString("GoVersion").Set(runtime.Version())
	expvar.NewString("BuildVersion").Set(BuildVersion)

	viper.SetConfigName("g2mt")
	if *configFile != "" {
		viper.AddConfigPath(*configFile)
	}
	viper.SetDefault("", config)

	viper.AddConfigPath("/etc/g2mt/")
	viper.AddConfigPath("/etc/")
	viper.AddConfigPath(".")
	err = viper.ReadInConfig()
	if err != nil {
		logger.Fatal("unable to load config file",
			zap.Error(err),
		)
	}

	err = viper.Unmarshal(&config)
	if err != nil {
		logger.Fatal("Error parsing config",
			zap.Error(err),
		)
	}

	logger.Info("Starting",
		zap.Any("config", config),
	)

	err = zapwriter.ApplyConfig(config.Logger)
	if err != nil {
		logger.Fatal("Failed to apply config",
			zap.Any("config", config.Logger),
			zap.Error(err),
		)
	}

	exitChan := make(chan struct{})
	errChan := make(chan error, 64*1024)

	for _, l := range config.Listeners {
		transports := make([]transport.Sender, 0)
		for _, t := range l.Transports {
			switch t.Type {
			case "kafka":
				for _, cfg := range t.Config {
					kafka, err := transport.NewKafkaSender(cfg, exitChan, l.TransportWorkers, l.MaxBatchSize, l.SendInterval)
					if err != nil {
						logger.Fatal("Failed to start transport",
							zap.Error(err),
						)
					}
					transports = append(transports, kafka)
					go kafka.Start()
				}
			default:
				logger.Fatal("Unsupported Transport Type",
					zap.String("type", t.Type),
				)
			}
		}

		r := make(map[string]routers.Router)
		for name, cfg := range l.Routers {
			switch cfg.Type {
			case "relay":
				r[name] = routers.NewRelayRouter(transports, cfg.Config)
			default:
				logger.Fatal("Unsupported Router Type",
					zap.String("type", cfg.Type),
				)
			}
		}

		for _, cfg := range l.Receivers {
			if cfg.Type == "graphite" {

				for _, c := range cfg.Config {
					graphite, err := receiver.NewGraphiteLineReceiver(c, r[cfg.Router], exitChan, l.MaxBatchSize, cfg.SendInterval)
					if err != nil {
						logger.Fatal("Failed to start receiver",
							zap.Error(err),
							zap.Any("cfg", cfg),
							zap.Any("routers", r),
						)
					}
					go graphite.Start()
				}
			} else {
				logger.Fatal("Unsupported Receiver Type",
					zap.String("type", cfg.Type),
				)
			}
		}
	}

	go errorPrinter(exitChan, errChan)

	http.ListenAndServe(config.Debug.Listen, nil)
}
