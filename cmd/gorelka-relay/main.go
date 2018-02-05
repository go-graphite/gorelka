package main

import (
	"expvar"
	"flag"
	"fmt"
	"log"
	"runtime"

	"net/http"
	_ "net/http/pprof"

	"github.com/go-graphite/gorelka/metrics"
	"github.com/go-graphite/gorelka/receiver"
	"github.com/go-graphite/gorelka/transport"
	"github.com/go-graphite/gorelka/transport/common"
	"github.com/lomik/zapwriter"
	"github.com/spf13/viper"
	"go.uber.org/zap"

	"time"

	"github.com/go-graphite/gorelka/routers"
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
	Listen               string
	MutexProfileFraction int
}

type receiverConfig struct {
	Type          string
	Router        string
	SendInterval  time.Duration
	AcceptTimeout time.Duration
	Config        []receiver.Config
}

type destinationsConfig struct {
	Config common.ConfigForFile
}

type routerConfig struct {
	Type   string
	Config routers.Config
}

type relayConf struct {
	Destinations     map[string]destinationsConfig
	Listeners        map[string]receiverConfig
	Routers          map[string]routerConfig
	MaxBatchSize     int
	TransportWorkers int
	SendInterval     time.Duration
	QueueSize        int
}

var config = struct {
	Relay   relayConf
	Logger  []zapwriter.Config `json:"Logger"`
	Debug   debugConfig
	Metrics metrics.Config `json:"metrics"`

	// Additional tags to be attached graphite metrics
	Tags map[string]string
}{
	/*
		Configs: []relayConf{{
			MaxBatchSize:          500000,
			SendInterval:          200 * time.Millisecond,
			TransportWorkers:      4,
			TransportChanCapacity: 64 * 1024,
			Configs: map[string]receiverConfig{
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
			Destinations: []destinationsConfig{
				{
					Type:   "kafka",
					Router: "default_relay",
					Config: []common.Config{
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
	*/
	Debug: debugConfig{
		Listen: ":6060",
	},
	Logger: []zapwriter.Config{defaultLoggerConfig},
}

// BuildVersion contains version and/or commit of current build. Defaults to "Development"
var BuildVersion = "(development)"

var errNoListenersFmt = "no %v specified"
var errNoTimoutSetFmt = "no timeout set for %v"

func validateConfig() {
	logger := zapwriter.Logger("config_validator")

	fatalErrors := make([]string, 0)

	for k, cfg := range config.Relay.Listeners {
		if cfg.SendInterval == 0 {
			fatalErrors = append(fatalErrors, fmt.Sprintf(errNoTimoutSetFmt, fmt.Sprintf("relays.listeners.%v.%v", k, "SendInterval")))
		}
		if cfg.AcceptTimeout == 0 {
			fatalErrors = append(fatalErrors, fmt.Sprintf(errNoTimoutSetFmt, fmt.Sprintf("relays.listeners.%v.%v", k, "AcceptTimeout")))
		}
	}

	for k, cfg := range config.Relay.Destinations {
		changesNeeded := false
		if cfg.Config.SendInterval == 0 {
			cfg.Config.SendInterval = 100 * time.Millisecond
			changesNeeded = true
		}
		if cfg.Config.FlushFrequency == 0 {
			cfg.Config.FlushFrequency = 200 * time.Millisecond
			changesNeeded = true
		}
		if cfg.Config.ChannelBufferSize == 0 {
			cfg.Config.ChannelBufferSize = 1000000
			changesNeeded = true
		}
		if changesNeeded {
			config.Relay.Destinations[k] = cfg
		}
	}

	if config.Relay.MaxBatchSize == 0 {
		config.Relay.MaxBatchSize = 1000000
	}

	if config.Relay.QueueSize == 0 {
		config.Relay.QueueSize = 1000000
	}

	if config.Relay.Listeners == nil {
		fatalErrors = append(fatalErrors, fmt.Sprintf(errNoListenersFmt, "listeners"))
	}

	if config.Relay.Destinations == nil {
		fatalErrors = append(fatalErrors, fmt.Sprintf(errNoListenersFmt, "destinations"))
	}

	if config.Relay.Routers == nil {
		fatalErrors = append(fatalErrors, fmt.Sprintf(errNoListenersFmt, "routers"))
	}

	if len(fatalErrors) != 0 {
		logger.Fatal("config is invalid",
			zap.Strings("fatal_errors", fatalErrors),
		)
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

	viper.SetConfigName("g2mt")
	if *configFile != "" {
		viper.SetConfigFile(*configFile)
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
		logger.Fatal("error parsing config",
			zap.Error(err),
		)
	}

	err = zapwriter.ApplyConfig(config.Logger)
	if err != nil {
		logger.Fatal("failed to apply config",
			zap.Any("config", config.Logger),
			zap.Error(err),
		)
	}

	validateConfig()

	logger.Info("starting",
		zap.String("config_file_used", viper.ConfigFileUsed()),
		zap.Any("config", config),
	)

	if config.Debug.MutexProfileFraction > 0 {
		runtime.SetMutexProfileFraction(config.Debug.MutexProfileFraction)
	}

	exitChan := make(chan struct{})

	metrics.NewCollector(&config.Metrics)

	expvar.NewString("GoVersion").Set(runtime.Version())
	expvar.NewString("BuildVersion").Set(BuildVersion)
	expvar.Publish("config", expvar.Func(func() interface{} { return config }))

	transports := make([]transport.Sender, 0)
	for k, t := range config.Relay.Destinations {
		c := common.Config{}
		err = c.FromParsed(t.Config)
		c.Name = k
		if err != nil {
			logger.Fatal("failed to parse config",
				zap.Error(err),
			)
		}

		var senderInit transport.SenderInitFunc
		switch c.Type {
		case common.Kafka:
			senderInit = transport.NewKafkaSender
		case common.TCP, common.UDP:
			senderInit = transport.NewNetSender
		default:
			logger.Fatal("unsupported transport type",
				zap.String("type", c.Type.String()),
			)
		}

		sender, err := senderInit(c, exitChan, config.Relay.TransportWorkers, config.Relay.MaxBatchSize)
		if err != nil {
			logger.Fatal("failed to start transport",
				zap.Error(err),
			)
		}

		transports = append(transports, sender)
		go sender.Start()
	}

	r := make(map[string]routers.Router)
	for name, cfg := range config.Relay.Routers {
		switch cfg.Type {
		case "relay":
			r[name] = routers.NewRelayRouter(transports, cfg.Config)
		default:
			logger.Fatal("unsupported router type",
				zap.String("type", cfg.Type),
			)
		}
	}

	for _, cfg := range config.Relay.Listeners {
		if cfg.Type == "graphite" {
			for _, c := range cfg.Config {
				graphite, err := receiver.NewGraphiteLineReceiver(c, r[cfg.Router], exitChan, config.Relay.MaxBatchSize, config.Relay.QueueSize, cfg.SendInterval, cfg.AcceptTimeout)
				if err != nil {
					logger.Fatal("failed to start receiver",
						zap.Error(err),
						zap.Any("cfg", cfg),
						zap.Any("routers", r),
					)
				}
				go graphite.Start()
			}
		} else {
			logger.Fatal("unsupported receiver type",
				zap.String("type", cfg.Type),
			)
		}
	}

	http.ListenAndServe(config.Debug.Listen, nil)
}
