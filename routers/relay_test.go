package routers

import (
	"github.com/go-graphite/g2mt/carbon"
	"github.com/go-graphite/g2mt/transport"
	"github.com/lomik/zapwriter"
	"testing"
)

func TestEmptyRules(t *testing.T) {
	var defaultLoggerConfig = zapwriter.Config{
		Logger:           "",
		File:             "stdout",
		Level:            "debug",
		Encoding:         "json",
		EncodingTime:     "iso8601",
		EncodingDuration: "seconds",
	}

	_ = zapwriter.ApplyConfig([]zapwriter.Config{defaultLoggerConfig})
	cfg := Config{}
	sender := transport.NewDummySender("sender0")
	payload := carbon.Payload{Metrics: []*carbon.Metric{
		{Metric: "test", Points: []carbon.Point{{1, 1}, {2, 2}}}}}
	router := NewRelayRouter([]transport.Sender{sender}, cfg)
	router.Route(payload)

	metrics := sender.GetReceivedMetrics()
	if len(metrics) > 0 {
		t.Fatalf("Got >0 metrics: %v", len(metrics))
	}
}

func TestRecursionDetection(t *testing.T) {
	var defaultLoggerConfig = zapwriter.Config{
		Logger:           "",
		File:             "stdout",
		Level:            "debug",
		Encoding:         "json",
		EncodingTime:     "iso8601",
		EncodingDuration: "seconds",
	}

	_ = zapwriter.ApplyConfig([]zapwriter.Config{defaultLoggerConfig})
	cfg := Config{
		Rules: []Rule{
			{
				Regex:        "^(t)est",
				RewriteTo:    "$1est",
				LogOnReceive: true,
				Destinations: []string{
					"sender0",
				},
			},
			{
				Regex:        "^(new_test[0-9]+)[0-9]",
				RewriteTo:    "$1",
				LogOnReceive: false,
				Destinations: []string{
					"sender0",
				},
			},
		},
		MaxRuleRecursion: 5,
	}
	sender := transport.NewDummySender("sender0")
	payload := carbon.Payload{Metrics: []*carbon.Metric{
		{Metric: "test", Points: []carbon.Point{{1, 1}, {2, 2}}},
		{Metric: "new_test0123456789", Points: []carbon.Point{{1, 1}, {2, 2}}},
	}}
	router := NewRelayRouter([]transport.Sender{sender}, cfg)
	router.Route(payload)

	metrics := sender.GetReceivedMetrics()
	if len(metrics) > 0 {
		t.Fatalf("Got >0 metrics: %v", len(metrics))
	}

	if router.Metrics.InfiniteRecursions != uint64(len(payload.Metrics)) {
		t.Fatalf("Infinite Recursion detector bug, got %v recursions, expected %v", router.Metrics.InfiniteRecursions, len(payload.Metrics))
	}
}

func TestRewrites(t *testing.T) {
	var defaultLoggerConfig = zapwriter.Config{
		Logger:           "",
		File:             "stdout",
		Level:            "debug",
		Encoding:         "json",
		EncodingTime:     "iso8601",
		EncodingDuration: "seconds",
	}

	_ = zapwriter.ApplyConfig([]zapwriter.Config{defaultLoggerConfig})
	cfg := Config{
		Rules: []Rule{
			{
				Regex:                 "^(te)st(.*)",
				RewriteTo:             "$1dt$2",
				LogOnReceive:          true,
				SaveOriginalOnRewrite: true,
				LastIfMatched:         true,
				Destinations: []string{
					"sender0",
				},
			},
			{
				Regex:                 "^(new_test)",
				RewriteTo:             "foo",
				LogOnReceive:          false,
				SaveOriginalOnRewrite: false,
				Destinations: []string{
					"sender0",
				},
			},
			{
				StartsWith:   "",
				LogOnReceive: false,
				Destinations: []string{
					"sender0",
				},
			},
		},
		MaxRuleRecursion: 5,
	}
	sender := transport.NewDummySender("sender0")
	payload := carbon.Payload{Metrics: []*carbon.Metric{
		{Metric: "test", Points: []carbon.Point{{1, 1}, {2, 2}}},
		{Metric: "new_test", Points: []carbon.Point{{3, 3}, {4, 4}}},
	}}
	router := NewRelayRouter([]transport.Sender{sender}, cfg)
	router.Route(payload)

	metrics := sender.GetReceivedMetrics()
	if len(metrics) != len(payload.Metrics)+1 {
		t.Fatalf("Got %v metrics, expected %v; metrics: %v", len(metrics), len(payload.Metrics)+1, metrics)
	}
}

func TestRewriteWithoutGroups(t *testing.T) {
	var defaultLoggerConfig = zapwriter.Config{
		Logger:           "",
		File:             "stdout",
		Level:            "debug",
		Encoding:         "json",
		EncodingTime:     "iso8601",
		EncodingDuration: "seconds",
	}

	_ = zapwriter.ApplyConfig([]zapwriter.Config{defaultLoggerConfig})
	cfg := Config{
		Rules: []Rule{
			{
				Regex:                 "^(t)(e)(s)(t)",
				RewriteTo:             "$1",
				LogOnReceive:          true,
				SaveOriginalOnRewrite: true,
				Destinations: []string{
					"sender0",
				},
			},
		},
		MaxRuleRecursion: 5,
	}
	sender := transport.NewDummySender("sender0")
	payload := carbon.Payload{Metrics: []*carbon.Metric{
		{Metric: "test", Points: []carbon.Point{{1, 1}, {2, 2}}},
	}}
	router := NewRelayRouter([]transport.Sender{sender}, cfg)
	router.Route(payload)

	metrics := sender.GetReceivedMetrics()
	if len(metrics) != 1 {
		t.Fatalf("Got %v metrics, expected %v; metrics: %v", len(metrics), 1, metrics)
	}
}

func TestBadRegexps(t *testing.T) {
	var defaultLoggerConfig = zapwriter.Config{
		Logger:           "",
		File:             "stdout",
		Level:            "debug",
		Encoding:         "json",
		EncodingTime:     "iso8601",
		EncodingDuration: "seconds",
	}

	_ = zapwriter.ApplyConfig([]zapwriter.Config{defaultLoggerConfig})
	cfg := Config{
		Rules: []Rule{
			{
				Regex:                 "(.?*",
				RewriteTo:             "$1",
				LogOnReceive:          true,
				SaveOriginalOnRewrite: true,
				Destinations: []string{
					"sender0",
				},
			},
			{
				Regex:                 "^(.*)",
				RewriteTo:             "(.",
				LogOnReceive:          false,
				SaveOriginalOnRewrite: false,
				Destinations: []string{
					"sender0",
				},
			},
		},
		MaxRuleRecursion: 5,
	}
	sender := transport.NewDummySender("sender0")
	payload := carbon.Payload{Metrics: []*carbon.Metric{
		{Metric: "test", Points: []carbon.Point{{1, 1}, {2, 2}}},
		{Metric: "new_test", Points: []carbon.Point{{1, 1}, {2, 2}}},
	}}
	router := NewRelayRouter([]transport.Sender{sender}, cfg)
	router.Route(payload)

	metrics := sender.GetReceivedMetrics()
	if len(metrics) != 0 {
		t.Fatalf("Got %v metrics, expected %v; metrics: %v", len(metrics), 0, metrics)
	}
}

func TestRules(t *testing.T) {
	var defaultLoggerConfig = zapwriter.Config{
		Logger:           "",
		File:             "stdout",
		Level:            "debug",
		Encoding:         "json",
		EncodingTime:     "iso8601",
		EncodingDuration: "seconds",
	}

	_ = zapwriter.ApplyConfig([]zapwriter.Config{defaultLoggerConfig})
	cfg := Config{
		Rules: []Rule{
			{
				StartsWith:    "prefix",
				LastIfMatched: true,
				Blackhole:     true,
			},
			{
				Regex:         "^DONT_SEND_ME",
				LastIfMatched: true,
				Blackhole:     true,
			},
			{
				Regex:         "^test999",
				LastIfMatched: true,
				LogOnReceive:  false,
				Destinations: []string{
					"sender3",
				},
			},
			{
				Regex:         "^test[23]",
				LastIfMatched: false,
				LogOnReceive:  true,
				Destinations: []string{
					"sender3",
				},
			},
			{
				Regex:         "^t",
				LastIfMatched: true,
				Destinations: []string{
					"sender1",
					"sender4",
				},
			},
		},
		MaxRuleRecursion: 5,
	}

	sender1 := transport.NewDummySender("sender1")
	sender2 := transport.NewDummySender("sender2")
	sender3 := transport.NewDummySender("sender3")
	sender4 := transport.NewDummySender("sender4")
	payloadPrefixMatch := carbon.Payload{Metrics: []*carbon.Metric{
		{
			Metric: "prefix_metric", Points: []carbon.Point{{0, 0}},
		},
	}}
	payloadBlackhole := carbon.Payload{Metrics: []*carbon.Metric{
		{
			Metric: "DONT_SEND_ME_AAAA", Points: []carbon.Point{{0, 0}},
		},
	}}
	payloadSender3Only := carbon.Payload{Metrics: []*carbon.Metric{
		{
			Metric: "test999", Points: []carbon.Point{{0, 0}},
		},
	}}
	payload := carbon.Payload{Metrics: []*carbon.Metric{
		{
			Metric: "test", Points: []carbon.Point{{1, 1}, {2, 2}},
		},
		{
			Metric: "test2", Points: []carbon.Point{{3, 3}, {4, 4}},
		},
		{
			Metric: "test3", Points: []carbon.Point{{5, 5}, {6, 6}},
		},
		{
			Metric: "test4", Points: []carbon.Point{{7, 7}, {8, 8}},
		},
		{
			Metric: "test5", Points: []carbon.Point{{9, 9}, {10, 10}},
		},
		{
			Metric: "test6", Points: []carbon.Point{{11, 11}, {12, 12}},
		},
	}}
	router := NewRelayRouter([]transport.Sender{sender1, sender2, sender3, sender4}, cfg)
	router.Route(payloadPrefixMatch)
	router.Route(payloadBlackhole)
	router.Route(payloadSender3Only)
	router.Route(payload)

	metrics := sender1.GetReceivedMetrics()
	metrics = append(metrics, sender2.GetReceivedMetrics()...)

	metrics3 := sender3.GetReceivedMetrics()
	metrics4 := sender4.GetReceivedMetrics()
	if len(metrics3) != 3 {
		t.Errorf("Sender: Got = %v (expected %v), metrics: %v", len(metrics3), 3, metrics3)
	}

	if len(metrics) != len(payload.Metrics) || len(metrics4) != len(payload.Metrics) {
		t.Errorf("Sender: Got = %v and %v (expected %v and %v), metrics: %v", len(metrics), len(metrics4), len(payload.Metrics), len(payload.Metrics), metrics)
	}
}

func TestRulesCache(t *testing.T) {
	var defaultLoggerConfig = zapwriter.Config{
		Logger:           "",
		File:             "stdout",
		Level:            "debug",
		Encoding:         "json",
		EncodingTime:     "iso8601",
		EncodingDuration: "seconds",
	}

	_ = zapwriter.ApplyConfig([]zapwriter.Config{defaultLoggerConfig})
	cfg := Config{
		Rules: []Rule{
			{
				Regex:         "^t",
				LastIfMatched: true,
				Destinations: []string{
					"sender1",
				},
			},
		},
		MaxRuleRecursion: 5,
	}

	sender1 := transport.NewDummySender("sender1")
	payload := carbon.Payload{Metrics: []*carbon.Metric{
		{
			Metric: "test", Points: []carbon.Point{{1, 1}, {2, 2}},
		},
	}}
	router := NewRelayRouter([]transport.Sender{sender1}, cfg)
	router.Route(payload)
	router.Route(payload)

	metrics := sender1.GetReceivedMetrics()
	if len(metrics) != 2 {
		t.Errorf("Sender: Got = %v (expected %v), metrics: %v", len(metrics), 2, metrics)
	}

	if router.Metrics.RulesCacheHit != 1 {
		t.Errorf("Rules cache doesn't work, hits: %v, misses: %v, expected_hits: %v", router.Metrics.RulesCacheHit, router.Metrics.RulesCacheMiss, 1)
	}

	if router.Metrics.RulesCacheMiss != 1 {
		t.Fatalf("Rules cache doesn't work, hits: %v, misses: %v, expected_misses: %v", router.Metrics.RulesCacheHit, router.Metrics.RulesCacheMiss, 1)
	}
}
