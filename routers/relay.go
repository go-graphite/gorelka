package routers

import (
	"github.com/go-graphite/g2mt/carbon"
	"github.com/go-graphite/g2mt/transport"

	"github.com/lomik/zapwriter"
	"go.uber.org/zap"
	"golang.org/x/sync/syncmap"
	"regexp"
	"strconv"
	"strings"

	"sync/atomic"
)

// Rule is a generic struct that will describe all possible rules
type Rule struct {
	Regex                 string
	StartsWith            string
	RewriteTo             string
	Destinations          []string
	SaveOriginalOnRewrite bool
	LastIfMatched         bool
	Blackhole             bool
	LogOnReceive          bool

	senders []transport.Sender
}

// Metrics contains all internal metrics
type Metrics struct {
	InfiniteRecursions uint64
	MetricsRouted      uint64
	MetricsRewritten   uint64
	RulesCacheMiss     uint64
	RulesCacheHit      uint64
}

// Config is a structure that contains router-specific config options
type Config struct {
	Rules            []Rule
	MaxRuleRecursion int
}

// RelayRouter describes internal router state.
type RelayRouter struct {
	Config
	matchCache syncmap.Map
	reCache    syncmap.Map

	senders []transport.Sender

	blackholeMatch ruleMatch

	logger *zap.Logger

	Metrics Metrics
}

// NewRelayRouter will create a new router
func NewRelayRouter(senders []transport.Sender, config Config) *RelayRouter {
	logger := zapwriter.Logger("relay_new")
	for i := range config.Rules {
		for _, dst := range config.Rules[i].Destinations {
			for _, sender := range senders {
				logger.Debug("trying to match",
					zap.String("dst", dst),
					zap.String("name", sender.GetName()),
				)
				if sender.GetName() == dst {
					config.Rules[i].senders = append(config.Rules[i].senders, sender)
					logger.Debug("matched",
						zap.String("sender_name", sender.GetName()),
					)
				}
			}
		}
	}

	return &RelayRouter{
		senders: senders,
		Config:  config,
		blackholeMatch: ruleMatch{
			senders:       []transport.Sender{transport.NewBlackholeSender()},
			lastIfMatched: true,
		},
		logger: zapwriter.Logger("router"),
	}
}

type ruleMatch struct {
	logOnReceive          bool
	lastIfMatched         bool
	saveOriginalOnRewrite bool
	re                    *regexp.Regexp
	rewriteTo             []string
	senders               []transport.Sender
}

func rewrite(re *regexp.Regexp, from, to string) string {
	matches := re.FindAllStringSubmatch(from, -1)

	// Make sure it matches the string
	newPath := to

	dollarMatch, _ := regexp.Compile(`\$\d+`) // Prepare our regex
	replace := dollarMatch.FindAllStringSubmatch(to, -1)

	replaceIdx := make(map[string]string)
	for mI, replacementVal := range matches[0] {
		indexVal := "$" + strconv.Itoa(mI)
		replaceIdx[indexVal] = replacementVal
	}

	for _, v := range replace {
		newPath = strings.Replace(newPath, v[0], replaceIdx[v[0]], -1)
	}

	// matched?? Set the modified path
	return newPath
}

// Reload will try to reload configuration of router. Returns true on success.
func (r *RelayRouter) Reload(senders []transport.Sender, config Config) bool {
	// TODO: Not implemented Yet
	return false
}

// Route allows to route bunch of metrics
func (r *RelayRouter) Route(payload carbon.Payload) {
	for _, metric := range payload.Metrics {
		r.routeMetric(metric, 0)
	}
}

func (r *RelayRouter) routeMetric(metric *carbon.Metric, iteration int) {
	var err error
	var re *regexp.Regexp

	// This is simple anti recursion way - e.x. when we have a loop of rewrite regexs ( foo -> bar -> foo ... )
	// We want to break that cycle at some point.
	// Please note that "save original" will still produce some extra metrics
	if iteration > r.MaxRuleRecursion {
		atomic.AddUint64(&r.Metrics.InfiniteRecursions, 1)
		r.logger.Warn("suspected loop",
			zap.String("reason", "max_allowed_recursion_depth exceeded"),
			zap.Int("iteration", iteration),
			zap.Int("max_allowed_recursion_depth", r.MaxRuleRecursion),
			zap.Any("metric", metric),
		)
		return
	}
	match := ruleMatch{
		senders: []transport.Sender{},
	}

	// Try to do a cache lookup
	// To speed up processing we will cache matches for each metric
	// TODO: Optimize memory consumption here
	m, fromCache := r.matchCache.Load(metric.Metric)
	switch fromCache {
	case true:
		atomic.AddUint64(&r.Metrics.RulesCacheHit, 1)
		match = m.(ruleMatch)
	default:
		atomic.AddUint64(&r.Metrics.RulesCacheMiss, 1)

		for _, rule := range r.Rules {
			r.logger.Debug("trying rule",
				zap.Any("rule", rule),
				zap.String("metric_name", metric.Metric),
			)
			// Idea is to construct a match object
			// That will contain all needed information on how to send metric.
			// E.x. it will have several flags
			// Plus pointers to senders, already pre-calculated by hash

			// StatsWith is an optimization for prefix matching. We usually don't need complex regexs here. Plus regex
			// in Go are very slow
			if rule.StartsWith != "" {
				if !strings.HasPrefix(metric.Metric, rule.StartsWith) {
					continue
				}
			} else {
				reRaw, ok := r.reCache.Load(rule.Regex)
				if !ok {
					re, err = regexp.Compile(rule.Regex)
					if err != nil {
						r.logger.Warn("broken regexp, skipping",
							zap.String("regexp", rule.Regex),
							zap.Error(err),
						)
						continue
					}
					r.reCache.Store(rule.Regex, re)
				} else {
					re = reRaw.(*regexp.Regexp)
				}

				if !re.Match([]byte(metric.Metric)) {
					continue
				}
			}

			// Write metric to the log if matched
			if rule.LogOnReceive {
				match.logOnReceive = true
			}

			// Special case for blackhole
			if rule.Blackhole {
				continue
			}

			// Check if we need to rewrite metric
			if rule.Regex != "" && rule.RewriteTo != "" {
				name := rewrite(re, metric.Metric, rule.RewriteTo)
				if name == metric.Metric {
					atomic.AddUint64(&r.Metrics.InfiniteRecursions, 1)
					r.logger.Warn("suspected loop",
						zap.String("reason", "Metric name haven't changed after Rewrite"),
						zap.Int("iteration", iteration),
						zap.Int("max_allowed_recursion_depth", r.MaxRuleRecursion),
						zap.Any("metric", metric),
					)
					continue
				}
				match.rewriteTo = append(match.rewriteTo, name)
				match.saveOriginalOnRewrite = rule.SaveOriginalOnRewrite
				match.re = re
				r.logger.Debug("rewrite",
					zap.String("original", metric.Metric),
					zap.String("rewrite_to", rule.RewriteTo),
					zap.String("new_name", name),
					zap.Int("iteration", iteration),
				)

				if !rule.SaveOriginalOnRewrite {
					if rule.LastIfMatched {
						break
					}
					continue
				}
			}

			r.logger.Debug("routing metric",
				zap.String("metric_name", metric.Metric),
				zap.Any("senders", rule.senders),
			)

			match.senders = append(match.senders, rule.senders...)

			// Stop applying rules if the rule is marked as "last"
			if rule.LastIfMatched {
				break
			}
		}

	}

	if len(match.rewriteTo) != 0 {
		for _, name := range match.rewriteTo {
			atomic.AddUint64(&r.Metrics.MetricsRewritten, 1)
			newMetric := &carbon.Metric{
				Metric: name,
				Points: metric.Points,
			}
			r.routeMetric(newMetric, iteration+1)
		}
		if !match.saveOriginalOnRewrite {
			return
		}
	}

	// To unify the process, replace empty match with Blackhole.
	if len(match.senders) == 0 {
		match = r.blackholeMatch
	}

	if !fromCache {
		r.matchCache.Store(metric.Metric, match)
	}
	atomic.AddUint64(&r.Metrics.MetricsRouted, 1)
	if match.logOnReceive {
		r.logger.Info("logging metric",
			zap.Any("metric", metric),
		)
	}

	// Actually send the metrics
	for _, sender := range match.senders {
		r.logger.Debug("sending metric",
			zap.String("metric", metric.Metric),
			zap.Any("sender", sender),
		)
		sender.Send(metric)
	}
}
