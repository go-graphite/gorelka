package receiver

import (
	"fmt"
	"io/ioutil"
	"os"
	"runtime"
	"testing"

	"github.com/pkg/errors"

	"net"
	"time"

	"github.com/go-graphite/gorelka/carbon"
	"github.com/go-graphite/gorelka/routers"
	"github.com/lomik/zapwriter"
)

// Helper functions and structs
type testCase struct {
	testName       string
	data           []byte
	expectedError  error
	expectedAnswer *carbon.Metric
}

func generateName(size int) []byte {
	r := make([]byte, 0, size)

	for i := 0; i < size; i++ {
		r = append(r, 'a')
	}

	r = append(r, '\n')

	return r
}

func errorsEqual(first, second error) bool {
	if first == nil {
		if second == nil {
			return true
		}
		return false
	}
	if second == nil {
		return false
	}

	if first.Error() == second.Error() {
		return true
	}
	return false
}

func metricsEquals(first, second *carbon.Metric) bool {
	if first == nil {
		if second == nil {
			return true
		}
		return false
	}
	if second == nil {
		return false
	}

	if first.Metric != second.Metric {
		return false
	}
	if len(first.Points) != len(second.Points) {
		return false
	}

	for i := range first.Points {
		if first.Points[i].Timestamp != second.Points[i].Timestamp || first.Points[i].Value != second.Points[i].Value {
			return false
		}
	}

	return true
}

func TestGraphiteReceiverStartStop(t *testing.T) {
	// Initialize r
	var defaultLoggerConfig = zapwriter.Config{
		Logger:           "",
		File:             "stderr",
		Level:            "debug",
		Encoding:         "json",
		EncodingTime:     "iso8601",
		EncodingDuration: "seconds",
	}

	_ = zapwriter.ApplyConfig([]zapwriter.Config{defaultLoggerConfig})

	startGoroutineNum := runtime.NumGoroutine()
	dir, err := ioutil.TempDir("", "example")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	exitChan := make(chan struct{})
	config := Config{
		Listen:   dir + "/test.unix",
		Protocol: "unix",
		Workers:  1,
		Strict:   true,
	}
	router := routers.NewDummyRouter()
	maxBatchSize := 100
	r, err := NewGraphiteLineReceiver(config, router, exitChan, maxBatchSize, config.Workers, 100*time.Millisecond, time.Second)
	if err != nil {
		t.Fatalf("Failed to initialize r: %v", err)
	}
	goroutineNum := runtime.NumGoroutine()
	if startGoroutineNum != startGoroutineNum {
		t.Errorf("Unexpected number of goroutines %v, should be %v", goroutineNum, startGoroutineNum)
	}
	go r.Start()
	// End of initialization

	close(exitChan)

	time.Sleep(2 * time.Second)
	goroutineNum = runtime.NumGoroutine()
	if startGoroutineNum != startGoroutineNum {
		t.Errorf("Unexpected number of goroutines %v, should be %v", goroutineNum, startGoroutineNum)
	}
}

type graphiteParser struct {
	config   *Config
	receiver *GraphiteLineReceiver
	router   *routers.DummyRouter
}

func initGraphiteParser(dir string, strict bool, exitChan chan struct{}) (*graphiteParser, error) {
	config := Config{
		Listen:   fmt.Sprintf("%s/test_strict=%v.unix", dir, strict),
		Protocol: "unix",
		Workers:  100,
		Strict:   strict,
	}
	router := routers.NewDummyRouter()
	maxBatchSize := 100
	r, err := NewGraphiteLineReceiver(config, router, exitChan, maxBatchSize, config.Workers, 100*time.Millisecond, time.Second)

	return &graphiteParser{
		config:   &config,
		receiver: r,
		router:   router,
	}, err
}

func TestGraphiteParserStrict(t *testing.T) {
	// Initialize receiver
	var defaultLoggerConfig = zapwriter.Config{
		Logger:           "",
		File:             "stderr",
		Level:            "debug",
		Encoding:         "json",
		EncodingTime:     "iso8601",
		EncodingDuration: "seconds",
	}

	_ = zapwriter.ApplyConfig([]zapwriter.Config{defaultLoggerConfig})

	dir, err := ioutil.TempDir("", "example")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	exitChan := make(chan struct{})
	parser, err := initGraphiteParser(dir, true, exitChan)
	if err != nil {
		t.Fatalf("Failed to initialize receiver: %v", err)
	}
	r := parser.receiver

	testCases := []testCase{
		{
			testName:      "Simple valid metric",
			data:          stringToMetric("foo 10 100"),
			expectedError: nil,
			expectedAnswer: &carbon.Metric{
				Metric: "foo",
				Points: []carbon.Point{
					{
						Value:     10,
						Timestamp: 100,
					},
				},
			},
		},

		{
			testName:      "Scientific valid metric #1",
			data:          stringToMetric("foo 1e1 1e2"),
			expectedError: nil,
			expectedAnswer: &carbon.Metric{
				Metric: "foo",
				Points: []carbon.Point{
					{
						Value:     10,
						Timestamp: 100,
					},
				},
			},
		},

		{
			testName:      "Scientific valid metric #2",
			data:          stringToMetric("foo 1E1 1E2"),
			expectedError: nil,
			expectedAnswer: &carbon.Metric{
				Metric: "foo",
				Points: []carbon.Point{
					{
						Value:     10,
						Timestamp: 100,
					},
				},
			},
		},

		{
			testName:      "Float valid metric",
			data:          stringToMetric("foo 1.0 10.0"),
			expectedError: nil,
			expectedAnswer: &carbon.Metric{
				Metric: "foo",
				Points: []carbon.Point{
					{
						Value:     1,
						Timestamp: 10,
					},
				},
			},
		},

		{
			testName:       "Line is too long",
			data:           generateName(GraphiteLineReceiverMaxLineSize + 10),
			expectedError:  errors.Wrap(errFmtParseError, "line is too large or malformed"),
			expectedAnswer: nil,
		},

		{
			testName:       "Invalid metric: only name",
			data:           stringToMetric("foo"),
			expectedError:  errors.Wrap(errFmtParseError, "line is too large or malformed"),
			expectedAnswer: nil,
		},

		{
			testName:       "Invalid metric: value is empty",
			data:           stringToMetric("foo "),
			expectedError:  errors.Wrap(errFmtParseError, "no value field"),
			expectedAnswer: nil,
		},

		{
			testName:       "Invalid metric: name + value",
			data:           stringToMetric("foo 1"),
			expectedError:  errors.Wrap(errFmtParseError, "no value field"),
			expectedAnswer: nil,
		},

		{
			testName:       "Invalid metric: name + value",
			data:           stringToMetric("foo 1 "),
			expectedError:  errors.Wrap(errFmtParseError, "invalid timestamp"),
			expectedAnswer: nil,
		},

		{
			testName:       "Invalid metric: invalid value",
			data:           stringToMetric("foo bar 1"),
			expectedError:  errors.Wrap(errFmtParseError, "invalid value"),
			expectedAnswer: nil,
		},

		{
			testName:       "Invalid metric: invalid timestamp",
			data:           stringToMetric("foo 1 baz"),
			expectedError:  errors.Wrap(errFmtParseError, "invalid timestamp"),
			expectedAnswer: nil,
		},

		{
			testName:       "Invalid metric: invalid timestamp",
			data:           stringToMetric("foo 1 1 baz"),
			expectedError:  errors.Wrap(errFmtParseError, "invalid timestamp"),
			expectedAnswer: nil,
		},

		{
			testName: "Invalid metric: multiple spaces before value",
			data:     stringToMetric("foo  1 1"),

			expectedError:  errors.Wrap(errFmtParseError, "no value field"),
			expectedAnswer: nil,
		},

		{
			testName:       "Invalid metric: multiple spaces before timestamp",
			data:           stringToMetric("foo 1  1"),
			expectedError:  errors.Wrap(errFmtParseError, "invalid timestamp"),
			expectedAnswer: nil,
		},
	}

	for i := range testCases {
		t.Run(testCases[i].testName, func(t *testing.T) {
			r, err := r.Parse(testCases[i].data)
			if !errorsEqual(err, testCases[i].expectedError) {
				t.Errorf("Test %v failed: Unexpected error value '%v' (expected '%v')", testCases[i].testName, err, testCases[i].expectedError)
			}
			if !metricsEquals(r, testCases[i].expectedAnswer) {
				t.Errorf("Test %v failed: Unexpected result '%+v' (expected '%+v')", testCases[i].testName, r, testCases[i].expectedAnswer)
			}
		})
	}
}

func TestGraphiteParserRelaxed(t *testing.T) {
	// Initialize receiver
	var defaultLoggerConfig = zapwriter.Config{
		Logger:           "",
		File:             "stderr",
		Level:            "debug",
		Encoding:         "json",
		EncodingTime:     "iso8601",
		EncodingDuration: "seconds",
	}

	_ = zapwriter.ApplyConfig([]zapwriter.Config{defaultLoggerConfig})

	dir, err := ioutil.TempDir("", "example")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	exitChan := make(chan struct{})
	parser, err := initGraphiteParser(dir, false, exitChan)
	if err != nil {
		t.Fatalf("Failed to initialize receiver: %v", err)
	}
	r := parser.receiver

	testCases := []testCase{
		{
			testName:      "Simple valid metric",
			data:          stringToMetric("foo 10 100"),
			expectedError: nil,
			expectedAnswer: &carbon.Metric{
				Metric: "foo",
				Points: []carbon.Point{
					{
						Value:     10,
						Timestamp: 100,
					},
				},
			},
		},

		{
			testName:      "Scientific valid metric #1",
			data:          stringToMetric("foo 1e1 1e2"),
			expectedError: nil,
			expectedAnswer: &carbon.Metric{
				Metric: "foo",
				Points: []carbon.Point{
					{
						Value:     10,
						Timestamp: 100,
					},
				},
			},
		},

		{
			testName:      "Scientific valid metric #2",
			data:          stringToMetric("foo 1E1 1E2"),
			expectedError: nil,
			expectedAnswer: &carbon.Metric{
				Metric: "foo",
				Points: []carbon.Point{
					{
						Value:     10,
						Timestamp: 100,
					},
				},
			},
		},

		{
			testName:      "Float valid metric",
			data:          stringToMetric("foo 1.0 10.0"),
			expectedError: nil,
			expectedAnswer: &carbon.Metric{
				Metric: "foo",
				Points: []carbon.Point{
					{
						Value:     1,
						Timestamp: 10,
					},
				},
			},
		},

		{
			testName:       "Line is too long",
			data:           generateName(GraphiteLineReceiverMaxLineSize + 10),
			expectedError:  errors.Wrap(errFmtParseError, "line is too large or malformed"),
			expectedAnswer: nil,
		},

		{
			testName:       "Invalid metric: only name",
			data:           stringToMetric("foo"),
			expectedError:  errors.Wrap(errFmtParseError, "line is too large or malformed"),
			expectedAnswer: nil,
		},

		{
			testName:       "Invalid metric: value is empty",
			data:           stringToMetric("foo "),
			expectedError:  errors.Wrap(errFmtParseError, "no value field"),
			expectedAnswer: nil,
		},

		{
			testName:       "Invalid metric: name + value",
			data:           stringToMetric("foo 1"),
			expectedError:  errors.Wrap(errFmtParseError, "no value field"),
			expectedAnswer: nil,
		},

		{
			testName:       "Invalid metric: name + value",
			data:           stringToMetric("foo 1 "),
			expectedError:  errors.Wrap(errFmtParseError, "invalid timestamp"),
			expectedAnswer: nil,
		},

		{
			testName:       "Invalid metric: invalid value",
			data:           stringToMetric("foo bar 1"),
			expectedError:  errors.Wrap(errFmtParseError, "invalid value"),
			expectedAnswer: nil,
		},

		{
			testName:       "Invalid metric: invalid timestamp",
			data:           stringToMetric("foo 1 baz"),
			expectedError:  errors.Wrap(errFmtParseError, "invalid timestamp"),
			expectedAnswer: nil,
		},

		{
			testName:       "Invalid metric: invalid timestamp",
			data:           stringToMetric("foo 1 1 baz"),
			expectedError:  errors.Wrap(errFmtParseError, "invalid timestamp"),
			expectedAnswer: nil,
		},

		{
			testName:      "Valid metric, multiple spaces before value",
			data:          stringToMetric("foo  10 100"),
			expectedError: nil,
			expectedAnswer: &carbon.Metric{
				Metric: "foo",
				Points: []carbon.Point{
					{
						Value:     10,
						Timestamp: 100,
					},
				},
			},
		},

		{
			testName:      "Valid metric, even more spaces before value",
			data:          stringToMetric("foo      10 100"),
			expectedError: nil,
			expectedAnswer: &carbon.Metric{
				Metric: "foo",
				Points: []carbon.Point{
					{
						Value:     10,
						Timestamp: 100,
					},
				},
			},
		},

		{
			testName:      "Valid metric, win-style end of line",
			data:          stringToMetricWinStyle("foo 10 100"),
			expectedError: nil,
			expectedAnswer: &carbon.Metric{
				Metric: "foo",
				Points: []carbon.Point{
					{
						Value:     10,
						Timestamp: 100,
					},
				},
			},
		},

		{
			testName:      "Valid metric, multiple spaces before timestamp",
			data:          stringToMetric("foo 10  100"),
			expectedError: nil,
			expectedAnswer: &carbon.Metric{
				Metric: "foo",
				Points: []carbon.Point{
					{
						Value:     10,
						Timestamp: 100,
					},
				},
			},
		},

		{
			testName:      "Valid metric, multiple spaces everywhere",
			data:          stringToMetric("foo    10    100"),
			expectedError: nil,
			expectedAnswer: &carbon.Metric{
				Metric: "foo",
				Points: []carbon.Point{
					{
						Value:     10,
						Timestamp: 100,
					},
				},
			},
		},
	}

	for i := range testCases {
		t.Run(testCases[i].testName, func(t *testing.T) {
			r, err := r.parseRelaxed(testCases[i].data)
			if !errorsEqual(err, testCases[i].expectedError) {
				t.Errorf("Test %v failed: Unexpected error value '%v' (expected '%v')", testCases[i].testName, err, testCases[i].expectedError)
			}
			if !metricsEquals(r, testCases[i].expectedAnswer) {
				t.Errorf("Test %v failed: Unexpected result '%+v' (expected '%+v')", testCases[i].testName, r, testCases[i].expectedAnswer)
			}
		})
	}
}

func stringToMetric(metric string) []byte {
	b := []byte(metric)
	b = append(b, '\n')
	return b
}

func stringToMetricWinStyle(metric string) []byte {
	b := []byte(metric)
	b = append(b, '\r')
	b = append(b, '\n')
	return b
}

func TestGraphiteFullPipeline(t *testing.T) {
	// Initialize r
	var defaultLoggerConfig = zapwriter.Config{
		Logger:           "",
		File:             "stderr",
		Level:            "debug",
		Encoding:         "json",
		EncodingTime:     "iso8601",
		EncodingDuration: "seconds",
	}

	_ = zapwriter.ApplyConfig([]zapwriter.Config{defaultLoggerConfig})

	startGoroutineNum := runtime.NumGoroutine()
	dir, err := ioutil.TempDir("", "example")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	exitChan := make(chan struct{})
	config := Config{
		Listen:   dir + "/test.unix",
		Protocol: "unix",
		Workers:  1,
		Strict:   true,
	}
	router := routers.NewDummyRouter()
	maxBatchSize := 10
	r, err := NewGraphiteLineReceiver(config, router, exitChan, maxBatchSize, config.Workers, 10*time.Millisecond, time.Second)
	if err != nil {
		t.Fatalf("Failed to initialize r: %v", err)
	}
	goroutineNum := runtime.NumGoroutine()
	if startGoroutineNum != startGoroutineNum {
		t.Errorf("Unexpected number of goroutines %v, should be %v", goroutineNum, startGoroutineNum)
	}
	go r.Start()
	// End of initialization
	sender, err := net.Dial(config.Protocol, config.Listen)
	if err != nil {
		t.Fatalf("Failed to establish connection: %v", err)
	}

	testCases := []testCase{
		{
			testName:      "Simple valid metric",
			data:          stringToMetric("foo 10 100"),
			expectedError: nil,
			expectedAnswer: &carbon.Metric{
				Metric: "foo",
				Points: []carbon.Point{
					{
						Value:     10,
						Timestamp: 100,
					},
				},
			},
		},
		{
			testName:       "Invalid metric",
			data:           stringToMetric("foo"),
			expectedError:  nil,
			expectedAnswer: nil,
		},
	}

	haveErrors := false
	for i := range testCases {
		sender.SetWriteDeadline(time.Now().Add(30 * time.Millisecond))
		_, err := sender.Write(testCases[i].data)
		if !errorsEqual(err, testCases[i].expectedError) {
			haveErrors = true
			t.Errorf("Test %v failed: Unexpected error value '%v' (expected '%v')", testCases[i].testName, err, testCases[i].expectedError)
		}
	}
	sender.Close()
	time.Sleep(50 * time.Millisecond)

	if !haveErrors {
		points := router.GetData()
		if points == nil {
			t.Errorf("Result it nil, exepceted %+v", testCases)

		} else {
			for i := range testCases {
				if testCases[i].expectedAnswer != nil {
					if metric, ok := points[testCases[i].expectedAnswer.Metric]; ok {
						if !metricsEquals(metric, testCases[i].expectedAnswer) {
							t.Errorf("Test %v failed: Unexpected result '%+v' (expected '%+v')", testCases[i].testName, metric, testCases[i].expectedAnswer)
						}
					} else {
						t.Errorf("Test %v failed: failed to get metric, result %+v", testCases[i].testName, points)
					}
				}
			}
		}
	}
	// Send a lot of data without checking

	var data []byte
	metrics := 100
	metric := stringToMetric("foo.bar 100 100")
	for i := 0; i < metrics; i++ {
		data = append(data, metric...)
	}

	sender, err = net.Dial(config.Protocol, config.Listen)
	if err != nil {
		t.Fatalf("Failed to establish connection: %v", err)
	}

	sender.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	_, err = sender.Write(data)
	if err != nil {
		t.Errorf("Failed to send data: %v", err)
	}

	time.Sleep(150 * time.Millisecond)
	points := router.GetData()
	if metric, ok := points["foo.bar"]; ok {
		if len(metric.Points) != metrics {
			t.Errorf("Test %v failed: Unexpected amount of metrics '%v' (expected '%v')", "A lot of metrics", len(metric.Points), metrics)
		}
	} else {
		t.Errorf("Test %v failed: no metrics received", "A lot of metrics")
	}

	sender.Close()

	// Test single metric
	sender, err = net.Dial(config.Protocol, config.Listen)
	if err != nil {
		t.Fatalf("Failed to establish connection: %v", err)
	}

	sender.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))

	_, err = sender.Write(metric)
	if err != nil {
		t.Errorf("Failed to send data: %v", err)
	}

	sender.Close()
	time.Sleep(10 * time.Millisecond)
	close(exitChan)

	points = router.GetData()
	if metric, ok := points["foo.bar"]; ok {
		if len(metric.Points) != 1 {
			t.Errorf("Test %v failed: Unexpected amount of metrics '%v' (expected '%v')", "A lot of metrics", 1, metrics)
		}
	} else {
		t.Errorf("Test %v failed: no metrics received", "Single metric")
	}

	time.Sleep(100 * time.Millisecond)
	goroutineNum = runtime.NumGoroutine()
	if startGoroutineNum != startGoroutineNum {
		t.Errorf("Unexpected number of goroutines %v, should be %v", goroutineNum, startGoroutineNum)
	}
}

func TestGraphiteFullPipelineSendMetrics(t *testing.T) {
	// Initialize r
	var defaultLoggerConfig = zapwriter.Config{
		Logger:           "",
		File:             "stderr",
		Level:            "info",
		Encoding:         "json",
		EncodingTime:     "iso8601",
		EncodingDuration: "seconds",
	}

	_ = zapwriter.ApplyConfig([]zapwriter.Config{defaultLoggerConfig})

	dir, err := ioutil.TempDir("", "example")
	if err != nil {
		t.Fatal(err)
	}

	defer os.RemoveAll(dir)

	tests := []bool{true, false}
	for _, v := range tests {
		t.Run(fmt.Sprintf("strict=%v", v), func(t *testing.T) {
			exitChan := make(chan struct{})
			startGoroutineNum := runtime.NumGoroutine()
			parser, err := initGraphiteParser(dir, v, exitChan)
			if err != nil {
				t.Fatalf("Failed to initialize r: %v", err)
			}

			r := parser.receiver
			router := parser.router

			go r.Start()
			// End of initialization
			line := stringToMetric("foo.bar 10 100")

			sender, err := net.Dial(parser.config.Protocol, parser.config.Listen)
			if err != nil {
				t.Fatalf("Failed to establish connection: %v", err)
			}

			sender.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
			var lastRcvDeadline time.Time
			sendTimeout := 300 * time.Millisecond
			for i := 0; i < 300000; i++ {
				now := time.Now()
				if now.Sub(lastRcvDeadline) > (sendTimeout >> 2) {
					err = sender.SetDeadline(now.Add(sendTimeout))
					if err != nil {
						t.Fatalf("Failed to send data: %v", err)
					}
					lastRcvDeadline = now
				}
				_, err = sender.Write(line)
				if err != nil {
					t.Fatalf("Failed to send data: %v", err)
				}
			}

			sender.Close()
			time.Sleep(10 * time.Millisecond)
			close(exitChan)
			for {
				goroutineNum := runtime.NumGoroutine()

				if goroutineNum <= startGoroutineNum {
					break
				}
				time.Sleep(10 * time.Millisecond)
			}

			points := router.GetData()
			if _, ok := points["foo.bar"]; !ok {
				t.Errorf("Test failed, got %v", points)
			}
		})
	}
}

func BenchmarkLineProtocolParserSmall(b *testing.B) {
	var defaultLoggerConfig = zapwriter.Config{
		Logger:           "",
		File:             "stderr",
		Level:            "debug",
		Encoding:         "json",
		EncodingTime:     "iso8601",
		EncodingDuration: "seconds",
	}

	_ = zapwriter.ApplyConfig([]zapwriter.Config{defaultLoggerConfig})

	dir, err := ioutil.TempDir("", "example")
	if err != nil {
		b.Fatal(err)
	}

	defer os.RemoveAll(dir)

	exitChan := make(chan struct{})
	config := Config{
		Listen:   dir + "/test.unix",
		Protocol: "unix",
		Workers:  1,
		Strict:   true,
	}
	router := routers.NewDummyRouter()
	maxBatchSize := 100
	r, err := NewGraphiteLineReceiver(config, router, exitChan, maxBatchSize, config.Workers, 100*time.Millisecond, time.Second)
	if err != nil {
		b.Fatalf("Failed to initialize receiver: %v", err)
	}
	line := stringToMetric("foo 10 100")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d, err := r.Parse(line)
		if err != nil {
			b.Fatal("Unexpected error")
		}
		_ = d
	}
}

func BenchmarkLineProtocolParserSmallRelaxed(b *testing.B) {
	var defaultLoggerConfig = zapwriter.Config{
		Logger:           "",
		File:             "stderr",
		Level:            "debug",
		Encoding:         "json",
		EncodingTime:     "iso8601",
		EncodingDuration: "seconds",
	}

	_ = zapwriter.ApplyConfig([]zapwriter.Config{defaultLoggerConfig})

	dir, err := ioutil.TempDir("", "example")
	if err != nil {
		b.Fatal(err)
	}

	defer os.RemoveAll(dir)

	exitChan := make(chan struct{})
	config := Config{
		Listen:   dir + "/test.unix",
		Protocol: "unix",
		Workers:  1,
		Strict:   false,
	}
	router := routers.NewDummyRouter()
	maxBatchSize := 100
	r, err := NewGraphiteLineReceiver(config, router, exitChan, maxBatchSize, config.Workers, 100*time.Millisecond, time.Second)
	if err != nil {
		b.Fatalf("Failed to initialize receiver: %v", err)
	}
	line := stringToMetric("foo 10 100")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d, err := r.parseRelaxed(line)
		if err != nil {
			b.Fatal("Unexpected error")
		}
		_ = d
	}
}

func BenchmarkLineProtocolParserSmallRelaxedExtra5Spaces(b *testing.B) {
	var defaultLoggerConfig = zapwriter.Config{
		Logger:           "",
		File:             "stderr",
		Level:            "debug",
		Encoding:         "json",
		EncodingTime:     "iso8601",
		EncodingDuration: "seconds",
	}

	_ = zapwriter.ApplyConfig([]zapwriter.Config{defaultLoggerConfig})

	dir, err := ioutil.TempDir("", "example")
	if err != nil {
		b.Fatal(err)
	}

	defer os.RemoveAll(dir)

	exitChan := make(chan struct{})
	config := Config{
		Listen:   dir + "/test2.unix",
		Protocol: "unix",
		Workers:  1,
		Strict:   false,
	}
	router := routers.NewDummyRouter()
	maxBatchSize := 100
	r, err := NewGraphiteLineReceiver(config, router, exitChan, maxBatchSize, config.Workers, 100*time.Millisecond, time.Second)
	if err != nil {
		b.Fatalf("Failed to initialize receiver: %v", err)
	}
	line := stringToMetric("foo      10 100")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d, err := r.parseRelaxed(line)
		if err != nil {
			b.Fatalf("Unexpected error: %v", err)
		}
		_ = d
	}
}

func BenchmarkLineProtocolParserLong(b *testing.B) {
	var defaultLoggerConfig = zapwriter.Config{
		Logger:           "",
		File:             "stderr",
		Level:            "debug",
		Encoding:         "json",
		EncodingTime:     "iso8601",
		EncodingDuration: "seconds",
	}

	_ = zapwriter.ApplyConfig([]zapwriter.Config{defaultLoggerConfig})

	dir, err := ioutil.TempDir("", "example")
	if err != nil {
		b.Fatal(err)
	}

	defer os.RemoveAll(dir)

	exitChan := make(chan struct{})
	config := Config{
		Listen:   dir + "/test.unix",
		Protocol: "unix",
		Workers:  1,
		Strict:   true,
	}
	router := routers.NewDummyRouter()
	maxBatchSize := 100
	r, err := NewGraphiteLineReceiver(config, router, exitChan, maxBatchSize, config.Workers, 100*time.Millisecond, time.Second)
	if err != nil {
		b.Fatalf("Failed to initialize receiver: %v", err)
	}
	line := stringToMetric("foo.bar.baz.boo.foo.bar.bar.bar.bar.baz.count 10.003 1500000000")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d, err := r.Parse(line)
		if err != nil {
			b.Fatal("Unexpected error")
		}
		_ = d
	}
}

func BenchmarkGraphiteFullPipelineSingleMetric(b *testing.B) {
	// Initialize r
	var defaultLoggerConfig = zapwriter.Config{
		Logger:           "",
		File:             "stderr",
		Level:            "info",
		Encoding:         "json",
		EncodingTime:     "iso8601",
		EncodingDuration: "seconds",
	}

	_ = zapwriter.ApplyConfig([]zapwriter.Config{defaultLoggerConfig})

	startGoroutineNum := runtime.NumGoroutine()
	dir, err := ioutil.TempDir("", "example")
	if err != nil {
		b.Fatal(err)
	}

	defer os.RemoveAll(dir)

	exitChan := make(chan struct{})
	config := Config{
		Listen:   dir + "/test.unix",
		Protocol: "unix",
		Workers:  runtime.NumCPU(),
		Strict:   true,
	}
	router := routers.NewDummyRouter()
	maxBatchSize := 10
	r, err := NewGraphiteLineReceiver(config, router, exitChan, maxBatchSize, config.Workers, 10*time.Millisecond, time.Second)
	if err != nil {
		b.Fatalf("Failed to initialize r: %v", err)
	}

	go r.Start()
	// End of initialization
	line := stringToMetric("foo.bar 10 100")

	sender, err := net.Dial(config.Protocol, config.Listen)
	if err != nil {
		b.Fatalf("Failed to establish connection: %v", err)
	}

	sender.SetWriteDeadline(time.Now().Add(100 * time.Millisecond))
	b.ResetTimer()
	var lastRcvDeadline time.Time
	sendTimeout := 50 * time.Millisecond
	for i := 0; i < b.N; i++ {
		now := time.Now()
		if now.Sub(lastRcvDeadline) > (sendTimeout >> 2) {
			err = sender.SetDeadline(now.Add(sendTimeout))
			if err != nil {
				b.Fatalf("Failed to send data: %v", err)
			}
			lastRcvDeadline = now
		}
		_, err = sender.Write(line)
		if err != nil {
			b.Fatalf("Failed to send data: %v", err)
		}
	}

	sender.Close()
	time.Sleep(10 * time.Millisecond)
	close(exitChan)
	for {
		goroutineNum := runtime.NumGoroutine()

		if goroutineNum <= startGoroutineNum {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	points := router.GetData()
	if _, ok := points["foo.bar"]; !ok {
		b.Errorf("Benchmark failed, got %v", points)
	}
}
