package handler

import (
	"fullerite/metric"

	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	l "github.com/Sirupsen/logrus"
	"github.com/stretchr/testify/assert"
)

func assertEmpty(t *testing.T, channel chan metric.Metric) {
	close(channel)
	for range channel {
		t.Fatal("The channel was not empty")
	}
}

func TestNewHandler(t *testing.T) {
	names := []string{"Graphite", "Kairos", "SignalFx", "Datadog", "Log"}
	for _, name := range names {
		h := New(name)
		assert.NotNil(t, h, "should create a Handler for "+name)
		assert.NotNil(t, h.Channel(), "should create a channel")
		assert.Equal(t, name, h.Name())
		assert.Equal(t, "", h.Prefix(), "")
		assert.Equal(t, 0, len(h.DefaultDimensions()))
		assert.Equal(t, DefaultBufferSize, h.MaxBufferSize())
		assert.Equal(t, DefaultInterval, h.Interval())
		assert.Equal(t, name+"Handler", fmt.Sprintf("%s", h), "String() should append Handler to the name for "+name)

		// Test Set* functions
		h.SetInterval(999)
		assert.Equal(t, 999, h.Interval())

		h.SetMaxBufferSize(999)
		assert.Equal(t, 999, h.MaxBufferSize())

		dims := map[string]string{"test": "test value"}
		h.SetDefaultDimensions(dims)
		assert.Equal(t, 1, len(h.DefaultDimensions()))
	}
}

// If configured, per handler dimensions should over write default dimensions
func TestPerHandlerDimensions(t *testing.T) {
	b := new(BaseHandler)
	dims := map[string]string{"test": "test value", "host": "test host"}
	b.SetDefaultDimensions(dims)
	assert.Equal(t, 2, len(b.DefaultDimensions()))

	handlerLevelDimensions := "{ \"test\" : \"updated value\", \"runtimeenv\": \"dev\", \"region\":\"uswest1-devc\"}"
	configMap := map[string]interface{}{
		"defaultDimensions": handlerLevelDimensions,
	}

	b.configureCommonParams(configMap)
	assert.Equal(t, 3, len(b.DefaultDimensions()))
	assert.Equal(t, "updated value", b.DefaultDimensions()["test"])
	assert.Equal(t, "", b.DefaultDimensions()["host"])
}

func TestCollectorBlackList(t *testing.T) {
	b := new(BaseHandler)
	collectorBlackList := "[\"TestCollector1\", \"TestCollector2\"]"
	configMap := map[string]interface{}{
		"collectorBlackList": collectorBlackList,
	}

	b.configureCommonParams(configMap)
	assert.Equal(t, 2, len(b.CollectorBlackList()))

	val, _ := b.IsCollectorBlackListed("TestCollector1")
	assert.Equal(t, true, val)

	val, _ = b.IsCollectorBlackListed("WhiteListed")
	assert.Equal(t, false, val)
}

func TestCommonKeepAliveConfig(t *testing.T) {
	b := new(BaseHandler)

	configMap := map[string]interface{}{
		"keepAliveInterval":         100,
		"maxIdleConnectionsPerHost": 5,
	}
	b.configureCommonParams(configMap)
	assert.Equal(t, 5, b.MaxIdleConnectionsPerHost())
	assert.Equal(t, 100, b.KeepAliveInterval())
}

func TestEmissionAndRecord(t *testing.T) {
	emitCalled := false

	emitFunc := func([]metric.Metric) bool {
		emitCalled = true
		return true
	}
	metrics := []metric.Metric{metric.New("example")}

	base := BaseHandler{
		emissionTimingChannel: make(chan emissionTiming),
	}
	base.log = l.WithField("testing", "basehandler_emit")
	go base.emitAndTime(metrics, emitFunc)

	select {
	case timing := <-base.emissionTimingChannel:
		assert.NotNil(t, timing)
		assert.Equal(t, 1, timing.metricsSent)
		assert.NotNil(t, timing.timestamp)
		assert.NotNil(t, timing.duration)
	case <-time.After(2 * time.Second):
		t.Fatal("Failed to read from the callback channel after 2 seconds")
	}

	assert.True(t, emitCalled)
	base.emissionTimingChannel = nil
}

func TestRecordTimings(t *testing.T) {
	base := BaseHandler{
		emissionTimingChannel: make(chan emissionTiming),
	}
	base.log = l.WithField("testing", "basehandler_record")
	base.interval = 2

	minusFiveSec := -1 * 5 * time.Second
	minusSixSec := -1 * 6 * time.Second
	someDur := time.Duration(5)
	now := time.Now()

	// create a list of emissions in order with some older than 1 second
	base.emissionTimes.PushBack(emissionTiming{now.Add(minusSixSec), someDur, 0})
	base.emissionTimes.PushBack(emissionTiming{now.Add(minusFiveSec), someDur, 0})

	go func() {
		base.emissionTimingChannel <- emissionTiming{now, someDur, 0}
		close(base.emissionTimingChannel)
	}()

	base.recordEmissions()
	assert.Equal(t, 1, base.emissionTimes.Len())
	base.emissionTimingChannel = nil
}

func TestHandlerRunFlushInterval(t *testing.T) {
	var mu sync.Mutex
	base := BaseHandler{}
	base.log = l.WithField("testing", "basehandler_flush")
	base.interval = 1
	base.maxBufferSize = 2
	base.channel = make(chan metric.Metric)

	emitCalledOnce := false
	emitCalledTwice := false
	emitCalledThrice := false
	emitFunc := func(metrics []metric.Metric) bool {
		mu.Lock()
		defer mu.Unlock()
		if emitCalledOnce && !emitCalledTwice {
			assert.Equal(t, 1, len(metrics))
			emitCalledTwice = true
		} else {
			assert.Equal(t, 2, len(metrics))
			emitCalledOnce = true
		}
		return true
	}

	// now we are waiting for some metrics
	go base.run(emitFunc)

	base.channel <- metric.New("testMetric")
	base.channel <- metric.New("testMetric1")
	base.channel <- metric.New("testMetric2")
	time.Sleep(2 * time.Second)
	mu.Lock()
	assert.True(t, emitCalledOnce)
	assert.True(t, emitCalledTwice)
	assert.False(t, emitCalledThrice)
	mu.Unlock()
	assert.Equal(t, 1, base.GetEmissionTimesLen())
	assert.Equal(t, uint64(3), atomic.LoadUint64(&base.metricsSent))
	assert.Equal(t, uint64(0), atomic.LoadUint64(&base.metricsDropped))
	assert.Equal(t, uint64(2), atomic.LoadUint64(&base.totalEmissions))
	base.channel <- metric.Metric{}
}

func TestHandlerBufferSize(t *testing.T) {
	base := BaseHandler{}
	base.log = l.WithField("testing", "basehandler_flush")
	base.interval = 5
	base.maxBufferSize = 100
	base.channel = make(chan metric.Metric)
	base.collectorEndpoints = map[string]CollectorEnd{
		"collector1": CollectorEnd{make(chan metric.Metric), 3},
	}

	emitFunc := func(metrics []metric.Metric) bool {
		assert.Equal(t, 3, len(metrics))
		return true
	}

	go base.run(emitFunc)
	base.CollectorEndpoints()["collector1"].Channel <- metric.New("testMetric")
	base.CollectorEndpoints()["collector1"].Channel <- metric.New("testMetric1")
	base.CollectorEndpoints()["collector1"].Channel <- metric.New("testMetric2")
	time.Sleep(1 * time.Second)
	assert.Equal(t, uint64(3), atomic.LoadUint64(&base.metricsSent))
	assert.Equal(t, uint64(0), atomic.LoadUint64(&base.metricsDropped))

	// This is just to stop goroutines that have been started before
	base.channel <- metric.Metric{}
	base.CollectorEndpoints()["collector1"].Channel <- metric.Metric{}
}

func TestSentinelFlushing(t *testing.T) {
	base := BaseHandler{}
	base.log = l.WithField("testing", "basehandler_flush")
	base.interval = 100
	base.maxBufferSize = 100
	base.channel = make(chan metric.Metric)
	base.collectorEndpoints = map[string]CollectorEnd{
		"collector1": CollectorEnd{make(chan metric.Metric), 100},
	}

	emitFunc := func(metrics []metric.Metric) bool {
		assert.Equal(t, 2, len(metrics))
		return true
	}

	go base.run(emitFunc)
	base.CollectorEndpoints()["collector1"].Channel <- metric.New("testMetric")
	base.CollectorEndpoints()["collector1"].Channel <- metric.New("testMetric1")
	// emit sentinel value to trigger flush
	base.CollectorEndpoints()["collector1"].Channel <- metric.Sentinel()
	time.Sleep(1 * time.Second)
	assert.Equal(t, uint64(2), atomic.LoadUint64(&base.metricsSent))
	assert.Equal(t, uint64(0), atomic.LoadUint64(&base.metricsDropped))

	// send more metrics
	base.CollectorEndpoints()["collector1"].Channel <- metric.New("testMetric")
	base.CollectorEndpoints()["collector1"].Channel <- metric.New("testMetric1")
	// emit sentinel value to trigger flush
	base.CollectorEndpoints()["collector1"].Channel <- metric.Sentinel()
	time.Sleep(1 * time.Second)
	assert.Equal(t, uint64(4), atomic.LoadUint64(&base.metricsSent))
	assert.Equal(t, uint64(0), atomic.LoadUint64(&base.metricsDropped))
	assert.Equal(t, uint64(2), atomic.LoadUint64(&base.totalEmissions))

	// This is just to stop goroutines that have been started before
	base.channel <- metric.Metric{}
	base.CollectorEndpoints()["collector1"].Channel <- metric.Metric{}
}

func TestHandlerRun(t *testing.T) {
	var mu sync.Mutex
	base := BaseHandler{}
	base.log = l.WithField("testing", "basehandler_run")
	base.interval = 1
	base.maxBufferSize = 1
	base.channel = make(chan metric.Metric)

	emitCalled := false
	emitFunc := func(metrics []metric.Metric) bool {
		assert.Equal(t, 1, len(metrics))
		mu.Lock()
		defer mu.Unlock()
		emitCalled = true
		return true
	}

	// now we are waiting for some metrics
	go base.run(emitFunc)

	base.channel <- metric.New("testMetric")
	time.Sleep(1 * time.Second)
	mu.Lock()
	assert.True(t, emitCalled)
	mu.Unlock()
	assert.Equal(t, 1, base.GetEmissionTimesLen())
	assert.Equal(t, uint64(1), atomic.LoadUint64(&base.metricsSent))
	assert.Equal(t, uint64(0), atomic.LoadUint64(&base.metricsDropped))
	assert.Equal(t, uint64(1), atomic.LoadUint64(&base.totalEmissions))
	base.channel <- metric.Metric{}
}

func TestInternalMetrics(t *testing.T) {
	base := BaseHandler{}
	base.totalEmissions = 10
	base.metricsDropped = 100
	base.metricsSent = 2
	base.interval = 4

	timing := emissionTiming{time.Now(), 5 * time.Second, 0}
	base.emissionTimes.PushBack(timing)
	timing = emissionTiming{time.Now(), 10 * time.Second, 0}
	base.emissionTimes.PushBack(timing)
	timing = emissionTiming{time.Now(), 6 * time.Second, 0}
	base.emissionTimes.PushBack(timing)

	results := base.InternalMetrics()
	expected := metric.InternalMetrics{
		Counters: map[string]float64{
			"metricsDropped": 100,
			"metricsSent":    2,
			"totalEmissions": 10,
		},
		Gauges: map[string]float64{
			"averageEmissionTiming": 7,
			"emissionsInWindow":     3,
			"intervalLength":        4,
			"maxEmissionTiming":     10,
		},
	}
	assert.Equal(t, expected, results)
}

func TestInternalMetricsWithNan(t *testing.T) {
	base := BaseHandler{}

	expected := metric.InternalMetrics{
		Counters: map[string]float64{
			"metricsDropped": 0,
			"metricsSent":    0,
			"totalEmissions": 0,
		},
		// specifically missing the averageEmissionTiming
		// because we have no emissions yet
		Gauges: map[string]float64{
			"emissionsInWindow": 0,
			"intervalLength":    0,
		},
	}
	im := base.InternalMetrics()
	assert.Equal(t, expected, im)
}

func TestKeepAliveConfig(t *testing.T) {
	base := BaseHandler{}

	assert.Equal(t, 0, base.KeepAliveInterval())
	assert.Equal(t, 0, base.MaxIdleConnectionsPerHost())
}
