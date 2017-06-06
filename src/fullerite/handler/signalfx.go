package handler

import (
	"fullerite/config"
	"fullerite/metric"
	"fullerite/util"

	"bytes"
	"time"

	l "github.com/Sirupsen/logrus"
	"github.com/golang/protobuf/proto"
)

func init() {
	RegisterHandler("SignalFx", newSignalFx)
}

// SignalFx Handler
type SignalFx struct {
	BaseHandler
	endpoint   string
	authToken  string
	httpClient *util.HTTPAlive

	// If the following dimension exists,
	// then batch and emit it separately to Sfx
	batchByDimension string

	// When emitting batches made from "batchByDimension"
	// config, use the following auth token
	perBatchAuthToken map[string]string
}

var allowedNamePuncts = []rune{}
var allowedDimKeyPuncts = []rune{'-', '_'}

// newSignalFx returns a new SignalFx handler.
func newSignalFx(
	channel chan metric.Metric,
	initialInterval int,
	initialBufferSize int,
	initialTimeout time.Duration,
	log *l.Entry) Handler {

	inst := new(SignalFx)
	inst.name = "SignalFx"

	inst.interval = initialInterval
	inst.maxBufferSize = initialBufferSize
	inst.timeout = initialTimeout
	inst.maxIdleConnectionsPerHost = DefaultMaxIdleConnectionsPerHost
	inst.keepAliveInterval = DefaultKeepAliveInterval
	inst.log = log
	inst.channel = channel

	return inst
}

// Configure accepts the different configuration options for the signalfx handler
func (s *SignalFx) Configure(configMap map[string]interface{}) {
	if authToken, exists := configMap["authToken"]; exists {
		s.authToken = authToken.(string)
	} else {
		s.log.Error("There was no auth key specified for the SignalFx Handler, there won't be any emissions")
	}
	if endpoint, exists := configMap["endpoint"]; exists {
		s.endpoint = endpoint.(string)
	} else {
		s.log.Error("There was no endpoint specified for the SignalFx Handler, there won't be any emissions")
	}

	if batchByDimension, exists := configMap["batchByDimension"]; exists {
		s.batchByDimension = batchByDimension.(string)
		s.log.Info("Batching metrics by dimension: ", s.batchByDimension)

		// Checking if authtoken for batches are specified
		if perBatchAuthToken, exists := configMap["perBatchAuthToken"]; exists {
			s.perBatchAuthToken = config.GetAsMap(perBatchAuthToken)
			s.log.Info("Loaded authkeys for batches")
		} else {
			s.log.Info("Using default authToken for all batches")
		}

		// Use custom emission time reporting when
		// employing any fancy batching mechanism
		s.OverrideBaseEmissionMetricsReporter()
	}

	s.configureCommonParams(configMap)
}

// Endpoint returns SignalFx' API endpoint
func (s SignalFx) Endpoint() string {
	return s.endpoint
}

// Run runs the handler main loop
func (s *SignalFx) Run() {
	httpAliveClient := new(util.HTTPAlive)
	httpAliveClient.Configure(s.timeout,
		time.Duration(s.KeepAliveInterval())*time.Second,
		s.MaxIdleConnectionsPerHost())
	s.httpClient = httpAliveClient

	s.run(s.emitMetrics)
}

func signalFxValueSanitize(value string) string {
	return util.StrSanitize(value, true, allowedNamePuncts)
}

func signalFxKeySanitize(key string) string {
	return util.StrSanitize(key, false, allowedDimKeyPuncts)
}

func (s SignalFx) convertToProto(incomingMetric metric.Metric) *DataPoint {
	// Create a new values for the Datapoint that requires pointers.
	outname := s.Prefix() + signalFxValueSanitize(incomingMetric.Name)
	value := incomingMetric.Value

	now := time.Now().UnixNano() / int64(time.Millisecond)
	datapoint := new(DataPoint)
	datapoint.Timestamp = &now
	datapoint.Metric = &outname
	datapoint.Value = &Datum{
		DoubleValue: &value,
	}
	datapoint.Source = new(string)
	*datapoint.Source = "fullerite"

	switch incomingMetric.MetricType {
	case metric.Gauge:
		datapoint.MetricType = MetricType_GAUGE.Enum()
	case metric.Counter:
		datapoint.MetricType = MetricType_COUNTER.Enum()
	case metric.CumulativeCounter:
		datapoint.MetricType = MetricType_CUMULATIVE_COUNTER.Enum()
	}

	for key, value := range s.getSanitizedDimensions(incomingMetric) {
		// Dimension (protobuf) require a pointer to string
		// values. We need to create new string objects in the
		// scope of this for loop not to repeatedly add the
		// same key:value pairs to the the datapoint.
		dimKey := key
		dimValue := value
		dim := Dimension{
			Key:   &dimKey,
			Value: &dimValue,
		}
		datapoint.Dimensions = append(datapoint.Dimensions, &dim)
	}

	return datapoint
}

func (s SignalFx) getSanitizedDimensions(incomingMetric metric.Metric) map[string]string {
	dimSanitized := make(map[string]string)
	dimensions := incomingMetric.GetDimensions(s.DefaultDimensions())
	for key, value := range dimensions {
		dimSanitized[signalFxKeySanitize(key)] = signalFxValueSanitize(value)
	}
	return dimSanitized
}

// getAuthTokenForBatch will return an AuthToken associated with a batchname
// if no such batch name exists, the default auth token will be returned.
func (s *SignalFx) getAuthTokenForBatch(batchName string) string {
	if authToken, exists := s.perBatchAuthToken[batchName]; exists {
		return authToken
	}

	return s.authToken
}

func (s *SignalFx) makeBatches(metrics []metric.Metric) map[string][]metric.Metric {
	m := make(map[string][]metric.Metric)
	for _, metric := range metrics {
		dimValue := metric.Dimensions[s.batchByDimension]
		m[dimValue] = append(m[dimValue], metric)
	}
	return m
}

func (s *SignalFx) emitBatch(batchName string, metrics []metric.Metric) bool {
	s.log.Info("Starting to emit ", len(metrics), " metrics")

	if len(metrics) == 0 {
		s.log.Warn("Skipping send because of an empty payload")
		return false
	}

	datapoints := make([]*DataPoint, 0, len(metrics))
	for _, m := range metrics {
		datapoints = append(datapoints, s.convertToProto(m))
	}

	payload := new(DataPointUploadMessage)
	payload.Datapoints = datapoints

	// Get auth token to be used for batch
	authToken := s.getAuthTokenForBatch(batchName)

	if authToken == "" || s.endpoint == "" {
		s.log.Warn("Skipping emission because we're missing the auth token ",
			"or the endpoint, payload would have been ", payload)
		return false
	}
	serialized, err := proto.Marshal(payload)
	if err != nil {
		s.log.Error("Failed to serailize payload ", payload)
		return false
	}

	customHeader := map[string]string{
		"X-SF-TOKEN":   authToken,
		"Content-Type": "application/x-protobuf",
	}

	rsp, err := s.httpClient.MakeRequest(
		"POST",
		s.endpoint,
		bytes.NewBuffer(serialized),
		customHeader)

	if err != nil {
		s.log.Error("Failed to make request ", err,
			" to endpoint ", s.endpoint)
		return false
	}

	if rsp.StatusCode != 200 {
		s.log.Error("Failed to post to signalfx @", s.endpoint,
			" status was ", rsp.StatusCode,
			" rsp body was ", string(rsp.Body),
			" payload was ", payload)
		return false
	}

	s.log.Info("Successfully sent ", len(datapoints), " datapoints to SignalFx")
	return true
}

func (s *SignalFx) emitMetrics(metrics []metric.Metric) bool {

	if len(metrics) == 0 {
		s.log.Warn("Skipping send because of an empty payload")
		return false
	}

	if s.batchByDimension == "" {
		return s.emitBatch("", metrics)
	}

	for batchName, metricBatch := range s.makeBatches(metrics) {
		start := time.Now()
		emissionResult := s.emitBatch(batchName, metricBatch)
		elapsed := time.Since(start)
		timing := emissionTiming{
			timestamp:   time.Now(),
			duration:    elapsed,
			metricsSent: len(metrics),
		}
		s.reportEmissionMetrics(emissionResult, timing)
	}
	return true
}
