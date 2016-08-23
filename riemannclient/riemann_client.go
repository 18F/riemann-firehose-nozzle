package riemannclient

import (
	"fmt"
	"time"

	"log"

	"github.com/amir/raidman"
	"github.com/cloudfoundry/sonde-go/events"
)

type Client struct {
	host                  string
	port                  string
	transport             string
	metricPoints          map[metricKey]metricValue
	prefix                string
	deployment            string
	ip                    string
	totalMessagesReceived uint64
	totalMetricsSent      uint64
}

type metricKey struct {
	eventType  events.Envelope_EventType
	name       string
	deployment string
	job        string
	index      string
	ip         string
}

type metricValue struct {
	points     []Point
	attributes map[string]string
	tags       []string
}

type Point struct {
	Timestamp int64
	Value     float64
}

func New(host string, port string, transport string, prefix string, deployment string, ip string) *Client {
	return &Client{
		host:         host,
		port:         port,
		transport:    transport,
		metricPoints: make(map[metricKey]metricValue),
		prefix:       prefix,
		deployment:   deployment,
		ip:           ip,
	}
}

func (c *Client) AlertSlowConsumerError() {
	c.addInternalMetric("slowConsumerAlert", uint64(1))
}

func (c *Client) AddMetric(envelope *events.Envelope) {
	c.totalMessagesReceived++
	if envelope.GetEventType() != events.Envelope_ValueMetric && envelope.GetEventType() != events.Envelope_CounterEvent {
		return
	}

	key := metricKey{
		eventType:  envelope.GetEventType(),
		name:       getName(envelope),
		deployment: envelope.GetDeployment(),
		job:        envelope.GetJob(),
		index:      envelope.GetIndex(),
		ip:         envelope.GetIp(),
	}

	mVal := c.metricPoints[key]
	value := getValue(envelope)

	mVal.attributes = getAttributes(envelope)
	mVal.tags = getTags(envelope)
	mVal.points = append(mVal.points, Point{
		Timestamp: envelope.GetTimestamp() / int64(time.Second),
		Value:     value,
	})

	c.metricPoints[key] = mVal
}

func (c *Client) PostMetrics() error {
	c.populateInternalMetrics()
	numMetrics := len(c.metricPoints)
	log.Printf("Posting %d metrics", numMetrics)

	metrics := c.formatMetrics()

	client, err := raidman.Dial(c.transport, fmt.Sprintf("%s:%s", c.host, c.port))
	if err != nil {
		return err
	}

	err = client.SendMulti(metrics)
	if err != nil {
		return err
	}

	c.totalMetricsSent += uint64(len(metrics))
	c.metricPoints = make(map[metricKey]metricValue)

	return nil
}

func (c *Client) populateInternalMetrics() {
	c.addInternalMetric("totalMessagesReceived", c.totalMessagesReceived)
	c.addInternalMetric("totalMetricsSent", c.totalMetricsSent)

	if !c.containsSlowConsumerAlert() {
		c.addInternalMetric("slowConsumerAlert", uint64(0))
	}
}

func (c *Client) containsSlowConsumerAlert() bool {
	key := metricKey{
		name:       "slowConsumerAlert",
		deployment: c.deployment,
		ip:         c.ip,
	}
	_, ok := c.metricPoints[key]
	return ok
}

func (c *Client) formatMetrics() []*raidman.Event {
	metrics := []*raidman.Event{}

	for key, metric := range c.metricPoints {
		for _, point := range metric.points {
			metrics = append(metrics, &raidman.Event{
				Service:    c.prefix + key.name,
				Time:       point.Timestamp,
				Metric:     point.Value,
				Attributes: metric.attributes,
				Tags:       metric.tags,
			})
		}
	}

	return metrics
}

func formatTags(tags []string) string {
	var newTags string
	for index, tag := range tags {
		if index > 0 {
			newTags += ","
		}

		newTags += tag
	}
	return newTags
}

func (c *Client) addInternalMetric(name string, value uint64) {
	key := metricKey{
		name:       name,
		deployment: c.deployment,
		ip:         c.ip,
	}

	point := Point{
		Timestamp: time.Now().Unix(),
		Value:     float64(value),
	}

	mValue := metricValue{
		attributes: map[string]string{
			"ip":         c.ip,
			"deployment": c.deployment,
		},
		tags: []string{
			"internal",
		},
		points: []Point{point},
	}

	c.metricPoints[key] = mValue
}

func getName(envelope *events.Envelope) string {
	switch envelope.GetEventType() {
	case events.Envelope_ValueMetric:
		return envelope.GetOrigin() + "." + envelope.GetValueMetric().GetName()
	case events.Envelope_CounterEvent:
		return envelope.GetOrigin() + "." + envelope.GetCounterEvent().GetName()
	default:
		panic("Unknown event type")
	}
}

func getValue(envelope *events.Envelope) float64 {
	switch envelope.GetEventType() {
	case events.Envelope_ValueMetric:
		return envelope.GetValueMetric().GetValue()
	case events.Envelope_CounterEvent:
		return float64(envelope.GetCounterEvent().GetTotal())
	default:
		panic("Unknown event type")
	}
}

func getAttributes(envelope *events.Envelope) map[string]string {
	attributes := make(map[string]string)

	attributes = appendAttributeIfNotEmpty(attributes, "deployment", envelope.GetDeployment())
	attributes = appendAttributeIfNotEmpty(attributes, "job", envelope.GetJob())
	attributes = appendAttributeIfNotEmpty(attributes, "index", envelope.GetIndex())
	attributes = appendAttributeIfNotEmpty(attributes, "ip", envelope.GetIp())

	return attributes
}

func getTags(envelope *events.Envelope) []string {
	return []string{fmt.Sprintf("%+v", envelope.GetEventType())}
}

func appendAttributeIfNotEmpty(attributes map[string]string, key string, value string) map[string]string {
	if value != "" {
		attributes[key] = value
	}
	return attributes
}
