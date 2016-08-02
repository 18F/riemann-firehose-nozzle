package riemannfirehosenozzle_test

import (
	. "github.com/18F/influxdb-firehose-nozzle/testhelpers"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/cloudfoundry/sonde-go/events"
	"github.com/18F/influxdb-firehose-nozzle/influxdbclient"
	"github.com/18F/influxdb-firehose-nozzle/influxdbfirehosenozzle"
	"github.com/18F/influxdb-firehose-nozzle/nozzleconfig"
	"github.com/18F/influxdb-firehose-nozzle/uaatokenfetcher"
	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/websocket"
	"github.com/onsi/gomega/gbytes"
)

var _ = Describe("Datadog Firehose Nozzle", func() {
	var fakeUAA *FakeUAA
	var fakeFirehose *FakeFirehose
	var influxDbAPI *FakeInfluxDbAPI
	var config *nozzleconfig.NozzleConfig
	var nozzle *influxdbfirehosenozzle.InfluxDbFirehoseNozzle
	var logOutput *gbytes.Buffer

	BeforeEach(func() {
		fakeUAA = NewFakeUAA("bearer", "123456789")
		fakeToken := fakeUAA.AuthToken()
		fakeFirehose = NewFakeFirehose(fakeToken)
		influxDbAPI = NewFakeInfluxDbAPI()

		fakeUAA.Start()
		fakeFirehose.Start()
		influxDbAPI.Start()

		tokenFetcher := &uaatokenfetcher.UAATokenFetcher{
			UaaUrl: fakeUAA.URL(),
		}

		config = &nozzleconfig.NozzleConfig{
			UAAURL:               fakeUAA.URL(),
			FlushDurationSeconds: 10,
			InfluxDbUrl:          influxDbAPI.URL(),
			TrafficControllerURL: strings.Replace(fakeFirehose.URL(), "http:", "ws:", 1),
			DisableAccessControl: false,
			MetricPrefix:         "datadog.nozzle.",
		}

		logOutput = gbytes.NewBuffer()
		log.SetOutput(logOutput)
		nozzle = influxdbfirehosenozzle.NewInfluxDbFirehoseNozzle(config, tokenFetcher)
	})

	AfterEach(func() {
		fakeUAA.Close()
		fakeFirehose.Close()
		influxDbAPI.Close()
	})

	It("receives data from the firehose", func(done Done) {
		defer close(done)

		for i := 0; i < 10; i++ {
			envelope := events.Envelope{
				Origin:    proto.String("origin"),
				Timestamp: proto.Int64(1000000000),
				EventType: events.Envelope_ValueMetric.Enum(),
				ValueMetric: &events.ValueMetric{
					Name:  proto.String(fmt.Sprintf("metricName-%d", i)),
					Value: proto.Float64(float64(i)),
					Unit:  proto.String("gauge"),
				},
				Deployment: proto.String("deployment-name"),
				Job:        proto.String("doppler"),
			}
			fakeFirehose.AddEvent(envelope)
		}

		go nozzle.Start()

		var contents []byte
		Eventually(influxDbAPI.ReceivedContents).Should(Receive(&contents))

		var payload influxdbclient.Payload
		err := json.Unmarshal(contents, &payload)
		Expect(err).ToNot(HaveOccurred())

		Expect(logOutput).ToNot(gbytes.Say("Error while reading from the firehose"))

		// +3 internal metrics that show totalMessagesReceived, totalMetricSent, and slowConsumerAlert
		Expect(payload.Series).To(HaveLen(13))

	}, 2)

	It("sends a server disconnected metric when the server disconnects abnormally", func(done Done) {
		defer close(done)

		for i := 0; i < 10; i++ {
			envelope := events.Envelope{
				Origin:    proto.String("origin"),
				Timestamp: proto.Int64(1000000000),
				EventType: events.Envelope_ValueMetric.Enum(),
				ValueMetric: &events.ValueMetric{
					Name:  proto.String(fmt.Sprintf("metricName-%d", i)),
					Value: proto.Float64(float64(i)),
					Unit:  proto.String("gauge"),
				},
				Deployment: proto.String("deployment-name"),
				Job:        proto.String("doppler"),
			}
			fakeFirehose.AddEvent(envelope)
		}

		fakeFirehose.SetCloseMessage(websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "Client did not respond to ping before keep-alive timeout expired."))

		go nozzle.Start()

		var contents []byte
		Eventually(influxDbAPI.ReceivedContents).Should(Receive(&contents))

		var payload datadogclient.Payload
		err := json.Unmarshal(contents, &payload)
		Expect(err).ToNot(HaveOccurred())

		slowConsumerMetric := findSlowConsumerMetric(payload)
		Expect(slowConsumerMetric).NotTo(BeNil())
		Expect(slowConsumerMetric.Points).To(HaveLen(1))
		Expect(slowConsumerMetric.Points[0].Value).To(BeEquivalentTo(1))

		Expect(logOutput).To(gbytes.Say("Error while reading from the firehose"))
		Expect(logOutput).To(gbytes.Say("Client did not respond to ping before keep-alive timeout expired."))
		Expect(logOutput).To(gbytes.Say("Disconnected because nozzle couldn't keep up."))
	}, 2)

	It("does not report slow consumer error when closed for other reasons", func(done Done) {
		defer close(done)

		fakeFirehose.SetCloseMessage(websocket.FormatCloseMessage(websocket.CloseInvalidFramePayloadData, "Weird things happened."))

		go nozzle.Start()

		var contents []byte
		Eventually(influxDbAPI.ReceivedContents).Should(Receive(&contents))

		var payload datadogclient.Payload
		err := json.Unmarshal(contents, &payload)
		Expect(err).ToNot(HaveOccurred())

		errMetric := findSlowConsumerMetric(payload)
		Expect(errMetric).NotTo(BeNil())
		Expect(errMetric.Points[0].Value).To(BeEquivalentTo(0))

		Expect(logOutput).To(gbytes.Say("Error while reading from the firehose"))
		Expect(logOutput).NotTo(gbytes.Say("Client did not respond to ping before keep-alive timeout expired."))
		Expect(logOutput).NotTo(gbytes.Say("Disconnected because nozzle couldn't keep up."))
	}, 2)

	It("gets a valid authentication token", func() {
		go nozzle.Start()
		Eventually(fakeFirehose.Requested).Should(BeTrue())
		Consistently(fakeFirehose.LastAuthorization).Should(Equal("bearer 123456789"))
	})

	Context("receives a truncatingbuffer.droppedmessage value metric,", func() {
		It("sets a slow-consumer error", func() {
			slowConsumerError := events.Envelope{
				Origin:    proto.String("doppler"),
				Timestamp: proto.Int64(1000000000),
				EventType: events.Envelope_CounterEvent.Enum(),
				CounterEvent: &events.CounterEvent{
					Name:  proto.String("TruncatingBuffer.DroppedMessages"),
					Delta: proto.Uint64(1),
					Total: proto.Uint64(1),
				},
				Deployment: proto.String("deployment-name"),
				Job:        proto.String("doppler"),
			}
			fakeFirehose.AddEvent(slowConsumerError)

			go nozzle.Start()

			var contents []byte
			Eventually(influxDbAPI.ReceivedContents).Should(Receive(&contents))

			var payload datadogclient.Payload
			err := json.Unmarshal(contents, &payload)
			Expect(err).ToNot(HaveOccurred())

			Expect(findSlowConsumerMetric(payload)).NotTo(BeNil())

			Expect(logOutput).To(gbytes.Say("We've intercepted an upstream message which indicates that the nozzle or the TrafficController is not keeping up. Please try scaling up the nozzle."))
		})
	})

	Context("when the DisableAccessControl is set to true", func() {
		var tokenFetcher *FakeTokenFetcher

		BeforeEach(func() {
			fakeUAA = NewFakeUAA("", "")
			fakeToken := fakeUAA.AuthToken()
			fakeFirehose = NewFakeFirehose(fakeToken)
			influxDbAPI = NewFakeInfluxDbAPI()
			tokenFetcher = &FakeTokenFetcher{}

			fakeUAA.Start()
			fakeFirehose.Start()
			influxDbAPI.Start()

			config = &nozzleconfig.NozzleConfig{
				FlushDurationSeconds: 1,
				DataDogURL:           influxDbAPI.URL(),
				TrafficControllerURL: strings.Replace(fakeFirehose.URL(), "http:", "ws:", 1),
				DisableAccessControl: true,
			}

			nozzle = datadogfirehosenozzle.NewDatadogFirehoseNozzle(config, tokenFetcher)
		})

		AfterEach(func() {
			fakeUAA.Close()
			fakeFirehose.Close()
			influxDbAPI.Close()
		})

		It("can still tries to connect to the firehose", func() {
			go nozzle.Start()
			Eventually(fakeFirehose.Requested).Should(BeTrue())
		})

		It("gets an empty authentication token", func() {
			go nozzle.Start()
			Consistently(fakeUAA.Requested).Should(Equal(false))
			Consistently(fakeFirehose.LastAuthorization).Should(Equal(""))
		})

		It("does not rquire the presence of config.UAAURL", func() {
			nozzle.Start()
			Consistently(func() int { return tokenFetcher.NumCalls }).Should(Equal(0))
		})
	})

	Context("when idle timeout has expired", func() {
		var fakeIdleFirehose *FakeIdleFirehose
		BeforeEach(func() {
			fakeIdleFirehose = NewFakeIdleFirehose(time.Second * 7)
			fakeIdleFirehose.Start()

			config = &nozzleconfig.NozzleConfig{
				DataDogURL:           influxDbAPI.URL(),
				TrafficControllerURL: strings.Replace(fakeIdleFirehose.URL(), "http:", "ws:", 1),
				DisableAccessControl: true,
				IdleTimeoutSeconds:   1,
				FlushDurationSeconds: 1,
			}

			tokenFetcher := &FakeTokenFetcher{}
			nozzle = datadogfirehosenozzle.NewDatadogFirehoseNozzle(config, tokenFetcher)
		})
		AfterEach(func() {
			fakeIdleFirehose.Close()
		})

		It("Start returns an error", func() {
			err := nozzle.Start()
			Expect(err).To(HaveOccurred())
			Expect(err.Error()).To(ContainSubstring("i/o timeout"))
		})
	})
})

func findSlowConsumerMetric(payload datadogclient.Payload) *datadogclient.Metric {
	for _, metric := range payload.Series {
		if metric.Metric == "datadog.nozzle.slowConsumerAlert" {
			return &metric
		}
	}
	return nil
}
