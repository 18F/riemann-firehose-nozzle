package influxdbfirehosenozzle

import (
	"crypto/tls"
	"log"
	//"os"
	"time"

	"github.com/18F/influxdb-firehose-nozzle/influxdbclient"
	"github.com/18F/influxdb-firehose-nozzle/nozzleconfig"
	"github.com/cloudfoundry/noaa"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gorilla/websocket"
	"github.com/pivotal-golang/localip"
)

type InfluxDbFirehoseNozzle struct {
	config           *nozzleconfig.NozzleConfig
	errs             chan error
	messages         chan *events.Envelope
	authTokenFetcher AuthTokenFetcher
	consumer         *noaa.Consumer
	client           *influxdbclient.Client
}

type AuthTokenFetcher interface {
	FetchAuthToken() string
}

func NewInfluxDbFirehoseNozzle(config *nozzleconfig.NozzleConfig, tokenFetcher AuthTokenFetcher) *InfluxDbFirehoseNozzle {
	return &InfluxDbFirehoseNozzle{
		config:           config,
		errs:             make(chan error),
		messages:         make(chan *events.Envelope),
		authTokenFetcher: tokenFetcher,
	}
}

func (d *InfluxDbFirehoseNozzle) Start() error {
	var authToken string

	if !d.config.DisableAccessControl {
		authToken = d.authTokenFetcher.FetchAuthToken()
	}

	log.Print("Starting InfluxDb Firehose Nozzle...")
	d.createClient()
	d.consumeFirehose(authToken)
	err := d.postToInfluxDb()
	log.Print("InfluxDb Firehose Nozzle shutting down...")
	return err
}

func (d *InfluxDbFirehoseNozzle) createClient() {
	ipAddress, err := localip.LocalIP()
	if err != nil {
		panic(err)
	}

	d.client = influxdbclient.New(d.config.InfluxDbUrl, d.config.InfluxDbDatabase, d.config.InfluxDbUser,
		d.config.InfluxDbPassword, d.config.MetricPrefix, d.config.Deployment, ipAddress)
}

func (d *InfluxDbFirehoseNozzle) consumeFirehose(authToken string) {
	d.consumer = noaa.NewConsumer(
		d.config.TrafficControllerURL,
		&tls.Config{InsecureSkipVerify: d.config.InsecureSSLSkipVerify},
		nil)
	d.consumer.SetIdleTimeout(time.Duration(d.config.IdleTimeoutSeconds) * time.Second)
	go d.consumer.Firehose(d.config.FirehoseSubscriptionID, authToken, d.messages, d.errs)
}

func (d *InfluxDbFirehoseNozzle) postToInfluxDb() error {
	ticker := time.NewTicker(time.Duration(d.config.FlushDurationSeconds) * time.Second)
	for {
		select {
		case <-ticker.C:
			d.postMetrics()
		case envelope := <-d.messages:
			d.handleMessage(envelope)
			d.client.AddMetric(envelope)
		case err := <-d.errs:
			d.handleError(err)
			return err
		}
	}
}

func (d *InfluxDbFirehoseNozzle) postMetrics() {
	err := d.client.PostMetrics()
	if err != nil {
		log.Println("FATAL ERROR: " + err.Error())
		// os.Exit(1)
	}
}

func (d *InfluxDbFirehoseNozzle) handleError(err error) {
	switch closeErr := err.(type) {
	case *websocket.CloseError:
		switch closeErr.Code {
		case websocket.CloseNormalClosure:
		// no op
		case websocket.ClosePolicyViolation:
			log.Printf("Error while reading from the firehose: %v", err)
			log.Printf("Disconnected because nozzle couldn't keep up. Please try scaling up the nozzle.")
			d.client.AlertSlowConsumerError()
		default:
			log.Printf("Error while reading from the firehose: %v", err)
		}
	default:
		log.Printf("Error while reading from the firehose: %v", err)

	}

	log.Printf("Closing connection with traffic controller due to %v", err)
	d.consumer.Close()
	d.postMetrics()
}

func (d *InfluxDbFirehoseNozzle) handleMessage(envelope *events.Envelope) {
	if envelope.GetEventType() == events.Envelope_CounterEvent && envelope.CounterEvent.GetName() == "TruncatingBuffer.DroppedMessages" && envelope.GetOrigin() == "doppler" {
		log.Printf("We've intercepted an upstream message which indicates that the nozzle or the TrafficController is not keeping up. Please try scaling up the nozzle.")
		d.client.AlertSlowConsumerError()
	}
}
