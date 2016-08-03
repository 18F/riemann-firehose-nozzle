package riemannfirehosenozzle

import (
	"crypto/tls"
	"log"
	"time"

	"github.com/18F/riemann-firehose-nozzle/nozzleconfig"
	"github.com/18F/riemann-firehose-nozzle/riemannclient"
	"github.com/cloudfoundry/noaa/consumer"
	"github.com/cloudfoundry/sonde-go/events"
	"github.com/gorilla/websocket"
	"github.com/pivotal-golang/localip"
)

type RiemannFirehoseNozzle struct {
	config           *nozzleconfig.NozzleConfig
	errs             <-chan error
	messages         <-chan *events.Envelope
	authTokenFetcher AuthTokenFetcher
	consumer         *consumer.Consumer
	client           *riemannclient.Client
}

type AuthTokenFetcher interface {
	FetchAuthToken() string
}

func NewRiemannFirehoseNozzle(config *nozzleconfig.NozzleConfig, tokenFetcher AuthTokenFetcher) *RiemannFirehoseNozzle {
	return &RiemannFirehoseNozzle{
		config:           config,
		authTokenFetcher: tokenFetcher,
	}
}

func (d *RiemannFirehoseNozzle) Start() error {
	var authToken string

	if !d.config.DisableAccessControl {
		authToken = d.authTokenFetcher.FetchAuthToken()
	}

	log.Print("Starting Riemann Firehose Nozzle...")
	d.createClient()
	d.consumeFirehose(authToken)
	err := d.postToRiemann()
	log.Print("Riemann Firehose Nozzle shutting down...")
	return err
}

func (d *RiemannFirehoseNozzle) createClient() {
	ipAddress, err := localip.LocalIP()
	if err != nil {
		panic(err)
	}

	d.client = riemannclient.New(d.config.RiemannHost, d.config.RiemannPort, d.config.RiemannTransport,
		d.config.MetricPrefix, d.config.Deployment, ipAddress)
}

func (d *RiemannFirehoseNozzle) consumeFirehose(authToken string) {
	d.consumer = consumer.New(
		d.config.TrafficControllerURL,
		&tls.Config{InsecureSkipVerify: d.config.InsecureSSLSkipVerify},
		nil)
	d.consumer.SetIdleTimeout(time.Duration(d.config.IdleTimeoutSeconds) * time.Second)
	d.messages, d.errs = d.consumer.Firehose(d.config.FirehoseSubscriptionID, authToken)
}

func (d *RiemannFirehoseNozzle) postToRiemann() error {
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

func (d *RiemannFirehoseNozzle) postMetrics() {
	err := d.client.PostMetrics()
	if err != nil {
		log.Println("FATAL ERROR: " + err.Error())
		// os.Exit(1)
	}
}

func (d *RiemannFirehoseNozzle) handleError(err error) {
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

func (d *RiemannFirehoseNozzle) handleMessage(envelope *events.Envelope) {
	if envelope.GetEventType() == events.Envelope_CounterEvent && envelope.CounterEvent.GetName() == "TruncatingBuffer.DroppedMessages" && envelope.GetOrigin() == "doppler" {
		log.Printf("We've intercepted an upstream message which indicates that the nozzle or the TrafficController is not keeping up. Please try scaling up the nozzle.")
		d.client.AlertSlowConsumerError()
	}
}
