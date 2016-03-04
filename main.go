package main

import (
	"flag"
	"io"
	"log"

	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"syscall"

	"github.com/evoila/influxdb-firehose-nozzle/influxdbfirehosenozzle"
	"github.com/evoila/influxdb-firehose-nozzle/nozzleconfig"
	"github.com/evoila/influxdb-firehose-nozzle/uaatokenfetcher"
)

func main() {
	log.Print("Starting in main()")
	configFilePath := flag.String("config", "config/influxdb-firehose-nozzle.json", "Location of the nozzle config json file")
	flag.Parse()

	config, err := nozzleconfig.Parse(*configFilePath)
	if err != nil {
		log.Fatalf("Error parsing config: %s", err.Error())
	}

	tokenFetcher := &uaatokenfetcher.UAATokenFetcher{
		UaaUrl:                config.UAAURL,
		Username:              config.Username,
		Password:              config.Password,
		InsecureSSLSkipVerify: false, //config.InsecureSSLSkipVerify,
	}

	threadDumpChan := registerGoRoutineDumpSignalChannel()
	defer close(threadDumpChan)
	go dumpGoRoutine(threadDumpChan)

	go runServer()

	influxDbNozzle := influxdbfirehosenozzle.NewInfluxDbFirehoseNozzle(config, tokenFetcher)
	influxDbNozzle.Start()
}

func defaultResponse(w http.ResponseWriter, r *http.Request) {
	io.WriteString(w, "{ \"status\" : \"running\" }")
}

func runServer() {
	port := os.Getenv("PORT")

	log.Print("Go Port from environment: " + port)

	if port == "" {
		port = "8000"
	}

	log.Print("Starting server with port: " + port)

	http.HandleFunc("/", defaultResponse)
	http.ListenAndServe(":"+port, nil)
}

func registerGoRoutineDumpSignalChannel() chan os.Signal {
	threadDumpChan := make(chan os.Signal, 1)
	signal.Notify(threadDumpChan, syscall.SIGUSR1)

	return threadDumpChan
}

func dumpGoRoutine(dumpChan chan os.Signal) {
	for range dumpChan {
		goRoutineProfiles := pprof.Lookup("goroutine")
		if goRoutineProfiles != nil {
			goRoutineProfiles.WriteTo(os.Stdout, 2)
		}
	}
}
