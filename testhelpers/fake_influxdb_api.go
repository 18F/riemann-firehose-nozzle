package testhelpers

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
)

type FakeInfluxDbAPI struct {
	server           *httptest.Server
	ReceivedContents chan []byte
}

func NewFakeInfluxDbAPI() *FakeInfluxDbAPI {
	return &FakeInfluxDbAPI{
		ReceivedContents: make(chan []byte, 100),
	}
}

func (f *FakeInfluxDbAPI) Start() {
	f.server = httptest.NewUnstartedServer(f)
	f.server.Start()
}

func (f *FakeInfluxDbAPI) Close() {
	f.server.Close()
}

func (f *FakeInfluxDbAPI) URL() string {
	return f.server.URL
}

func (f *FakeInfluxDbAPI) ServeHTTP(rw http.ResponseWriter, r *http.Request) {
	contents, _ := ioutil.ReadAll(r.Body)
	defer r.Body.Close()

	go func() {
		f.ReceivedContents <- contents
	}()
}
