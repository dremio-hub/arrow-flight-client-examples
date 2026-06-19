package main

type FlightConfig struct {
	Host         string
	Port         string
	Pat          string
	User         string
	Pass         string
	Query        string
	TLS          bool `docopt:"--tls"`
	Certs        string
	ProjectID    string `docopt:"--project_id"`
	TraceID      string `docopt:"--trace_id"`
	TraceSampled bool   `docopt:"--trace_sampled"`
}
