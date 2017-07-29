package influxdb

import (
	"flag"
	"fmt"

	"github.com/uber/jaeger/pkg/influxdb/config"
)

type Options struct {
	conf config.Configuration
}

func NewOptions() *Options {
	return &Options{}
}

func (opt *Options) Bind(flags *flag.FlagSet, namespace string) {
	flags.StringVar(&opt.conf.Username, fmt.Sprintf("%s.username", namespace), "", "InfluxDB username (if applicable)")
	flags.StringVar(&opt.conf.Password, fmt.Sprintf("%s.password", namespace), "", "InfluxDB password (if applicable)")
	flags.StringVar(&opt.conf.Server, fmt.Sprintf("%s.host", namespace), "http://localhost:8086", "InfluxDB instance hostname")
	flags.StringVar(&opt.conf.Database, fmt.Sprintf("%s.database", namespace), "jaeger", "InfluxDB database to use for storage")
	flags.StringVar(&opt.conf.ConnectionType, fmt.Sprintf("%s.connection-type", namespace), "http", "Protocol to use for communication with InfluxDB. Valid options: [http, udp]")
}

func (opt *Options) GetPrimary() *config.Configuration {
	return &opt.conf
}
