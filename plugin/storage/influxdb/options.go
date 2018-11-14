package influxdb

import (
	"flag"

	"github.com/spf13/viper"
)

const (
	configPrefix = "influxdb."
)

type Options struct {
	username            string
	host                string
	password            string
	database            string
	spanMeasurementName string
	connectionType      string
}

// AddFlags adds flags for Options
func (opt *Options) AddFlags(flagSet *flag.FlagSet) {
	flagSet.String(
		configPrefix+"connection_type",
		"http",
		"http or udp")
	flagSet.String(
		configPrefix+"span_measurement_name",
		"spans",
		"the name of the measurement where spans are stored")
	flagSet.String(
		configPrefix+"username",
		"",
		"the username to authenticate against the influxdb server")
	flagSet.String(
		configPrefix+"host",
		"http://127.0.0.1:8086",
		"where is influxdb?")
	flagSet.String(
		configPrefix+"password",
		"",
		"the password to authenticate against the influxdb server")
	flagSet.String(
		configPrefix+"database",
		"jaeger",
		"database name",
	)
}

// InitFromViper initializes Options with properties from viper
func (opt *Options) InitFromViper(v *viper.Viper) {
	opt.username = v.GetString(configPrefix + "username")
	opt.host = v.GetString(configPrefix + "host")
	opt.password = v.GetString(configPrefix + "password")
	opt.database = v.GetString(configPrefix + "database")
	opt.connectionType = v.GetString(configPrefix + "connection_type")
	opt.spanMeasurementName = v.GetString(configPrefix + "span_measurement_name")
}
