package influxdb

import (
	"flag"

	"github.com/spf13/viper"
)

const (
	configPrefix = "influxdb."
)

type Options struct {
	host         string
	token        string
	bucket       string
	organization string
}

// AddFlags adds flags for Options
func (opt *Options) AddFlags(flagSet *flag.FlagSet) {
	flagSet.String(
		configPrefix+"host",
		"http://127.0.0.1:9999",
		"where is platform?")
	flagSet.String(
		configPrefix+"token",
		"",
		"the token to authenticate against the platform server")
	flagSet.String(
		configPrefix+"bucket",
		"jaeger",
		"database name",
	)
	flagSet.String(
		configPrefix+"organization",
		"jaeger",
		"organization name",
	)
}

// InitFromViper initializes Options with properties from viper
func (opt *Options) InitFromViper(v *viper.Viper) {
	opt.host = v.GetString(configPrefix + "host")
	opt.token = v.GetString(configPrefix + "token")
	opt.bucket = v.GetString(configPrefix + "bucket")
	opt.organization = v.GetString(configPrefix + "organization")
}
