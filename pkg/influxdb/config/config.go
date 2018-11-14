package config

import (
	"errors"

	influxclient "github.com/influxdata/influxdb/client/v2"
	"github.com/jaegertracing/jaeger/pkg/influxdb"
)

/*type InfluxConnectionType int

const (
	HTTP InfluxConnectionType = iota
	UDP
)*/

type Configuration struct {
	Server         string
	Username       string
	Password       string
	Database       string
	ConnectionType string
}

func (c *Configuration) NewClient() (influxdb.Client, error) {

	// If for some reason this isn't set, use http (it should be set to http default anyway through flags)
	// But just in case
	if c.ConnectionType == "" {
		c.ConnectionType = "http"
	}

	switch c.ConnectionType {
	case "http":
		client, err := influxclient.NewHTTPClient(influxclient.HTTPConfig{
			Addr:     c.Server,
			Username: c.Username,
			Password: c.Password,
		})
		if err != nil {
			return nil, err
		}

		return &influxdb.InternalClient{
			Client: client,
		}, nil
	case "udp":
		client, err := influxclient.NewUDPClient(influxclient.UDPConfig{
			Addr: c.Server,
		})
		if err != nil {
			return nil, err
		}

		return &influxdb.InternalClient{
			Client: client,
		}, nil
	default:
		return nil, errors.New("Missing choice of client protocol")

	}
}
