package influxdb

import (
	influxclient "github.com/influxdata/influxdb/client/v2"
	"github.com/uber/jaeger/model"
)

type Client interface {
	WriteSpans([]*model.Span) error
	QuerySpans(string, string) (*influxclient.Response, error)
}
