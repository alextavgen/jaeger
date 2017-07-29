package influxdb

import "github.com/uber/jaeger/model"
import "github.com/influxdata/influxdb/models"

type Client interface {
	WriteSpans([]*model.Span) error
	QuerySpans(string, string) ([][]*models.Row, error)
}
