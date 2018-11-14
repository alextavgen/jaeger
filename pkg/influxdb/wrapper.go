package influxdb

import (
	influxclient "github.com/influxdata/influxdb/client/v2"
	"github.com/jaegertracing/jaeger/model"
)

type InternalClient struct {
	Client influxclient.Client
}

func (c *InternalClient) WriteSpans(spans []*model.Span) error {
	return nil
}

func (c *InternalClient) QuerySpans(query string, database string) (*influxclient.Response, error) {
	return c.Client.Query(influxclient.NewQuery(query, database, "ns"))
	/*
		res, err := c.Client.Query(influxclient.NewQuery(query, database, "ns"))
		if err != nil {
			return nil, err
		}

		if err = res.Error(); err != nil {
			return nil, err
		}

		var ret [][]*models.Row

		for _, r := range res.Results {
			models := []*models.Row{}
			for _, series := range r.Series {
				models = append(models, &series)
			}
			ret = append(ret, models)
		}

		return ret, nil*/
}
