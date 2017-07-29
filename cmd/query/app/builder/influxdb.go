package builder

import (
	"github.com/uber/jaeger-lib/metrics"
	"github.com/uber/jaeger/pkg/influxdb"
	"github.com/uber/jaeger/pkg/influxdb/config"
	influxstore "github.com/uber/jaeger/plugin/storage/influxdb/spanstore"
	"github.com/uber/jaeger/storage/dependencystore"
	"github.com/uber/jaeger/storage/spanstore"
	"go.uber.org/zap"
)

type influxDBStoreBuilder struct {
	store          *influxstore.SpanReader
	client         influxdb.Client
	configuration  *config.Configuration
	logger         *zap.Logger
	metricsFactory metrics.Factory
}

func newinfluxDBStoreBuilder(c *config.Configuration, logger *zap.Logger, mf metrics.Factory) *influxDBStoreBuilder {
	return &influxDBStoreBuilder{
		configuration:  c,
		logger:         logger,
		metricsFactory: mf,
	}
}

func (b *influxDBStoreBuilder) NewSpanReader() (spanstore.Reader, error) {
	//return c.store, nil
	client, err := b.getClient()
	if err != nil {
		return nil, err
	}
	return influxstore.NewSpanReader(client, b.configuration), nil

}

func (s *influxDBStoreBuilder) NewDependencyReader() (dependencystore.Reader, error) {
	return nil, nil
}

func (s *influxDBStoreBuilder) getClient() (influxdb.Client, error) {
	if s.client == nil {
		client, err := s.configuration.NewClient()
		s.client = client
		return s.client, err
	}
	return s.client, nil
}
