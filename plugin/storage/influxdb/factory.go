package influxdb

import (
	"flag"

	client "github.com/influxdata/influxdb/client/v2"
	"github.com/jaegertracing/jaeger/pkg/influxdb"
	influxSpanStore "github.com/jaegertracing/jaeger/plugin/storage/influxdb/spanstore"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/spf13/viper"
	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"
)

// Factory implements storage.Factory and creates write-only storage components backed by kafka.
type Factory struct {
	options Options

	influxdbClient influxdb.Client
	metricsFactory metrics.Factory
	logger         *zap.Logger
}

func NewFactory() *Factory {
	return &Factory{}
}

// AddFlags implements plugin.Configurable
func (f *Factory) AddFlags(flagSet *flag.FlagSet) {
	f.options.AddFlags(flagSet)
}

// InitFromViper implements plugin.Configurable
func (f *Factory) InitFromViper(v *viper.Viper) {
	f.options.InitFromViper(v)
}

func (f *Factory) Initialize(metricsFactory metrics.Factory, logger *zap.Logger) error {
	f.metricsFactory, f.logger = metricsFactory, logger
	logger.Info("InfluxDB factory",
		zap.Any("span measurement name", f.options.spanMeasurementName),
		zap.Any("database", f.options.database))

	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     f.options.host,
		Username: f.options.username,
		Password: f.options.password,
	})
	if err != nil {
		return err
	}
	f.influxdbClient = &influxdb.InternalClient{Client: c}
	return nil
}

func (f *Factory) CreateSpanReader() (spanstore.Reader, error) {
	return influxSpanStore.NewSpanReader(f.influxdbClient, f.options.database, "autogen", f.options.spanMeasurementName), nil
}

func (f *Factory) CreateSpanWriter() (spanstore.Writer, error) {
	return &influxSpanStore.SpanWriter{}, nil
}

func (f *Factory) CreateDependencyReader() (dependencystore.Reader, error) {
	return influxSpanStore.NewSpanReader(f.influxdbClient, f.options.database, "autogen", f.options.spanMeasurementName), nil
}
