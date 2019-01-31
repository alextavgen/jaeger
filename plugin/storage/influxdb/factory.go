package influxdb

import (
	"flag"

	platform "github.com/influxdata/influxdb"
	"github.com/influxdata/influxdb/http"
	"github.com/influxdata/influxdb/snowflake"
	influxtracing "github.com/influxdata/opentracing-influxdb"
	influxSpanStore "github.com/jaegertracing/jaeger/plugin/storage/influxdb/spanstore"
	t "github.com/jaegertracing/jaeger/plugin/storage/integration"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"github.com/spf13/viper"
	"github.com/uber/jaeger-lib/metrics"
	"go.uber.org/zap"
)

// Factory implements storage.Factory and creates write-only storage components backed by kafka.
type Factory struct {
	options Options

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
		zap.Any("bucket", f.options.bucket),
		zap.Any("organization", f.options.organization))
	return nil
}

func (f *Factory) CreateSpanReader() (spanstore.Reader, error) {
	return influxSpanStore.NewSpanReader(), nil
}

func (f *Factory) CreateSpanWriter() (spanstore.Writer, error) {
	bucket, err := platform.IDFromString(f.options.bucket)
	if err != nil {
		return nil, err
	}
	org, err := platform.IDFromString(f.options.organization)
	if err != nil {
		return nil, err
	}

	writeService := http.WriteService{
		Addr:               f.options.host,
		Token:              t.options.token,
		Precision:          "ns",
		InsecureSkipVerify: false,
	}

	tracer := influxtracing.Tracer{
		OrgID:          *org,
		BucketID:       *bucket,
		IDGenerator:    snowflake.NewDefaultIDGenerator(),
		InfluxDBWriter: writeService,
	}
	return influxSpanStore.NewSpanWriter(tracer), nil
}

func (f *Factory) CreateDependencyReader() (dependencystore.Reader, error) {
	return influxSpanStore.NewSpanReader(), nil
}
