package spanstore

import (
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/pkg/influxdb"
)

type SpanWriter struct {
	client       influxdb.Client
	databaseName string
	rp           string
}

func NewSpanWriter(client influxdb.Client, db, rp string) *SpanWriter {
	return &SpanWriter{
		client:       client,
		databaseName: db,
		rp:           rp,
	}
}

func (s *SpanWriter) WriteSpan(span *model.Span) error {
	return nil
}
