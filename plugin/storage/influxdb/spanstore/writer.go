package spanstore

import (
	"github.com/jaegertracing/jaeger/model"
	opentracing "github.com/opentracing/opentracing-go"
)

type SpanWriter struct {
	tracer       opentracing.Tracer
	databaseName string
	rp           string
}

func NewSpanWriter(tracer opentracing.Tracer) *SpanWriter {
	return &SpanWriter{tracer: tracer}
}

func (s *SpanWriter) WriteSpan(span *model.Span) error {
	return nil
}
