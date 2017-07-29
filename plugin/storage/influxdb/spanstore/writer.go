package spanstore

import "github.com/uber/jaeger/model"

type SpanWriter struct {
}

func (s *SpanWriter) WriteSpan(span *model.Span) error {
	return nil
}
