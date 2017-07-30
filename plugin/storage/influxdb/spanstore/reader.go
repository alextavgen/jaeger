package spanstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	influx "github.com/influxdata/influxdb/models"
	"github.com/uber/jaeger/model"
	"github.com/uber/jaeger/pkg/influxdb"
	"github.com/uber/jaeger/pkg/influxdb/config"
	"github.com/uber/jaeger/storage/spanstore"
)

var (
	// ErrServiceNameNotSet occurs when attempting to query with an empty service name
	ErrServiceNameNotSet = errors.New("Service Name must be set")

	// ErrStartTimeMinGreaterThanMax occurs when start time min is above start time max
	ErrStartTimeMinGreaterThanMax = errors.New("Start Time Minimum is above Maximum")

	// ErrDurationMinGreaterThanMax occurs when duration min is above duration max
	ErrDurationMinGreaterThanMax = errors.New("Duration Minimum is above Maximum")

	// ErrMalformedRequestObject occurs when a request object is nil
	ErrMalformedRequestObject = errors.New("Malformed request object")

	// ErrStartAndEndTimeNotSet occurs when start time and end time are not set
	ErrStartAndEndTimeNotSet = errors.New("Start and End Time must be set")

	// ErrIncorrectValueFormat occurs when data from Influx was unexpected type.
	ErrIncorrectValueFormat = errors.New("Malformed response object")

	// ErrUnknownFields occurs when data from Influx had too many or too few fields
	ErrUnknownFields = errors.New("Unknown fields in response object")
)

type SpanReader struct {
	client influxdb.Client
	conf   *config.Configuration
}

const (
	GetTraceQueryTemplate = `SELECT * FROM "zipkin" WHERE "trace_id" = '%s'`
)

func (s *SpanReader) GetTrace(traceID model.TraceID) (*model.Trace, error) {
	query := fmt.Sprintf(GetTraceQueryTemplate, traceID.String())
	res, err := s.client.QuerySpans(query, s.conf.Database)
	if err != nil {
		return nil, err
	}
	if len(res.Results) != 1 || len(res.Results[0].Series) != 1 {
		return &model.Trace{}, nil
	}
	return NewTrace(res.Results[0].Series)
}

func NewTrace(series []influx.Row) (*model.Trace, error) {
	if len(series) == 0 {
		return &model.Trace{}, nil
	}

	trace := &model.Trace{}
	spans := make(map[model.SpanID]Spans)
	for _, row := range series {
		for _, value := range row.Values {
			s, err := NewSpan(row.Tags, row.Columns, value)
			if err != nil {
				return nil, err
			}
			spans[s.SpanID] = append(spans[s.SpanID], s)
		}
	}

	for _, s := range spans {
		trace.Spans = append(trace.Spans, s.Reduce())
	}

	return trace, nil
}

func formatTraceID(t *model.TraceID) string {
	return strconv.FormatUint(t.High, 10) + ":" + strconv.FormatUint(t.Low, 10)
}

func (s *SpanReader) GetServices() ([]string, error) {
	query := `SHOW TAG VALUES FROM "zipkin" WITH KEY = "service_name"`
	res, err := s.client.QuerySpans(query, s.conf.Database)
	if err != nil {
		return nil, err
	}
	services := []string{}
	for _, r := range res.Results {
		for _, row := range r.Series {
			for _, v := range row.Values {
				if name, ok := v[1].(string); ok {
					services = append(services, name)
				}
			}
		}
	}
	return services, nil
}

func (s *SpanReader) GetOperations(service string) ([]string, error) {
	query := fmt.Sprintf(`SHOW TAG VALUES FROM "zipkin" with key="name" WHERE "service_name" = '%s'`, service)
	res, err := s.client.QuerySpans(query, s.conf.Database)
	if err != nil {
		return nil, err
	}

	names := []string{}
	for _, r := range res.Results {
		for _, row := range r.Series {
			for _, v := range row.Values {
				if name, ok := v[1].(string); ok {
					names = append(names, name)
				}
			}
		}
	}
	return names, nil
}

func validateQuery(p *spanstore.TraceQueryParameters) error {
	if p == nil {
		return ErrMalformedRequestObject
	}
	if p.ServiceName == "" && len(p.Tags) > 0 {
		return ErrServiceNameNotSet
	}
	if p.StartTimeMin.IsZero() || p.StartTimeMax.IsZero() {
		return ErrStartAndEndTimeNotSet
	}
	if !p.StartTimeMin.IsZero() && !p.StartTimeMax.IsZero() && p.StartTimeMax.Before(p.StartTimeMin) {
		return ErrStartTimeMinGreaterThanMax
	}
	if p.DurationMin != 0 && p.DurationMax != 0 && p.DurationMin > p.DurationMax {
		return ErrDurationMinGreaterThanMax
	}

	return nil
}

type Span struct {
	*model.Span
	annotation     string
	annotation_key string
}

func NewSpan(tags map[string]string, fields []string, values []interface{}) (*Span, error) {
	if len(fields) != len(values) {
		return nil, ErrUnknownFields
	}

	span := &Span{
		Span: &model.Span{},
	}
	if op, ok := tags["name"]; ok {
		span.OperationName = op
	}

	if srv, ok := tags["service_name"]; ok {
		span.Process = &model.Process{
			ServiceName: srv,
		}
	}
	if tid, ok := tags["trace_id"]; ok {
		t, err := model.TraceIDFromString(tid)
		if err != nil {
			return nil, err
		}
		span.TraceID = t
	}

	for i := range fields {
		if err := span.AddField(fields[i], values[i]); err != nil {
			return nil, err
		}

	}
	if span.SpanID == span.ParentSpanID {
		span.ParentSpanID = model.SpanID(0)
	} else {
		span.References = []model.SpanRef{
			model.SpanRef{
				RefType: model.ChildOf,
				TraceID: span.TraceID,
				SpanID:  span.ParentSpanID,
			},
		}
	}
	return span, nil
}

func (s *Span) AddField(column string, value interface{}) error {
	if value == nil {
		return nil
	}
	switch column {
	case "time":
		n, ok := value.(json.Number)
		if !ok {
			return ErrIncorrectValueFormat
		}
		t, err := n.Int64()
		if err != nil {
			return err
		}
		s.StartTime = time.Unix(0, t)
	case "annotation":
		a, ok := value.(string)
		if !ok {
			return ErrIncorrectValueFormat
		}
		if len(s.Tags) > 0 {
			s.Tags[0].VStr = a
		} else {
			s.Tags = model.KeyValues{
				model.KeyValue{
					VStr:  a,
					VType: model.StringType,
				},
			}
		}
	case "annotation_key":
		a, ok := value.(string)
		if !ok {
			return ErrIncorrectValueFormat
		}
		if len(s.Tags) > 0 {
			s.Tags[0].Key = a
		} else {
			s.Tags = model.KeyValues{
				model.KeyValue{
					Key:   a,
					VType: model.StringType,
				},
			}
		}
	case "duration_ns":
		n, ok := value.(json.Number)
		if !ok {
			return ErrIncorrectValueFormat
		}
		d, err := n.Int64()
		if err != nil {
			return err
		}
		s.Duration = time.Duration(d)
	case "endpoint_host":
		// TODO: what is this going to be?
	case "id":
		v, ok := value.(string)
		if !ok {
			return ErrIncorrectValueFormat
		}
		id, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return err
		}
		s.SpanID = model.SpanID(id)
	case "name":
		v, ok := value.(string)
		if !ok {
			return ErrIncorrectValueFormat
		}
		s.OperationName = v
	case "parent_id":
		v, ok := value.(string)
		if !ok {
			return ErrIncorrectValueFormat
		}
		id, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return err
		}
		s.ParentSpanID = model.SpanID(id)
	case "service_name":
		v, ok := value.(string)
		if !ok {
			return ErrIncorrectValueFormat
		}
		s.Process = &model.Process{
			ServiceName: v,
		}
	case "trace_id":
		v, ok := value.(string)
		if !ok {
			return ErrIncorrectValueFormat
		}
		t, err := model.TraceIDFromString(v)
		if err != nil {
			return err
		}
		s.TraceID = t
	}
	return nil
}

func merge(span *Span, elems ...*Span) *Span {
	for _, e := range elems {
		for _, tag := range e.Tags {
			span.Tags = append(span.Tags, tag)
		}
		if len(span.References) == 0 && len(e.References) != 0 {
			span.References = append(span.References, e.References...)
		}
	}
	return span
}

type Spans []*Span

func (spans Spans) Reduce() *model.Span {
	switch len(spans) {
	case 0:
		return nil
	case 1:
		return spans[0].Span
	default:
		return merge(spans[0], spans[1:]...).Span
	}
}

func (s *SpanReader) FindTraces(q *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	if err := validateQuery(q); err != nil {
		return nil, err
	}
	fields := []string{"time", "annotation_key", "annotation", "endpoint_host", "id", "parent_id", "duration_ns"}
	query := `SELECT %s FROM "zipkin" WHERE "service_name" = '%s' AND time < %d AND time > %d `
	query = fmt.Sprintf(query, strings.Join(fields, ","), q.ServiceName, q.StartTimeMax.UTC().UnixNano(), q.StartTimeMin.UTC().UnixNano())

	if q.OperationName != "" {
		query += fmt.Sprintf(`AND "name" = '%s' `, q.OperationName)
	}
	tags := []string{}
	for k, v := range q.Tags {
		if k == "" || v == "" {
			continue
		}
		t := fmt.Sprintf(`("annotation_key" = '%s' AND "annotation" = '%s')`, k, v)
		tags = append(tags, t)
	}

	switch len(tags) {
	case 1:
		query += fmt.Sprintf(" AND %s", tags[0])
	case 2:
		query += fmt.Sprintf(" AND (%s)", strings.Join(tags, " OR "))
	}

	if q.DurationMin != 0 {
		query += fmt.Sprintf(` AND "duration" >= %d `, q.DurationMin.Nanoseconds())
	}

	if q.DurationMax != 0 {
		query += fmt.Sprintf(` AND "duration" <= %d `, q.DurationMax.Nanoseconds())
	}

	query += ` GROUP BY "service_name", "name", "trace_id" `

	if q.NumTraces > 0 {
		query += fmt.Sprintf(" SLIMIT %d", q.NumTraces)
	}

	//fmt.Printf("\n\n*** query %s***\n\n", query)
	res, err := s.client.QuerySpans(query, s.conf.Database)
	if err != nil {
		return nil, err
	}

	traces := []*model.Trace{}
	for _, result := range res.Results {
		trace, err := NewTrace(result.Series)
		if err != nil {
			return nil, err
		}
		traces = append(traces, trace)
	}
	//b, _ := json.MarshalIndent(traces, "", "    ")
	//fmt.Printf("\n\n%s\n\n", string(b))
	return traces, nil
}

func (s *SpanReader) WriteSpan(span *model.Span) error {
	return nil
}

func NewSpanReader(client influxdb.Client, conf *config.Configuration) *SpanReader {
	return &SpanReader{
		client: client,
		conf:   conf,
	}
}

/*

// GetDependencies loads service dependencies from influx.
func (s *Store) GetDependencies(endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	end := endTs.UTC().UnixNano()
	start := endTs.Add(-lookback).UTC().UnixNano()
	group := lookback.String()

	query := fmt.Sprintf(`SELECT COUNT("duration") FROM "zipkin" WHERE  time > %d AND time < %d AND annotation='' GROUP BY "id","parent_id",time(%s)`, start, end, group)
	res, err := s.client.Query(influxdb.Query{
		Command:  query,
		Database: "telegraf",
	})
	if err != nil {
		return nil, err
	}

	if err := res.Error(); err != nil {
		return nil, err
	}

	for i, result := range res.Results {
		for j, series := range result.Series {
		}
	}
	return nil, nil
}

*/
