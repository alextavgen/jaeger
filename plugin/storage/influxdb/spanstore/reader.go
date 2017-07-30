package spanstore

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb/models"
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

	return formatIntoTrace(res), nil
}

func formatIntoTrace(rows [][]*models.Row) *model.Trace {
	var spans []*model.Span
	for _, res := range rows {
		for _, row := range res {
			fmt.Println(res, row)
		}
	}

	return &model.Trace{
		Spans: spans,
	}
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
	for _, r := range res {
		for _, row := range r {
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
	for _, r := range res {
		for _, row := range r {
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
		/*a, ok := value.(string)
		if !ok {
			return ErrIncorrectValueFormat
		}*/
		// TODO:
	case "annotation_key":
		/*a, ok := value.(string)
		if !ok {
			return ErrIncorrectValueFormat
		}*/
	// TODO:
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
		s.SpanID = model.SpanID(id)
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

func (s *SpanReader) FindTraces(q *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	if err := validateQuery(q); err != nil {
		return nil, err
	}

	query := `SELECT * FROM "zipkin" WHERE "service_name" = '%s' AND "name" = '%s' AND time < %d AND time > %d `
	query = fmt.Sprintf(query, q.ServiceName, q.OperationName, q.StartTimeMax.UTC().UnixNano(), q.StartTimeMin.UTC().UnixNano())

	tags := make([]string, len(q.Tags))
	for k, v := range q.Tags {
		t := fmt.Sprintf(`("annotation_key" = '%s' AND "annotation" = '%s')`, k, v)
		tags = append(tags, t)
	}
	if len(tags) > 0 {
		query += fmt.Sprintf(" AND (%s)", strings.Join(tags, " OR "))
	}

	if q.DurationMin != 0 {
		query += fmt.Sprintf(` AND "duration" >= %d `, q.DurationMin.Nanoseconds())
	}

	if q.DurationMax != 0 {
		query += fmt.Sprintf(` AND "duration" <= %d `, q.DurationMax.Nanoseconds())
	}

	if q.NumTraces > 0 {
		query += fmt.Sprintf(" LIMIT %d", q.NumTraces)
	}

	res, err := s.client.QuerySpans(query, s.conf.Database)
	if err != nil {
		return nil, err
	}

	trace := &model.Trace{}
	for _, r := range res {
		for _, row := range r {
			columns := map[int]string{}
			for i, c := range row.Columns {
				columns[i] = c
			}
			for _, values := range row.Values {
				s := &Span{
					Span: &model.Span{},
				}
				// TODO: each span needs to be gathered up into an existing trace and/or span
				for i, v := range values {
					column := columns[i]
					if column == "annotation" || column == "annotation_key" {
						continue
					}
					if err := s.AddField(column, v); err != nil {
						return nil, err
					}
				}
				trace.Spans = append(trace.Spans, s.Span)
			}
		}
	}
	if len(trace.Spans) > 0 {
		return []*model.Trace{trace}, nil
	}
	return []*model.Trace{}, nil
	t := &model.Trace{
		Spans: []*model.Span{
			&model.Span{
				TraceID: model.TraceID{
					Low:  uint64(0),
					High: uint64(0),
				},
				SpanID:        model.SpanID(1),
				ParentSpanID:  model.SpanID(0),
				OperationName: "operation_name",
				Process: &model.Process{
					ServiceName: "service",
				},
				References: []model.SpanRef{
					model.SpanRef{
						RefType: model.ChildOf,
						TraceID: model.TraceID{
							Low:  uint64(0),
							High: uint64(0),
						},
						SpanID: model.SpanID(0),
					},
				},
				StartTime: time.Now().UTC().Add(-time.Minute),
				Duration:  time.Second,
				Tags: model.KeyValues{
					model.KeyValue{
						Key:   "mykey",
						VType: model.StringType,
						VStr:  "myvalue",
					},
				},
			},
		},
	}
	return []*model.Trace{t}, nil
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
