package spanstore

import (
	"fmt"
	"strconv"
	"time"

	"github.com/influxdata/influxdb/models"
	"github.com/uber/jaeger/model"
	"github.com/uber/jaeger/pkg/influxdb"
	"github.com/uber/jaeger/pkg/influxdb/config"
	"github.com/uber/jaeger/storage/spanstore"
)

type SpanReader struct {
	client influxdb.Client
	conf   *config.Configuration
}

const (
	GetTraceQueryTemplate = `"SELECT * FROM "zipkin" WHERE "trace_id" = '%s'"`
)

func (s *SpanReader) GetTrace(traceID model.TraceID) (*model.Trace, error) {
	/*	t := &model.Trace{
		Spans: []*model.Span{
			&model.Span{
				TraceID: model.TraceID{
					Low:  uint64(0),
					High: uint64(0),
				},
				SpanID:        model.SpanID(1),
				ParentSpanID:  model.SpanID(1),
				OperationName: "operation_name",
				StartTime:     time.Now(),
				Duration:      time.Second,
				Process: &model.Process{
					ServiceName: "servname",
				},
			},
		},
	}*/

	/*

		id := formatTraceID(&traceID)
			query := fmt.Sprintf(`"SELECT * FROM "zipkin" WHERE "trace_id" = '%s'"`, id)
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

			for _, result := range res.Results {
				for _, row := range result.Series {
					row.Tags
				}
			}
			return nil, nil


	*/
	id := formatTraceID(&traceID)
	query := fmt.Sprintf(GetTraceQueryTemplate, id)

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
	return []string{"service"}, nil
}
func (s *SpanReader) GetOperations(service string) ([]string, error) {
	return []string{"operation"}, nil
}

/*
TraceID       TraceID       `json:"traceID"`
SpanID        SpanID        `json:"spanID"`
ParentSpanID  SpanID        `json:"parentSpanID"`
OperationName string        `json:"operationName"`
References    []SpanRef     `json:"references,omitempty"`
Flags         Flags         `json:"flags,omitempty"`
StartTime     time.Time     `json:"startTime"`
Duration      time.Duration `json:"duration"`
Tags          KeyValues     `json:"tags,omitempty"`
Logs          []Log         `json:"logs,omitempty"`
Process       *Process      `json:"process"`
*/
func (s *SpanReader) FindTraces(query *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	t := &model.Trace{
		Spans: []*model.Span{
			&model.Span{
				TraceID: model.TraceID{
					Low:  uint64(0),
					High: uint64(0),
				},
				SpanID:        model.SpanID(1),
				ParentSpanID:  model.SpanID(1),
				OperationName: "operation_name",
				StartTime:     time.Now(),
				Duration:      time.Second,
				Process: &model.Process{
					ServiceName: "servname",
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
