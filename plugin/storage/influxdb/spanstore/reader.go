package spanstore

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

const ()

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
}

func NewSpanReader() *SpanReader {
	return &SpanReader{}
}

func (s *SpanReader) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	return &model.Trace{}, nil
}

func (s *SpanReader) GetServices(ctx context.Context) ([]string, error) {
	services := []string{}
	return services, nil
}

func (s *SpanReader) GetOperations(ctx context.Context, service string) ([]string, error) {
	names := []string{}
	return names, nil
}

type Span struct {
	*model.Span
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
	if span.SpanID == span.ParentSpanID() {
		span.ReplaceParentID(model.SpanID(0))
	} else {
		span.References = []model.SpanRef{
			model.SpanRef{
				RefType: model.ChildOf,
				TraceID: span.TraceID,
				SpanID:  span.ParentSpanID(),
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
		// TODO: zipkin uses HEX and int, we need to understand how to detect and decode both of them
		v, ok := value.(string)
		if !ok {
			return ErrIncorrectValueFormat
		}
		src := []byte(v)

		dst := make([]byte, hex.DecodedLen(len(src)))
		n, err := hex.Decode(dst, src)
		if err != nil {
			log.Fatal(err)
		}
		if err != nil {
			return err
		}
		s.SpanID = model.SpanID(n)
	case "parent_id":
		// TODO: same about HEX id as id
		v, ok := value.(string)
		if !ok {
			return ErrIncorrectValueFormat
		}
		id, err := strconv.ParseUint(v, 10, 64)
		if err != nil {
			return err
		}
		s.ReplaceParentID(model.SpanID(id))
	case "name":
		v, ok := value.(string)
		if !ok {
			return ErrIncorrectValueFormat
		}
		s.OperationName = v
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
		if span.Process == nil && e.Process != nil {
			span.Process = e.Process
		} else if span.Process != nil && e.Process != nil {
			span.Process.Tags = append(span.Process.Tags, e.Process.Tags...)
			if span.Process.ServiceName != e.Process.ServiceName {
				fmt.Printf("DIFFERent NAME %s\n", span.Process.ServiceName)
			}
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

func (s *SpanReader) FindTraces(ctx context.Context, q *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	traces := []*model.Trace{}
	return traces, nil
}

func (s *SpanReader) FindTraceIDs(ctx context.Context, q *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	return nil, nil
}

// GetDependencies loads service dependencies from influx.
func (s *SpanReader) GetDependencies(endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	deps := make([]model.DependencyLink, 0)
	return deps, nil
}
