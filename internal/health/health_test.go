package health

import (
	"context"
	"testing"
)

type stubChecker struct {
	name    string
	status  Status
	details string
}

func (s stubChecker) Name() string { return s.name }

func (s stubChecker) Check(ctx context.Context) (Status, string) {
	return s.status, s.details
}

func TestAggregateEmptyRegistryIsUP(t *testing.T) {
	r := NewRegistry()

	report := r.Aggregate(context.Background())

	if report.Status != StatusUP {
		t.Fatalf("expected status %s, got %s", StatusUP, report.Status)
	}
	if len(report.Components) != 0 {
		t.Fatalf("expected empty components, got %v", report.Components)
	}
}

func TestAggregateOneDownIsDegraded(t *testing.T) {
	r := NewRegistry(
		stubChecker{name: "database", status: StatusDOWN, details: "connection unavailable"},
	)

	report := r.Aggregate(context.Background())

	if report.Status != StatusDEGRADED {
		t.Fatalf("expected status %s, got %s", StatusDEGRADED, report.Status)
	}
	component, ok := report.Components["database"]
	if !ok {
		t.Fatalf("expected component %q in report", "database")
	}
	if component.Status != StatusDOWN {
		t.Fatalf("expected component status %s, got %s", StatusDOWN, component.Status)
	}
}

func TestAggregateUnknownDoesNotDegrade(t *testing.T) {
	r := NewRegistry(
		stubChecker{name: "cache", status: StatusUNKNOWN},
	)

	report := r.Aggregate(context.Background())

	if report.Status != StatusUP {
		t.Fatalf("expected status %s, got %s", StatusUP, report.Status)
	}
}
