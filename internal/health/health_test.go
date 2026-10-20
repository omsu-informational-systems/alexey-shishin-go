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

func TestAggregateRegistrationComponentUnknownBeforeRegistered(t *testing.T) {
	r := NewRegistry(
		stubChecker{name: "registration", status: StatusUNKNOWN},
	)

	report := r.Aggregate(context.Background())

	if report.Status != StatusUP {
		t.Fatalf("expected status %s while registration is still unknown, got %s", StatusUP, report.Status)
	}
	if report.Components["registration"].Status != StatusUNKNOWN {
		t.Fatalf("expected registration component UNKNOWN, got %v", report.Components["registration"])
	}
}

func TestAggregateRegistrationComponentUpAfterRegistered(t *testing.T) {
	r := NewRegistry(
		stubChecker{name: "registration", status: StatusUP},
	)

	report := r.Aggregate(context.Background())

	if report.Components["registration"].Status != StatusUP {
		t.Fatalf("expected registration component UP, got %v", report.Components["registration"])
	}
}

func TestAggregateRegistrationComponentDownWithReasonAfterRejected(t *testing.T) {
	r := NewRegistry(
		stubChecker{name: "registration", status: StatusDOWN, details: "duplicate accountId"},
	)

	report := r.Aggregate(context.Background())

	if report.Status != StatusDEGRADED {
		t.Fatalf("expected overall status %s, got %s", StatusDEGRADED, report.Status)
	}
	component := report.Components["registration"]
	if component.Status != StatusDOWN {
		t.Fatalf("expected registration component DOWN, got %v", component)
	}
	if component.Details != "duplicate accountId" {
		t.Fatalf("expected rejection reason in details, got %q", component.Details)
	}
}

func TestAggregateBrokerCheckersAreIndependentAndDoNotMaskEachOther(t *testing.T) {
	r := NewRegistry(
		stubChecker{name: "heartbeat-producer", status: StatusUP},
		stubChecker{name: "registration-reader", status: StatusUP},
		stubChecker{name: "candle-reader", status: StatusDOWN, details: "connection refused"},
	)

	report := r.Aggregate(context.Background())

	if report.Status != StatusDEGRADED {
		t.Fatalf("expected overall status %s, got %s", StatusDEGRADED, report.Status)
	}
	if report.Components["heartbeat-producer"].Status != StatusUP {
		t.Fatalf("expected heartbeat-producer to remain UP, got %v", report.Components["heartbeat-producer"])
	}
	if report.Components["registration-reader"].Status != StatusUP {
		t.Fatalf("expected registration-reader to remain UP, got %v", report.Components["registration-reader"])
	}
	if report.Components["candle-reader"].Status != StatusDOWN {
		t.Fatalf("expected candle-reader DOWN to be visible, not masked, got %v", report.Components["candle-reader"])
	}
}
