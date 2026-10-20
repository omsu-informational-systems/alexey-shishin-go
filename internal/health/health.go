package health

import "context"

type Status string

const (
	StatusUP       Status = "UP"
	StatusDOWN     Status = "DOWN"
	StatusUNKNOWN  Status = "UNKNOWN"
	StatusDEGRADED Status = "DEGRADED"
)

type Component struct {
	Status  Status `json:"status"`
	Details string `json:"details,omitempty"`
}

type Checker interface {
	Name() string
	Check(ctx context.Context) (Status, string)
}

type Registry struct {
	checkers []Checker
}

func NewRegistry(checkers ...Checker) *Registry {
	return &Registry{checkers: checkers}
}

type Report struct {
	Status     Status
	Components map[string]Component
}

func (r *Registry) Aggregate(ctx context.Context) Report {
	components := make(map[string]Component, len(r.checkers))
	overall := StatusUP

	for _, c := range r.checkers {
		status, details := c.Check(ctx)
		components[c.Name()] = Component{Status: status, Details: details}
		if status == StatusDOWN {
			overall = StatusDEGRADED
		}
	}

	return Report{Status: overall, Components: components}
}
