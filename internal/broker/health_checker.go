package broker

import (
	"context"
	"sync/atomic"

	"github.com/omsu-informational-systems/alexey-shishin/internal/health"
)

type livenessState struct {
	recorded bool
	err      error
}

type LivenessChecker struct {
	name  string
	state atomic.Value
}

func NewLivenessChecker(name string) *LivenessChecker {
	c := &LivenessChecker{name: name}
	c.state.Store(livenessState{})
	return c
}

func (c *LivenessChecker) Name() string {
	return c.name
}

func (c *LivenessChecker) Record(err error) {
	c.state.Store(livenessState{recorded: true, err: err})
}

func (c *LivenessChecker) Check(ctx context.Context) (health.Status, string) {
	s := c.state.Load().(livenessState)
	if !s.recorded {
		return health.StatusUNKNOWN, ""
	}
	if s.err != nil {
		return health.StatusDOWN, s.err.Error()
	}
	return health.StatusUP, ""
}
