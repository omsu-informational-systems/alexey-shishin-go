package registration

import (
	"context"
	"sync/atomic"

	"github.com/omsu-informational-systems/alexey-shishin/internal/health"
)

type registrationState struct {
	status  health.Status
	details string
}

type HealthChecker struct {
	state atomic.Value
}

func NewHealthChecker() *HealthChecker {
	c := &HealthChecker{}
	c.state.Store(registrationState{status: health.StatusUNKNOWN})
	return c
}

func (c *HealthChecker) Name() string {
	return "registration"
}

func (c *HealthChecker) MarkRegistered() {
	c.state.Store(registrationState{status: health.StatusUP})
}

func (c *HealthChecker) MarkRejected(reason string) {
	c.state.Store(registrationState{status: health.StatusDOWN, details: reason})
}

func (c *HealthChecker) Check(ctx context.Context) (health.Status, string) {
	s := c.state.Load().(registrationState)
	return s.status, s.details
}
