package sleeper

import (
	"time"
)

// exponentialBackoffSleeper
type exponentialBackoffSleeper struct {
	initial       time.Duration
	sleepDuration time.Duration
}

// NewExponentialSleeper
func NewExponentialSleeper(initial time.Duration) (*exponentialBackoffSleeper, error) {
	return &exponentialBackoffSleeper{
		initial:       initial,
		sleepDuration: initial,
	}, nil
}

// Sleep
func (e *exponentialBackoffSleeper) Sleep() {
	time.Sleep(e.sleepDuration)
	e.sleepDuration += e.sleepDuration
}

// Reset
func (e *exponentialBackoffSleeper) Reset() {
	e.sleepDuration = e.initial
}
