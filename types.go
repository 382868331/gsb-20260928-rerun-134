package testsched

import (
	"context"
	"time"
)

// Outcome is the structured result of one case (or one attempt).
type Outcome string

const (
	// OutcomePassed means the case completed successfully.
	OutcomePassed Outcome = "passed"
	// OutcomeFailed means the case executed and reported failure.
	OutcomeFailed Outcome = "failed"
	// OutcomeSkipped means the case executed but deliberately skipped itself.
	OutcomeSkipped Outcome = "skipped"
	// OutcomeNotRun means the case never produced a result (canceled before
	// start, interrupted by batch cancellation, or never dispatched).
	OutcomeNotRun Outcome = "not_run"
	// OutcomeUncertain means the worker vanished while the case may still have
	// been running, so success or failure cannot be determined.
	OutcomeUncertain Outcome = "uncertain"
)

// Status is returned by case bodies to describe how an attempt ended.
type Status struct {
	Outcome Outcome
	Err     error
}

// Status helpers for case implementations.
var (
	StatusPass      = Status{Outcome: OutcomePassed}
	StatusSkip      = Status{Outcome: OutcomeSkipped}
	StatusFail      = Status{Outcome: OutcomeFailed}
	StatusUncertain = Status{Outcome: OutcomeUncertain}
)

// Fail returns a failed status carrying the given error.
func Fail(err error) Status { return Status{Outcome: OutcomeFailed, Err: err} }

// RunFunc is the executable body of a test case. The attempt context is
// canceled when the owning batch is canceled.
type RunFunc func(ctx context.Context, exec *Execution) Status

// Case is one schedulable test case. ID is the stable identity of the case:
// adapters registered later must reuse the same ID for the same case.
type Case struct {
	ID        string
	Name      string
	Adapter   string
	Resources []string
	Run       RunFunc

	// Sources records every adapter that discovered this identity. The first
	// registered executable definition wins; later adapters only confirm the
	// same identity.
	Sources []string
}

// Attempt records exactly one execution of a case. History is append-only, so
// a successful retry never erases an earlier failure or unknown result.
type Attempt struct {
	Index     int
	WorkerID  int
	Outcome   Outcome
	Err       error
	StartedAt time.Time
	EndedAt   time.Time
}

// Record is the full history and final outcome of one case in a batch.
type Record struct {
	Case     Case
	Final    Outcome
	Attempts []Attempt
}

// Counts summarizes records by their final, per-case outcome.
type Counts struct {
	Total     int
	Passed    int
	Failed    int
	Skipped   int
	NotRun    int
	Uncertain int
}

// Result is the structured output of one batch.
type Result struct {
	BatchID string
	Records []Record
}

func (r *Result) index() map[string]*Record {
	m := make(map[string]*Record, len(r.Records))
	for i := range r.Records {
		m[r.Records[i].Case.ID] = &r.Records[i]
	}
	return m
}

// RecordFor returns the record for a stable case ID.
func (r *Result) RecordFor(id string) (Record, bool) {
	for _, rec := range r.Records {
		if rec.Case.ID == id {
			return rec, true
		}
	}
	return Record{}, false
}

// Counts computes final per-case counts. Each case is counted exactly once.
func (r *Result) Counts() Counts {
	c := Counts{Total: len(r.Records)}
	for _, rec := range r.Records {
		switch rec.Final {
		case OutcomePassed:
			c.Passed++
		case OutcomeFailed:
			c.Failed++
		case OutcomeSkipped:
			c.Skipped++
		case OutcomeNotRun:
			c.NotRun++
		case OutcomeUncertain:
			c.Uncertain++
		}
	}
	return c
}

// AttemptCounts counts every attempt, including failures later retried.
func (r *Result) AttemptCounts() Counts {
	c := Counts{}
	for _, rec := range r.Records {
		for _, a := range rec.Attempts {
			c.Total++
			switch a.Outcome {
			case OutcomePassed:
				c.Passed++
			case OutcomeFailed:
				c.Failed++
			case OutcomeSkipped:
				c.Skipped++
			case OutcomeNotRun:
				c.NotRun++
			case OutcomeUncertain:
				c.Uncertain++
			}
		}
	}
	return c
}

// ByOutcome groups case records by their final outcome.
func (r *Result) ByOutcome() map[Outcome][]Record {
	m := map[Outcome][]Record{
		OutcomePassed:    nil,
		OutcomeFailed:    nil,
		OutcomeSkipped:   nil,
		OutcomeNotRun:    nil,
		OutcomeUncertain: nil,
	}
	for _, rec := range r.Records {
		m[rec.Final] = append(m[rec.Final], rec)
	}
	return m
}
