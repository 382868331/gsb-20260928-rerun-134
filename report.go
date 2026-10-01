package testsched

import (
	"fmt"
	"strings"
)

// Report renders structured result data. Tests should assert Result/Counts
// directly rather than depending on this human-readable text.
func (r *Result) Report() string {
	c := r.Counts()
	var b strings.Builder
	fmt.Fprintf(&b, "batch %s: total=%d passed=%d failed=%d skipped=%d not_run=%d uncertain=%d\n",
		r.BatchID, c.Total, c.Passed, c.Failed, c.Skipped, c.NotRun, c.Uncertain)
	groups := r.ByOutcome()
	order := []Outcome{OutcomeFailed, OutcomeUncertain, OutcomeSkipped, OutcomeNotRun, OutcomePassed}
	for _, outcome := range order {
		for _, rec := range groups[outcome] {
			fmt.Fprintf(&b, "%s: %s attempts=%d\n", outcome, rec.Case.ID, len(rec.Attempts))
			for _, a := range rec.Attempts {
				line := fmt.Sprintf("  #%d worker=%d %s", a.Index, a.WorkerID, a.Outcome)
				if len(a.Reservations) > 0 {
					line += " reservations=" + strings.Join(a.Reservations, ",")
				}
				if a.Err != nil {
					line += ": " + a.Err.Error()
				}
				b.WriteString(line + "\n")
			}
		}
	}
	return b.String()
}
