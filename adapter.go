package testsched

import (
	"context"
	"fmt"
	"sort"
)

// Adapter discovers test cases. A local adapter ships in the first round;
// external adapters registered later reuse the same Case.ID identities.
type Adapter interface {
	Name() string
	Discover(ctx context.Context) ([]Case, error)
}

// FuncAdapter adapts a plain discovery function.
type FuncAdapter struct {
	AdapterName string
	DiscoverFn  func(ctx context.Context) ([]Case, error)
}

func (a FuncAdapter) Name() string { return a.AdapterName }

func (a FuncAdapter) Discover(ctx context.Context) ([]Case, error) {
	return a.DiscoverFn(ctx)
}

// LocalAdapter is the built-in adapter for tests shipped with the platform.
func LocalAdapter(cases []Case) Adapter {
	for i := range cases {
		if cases[i].Adapter == "" {
			cases[i].Adapter = "local"
		}
		if cases[i].Sources == nil {
			cases[i].Sources = []string{"local"}
		}
	}
	return FuncAdapter{AdapterName: "local", DiscoverFn: func(context.Context) ([]Case, error) {
		return cases, nil
	}}
}

// Registry holds adapters and performs identity-preserving discovery.
type Registry struct {
	adapters []Adapter
}

// NewRegistry creates a registry preloaded with the local adapter.
func NewRegistry(local Adapter) *Registry {
	r := &Registry{}
	if local != nil {
		r.adapters = append(r.adapters, local)
	}
	return r
}

// Register adds another adapter. Its cases are merged by Case.ID, so the same
// case reported by multiple adapters keeps one stable identity.
func (r *Registry) Register(a Adapter) {
	r.adapters = append(r.adapters, a)
}

// Adapters returns registered adapters in registration order.
func (r *Registry) Adapters() []Adapter { return r.adapters }

// Discover runs every adapter and deduplicates cases by stable ID.
func (r *Registry) Discover(ctx context.Context) ([]Case, []error) {
	byID := map[string]Case{}
	order := []string{}
	errs := []error{}
	for _, a := range r.adapters {
		found, err := a.Discover(ctx)
		if err != nil {
			errs = append(errs, fmt.Errorf("adapter %s: %w", a.Name(), err))
			continue
		}
		for _, c := range found {
			if c.ID == "" {
				errs = append(errs, fmt.Errorf("adapter %s: case with empty id", a.Name()))
				continue
			}
			if c.Adapter == "" {
				c.Adapter = a.Name()
			}
			if existing, ok := byID[c.ID]; ok {
				existing.Sources = appendUnique(existing.Sources, a.Name())
				byID[c.ID] = existing
				continue
			}
			if len(c.Sources) == 0 {
				c.Sources = []string{a.Name()}
			} else {
				c.Sources = appendUnique(c.Sources, a.Name())
			}
			byID[c.ID] = c
			order = append(order, c.ID)
		}
	}
	out := make([]Case, 0, len(order))
	for _, id := range order {
		c := byID[id]
		sort.Strings(c.Resources)
		c.Resources = dedupeSorted(c.Resources)
		out = append(out, c)
	}
	return out, errs
}

func appendUnique(xs []string, s string) []string {
	for _, x := range xs {
		if x == s {
			return xs
		}
	}
	return append(xs, s)
}

func dedupeSorted(xs []string) []string {
	if len(xs) < 2 {
		return xs
	}
	out := xs[:1]
	for _, x := range xs[1:] {
		if x != out[len(out)-1] {
			out = append(out, x)
		}
	}
	return out
}
