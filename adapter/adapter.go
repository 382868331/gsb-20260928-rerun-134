package adapter

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

type Status string

const (
	StatusPassed      Status = "passed"
	StatusFailed      Status = "failed"
	StatusSkipped     Status = "skipped"
	StatusNotExecuted Status = "not_executed"
	StatusUncertain   Status = "uncertain"
)

// Case is the scheduler's stable view of a test. The same logical test must
// keep Adapter and Name unchanged when it is discovered by a later adapter.
type Case struct {
	Identity   string
	Adapter    string
	Name       string
	Resources  []string
	Skip       bool
	SkipReason string
}

func (c Case) ID() string {
	if c.Identity != "" {
		return c.Identity
	}
	return c.Adapter + "::" + c.Name
}

func (c Case) SortedResources() []string {
	resources := append([]string(nil), c.Resources...)
	sort.Strings(resources)
	return resources
}

type Result struct {
	Status Status
	Output string
	Err    error
}

type Adapter interface {
	Name() string
	Discover(context.Context) ([]Case, error)
	Execute(context.Context, Case) Result
}

type Registry struct {
	adapters map[string]Adapter
}

func NewRegistry() *Registry {
	return &Registry{adapters: make(map[string]Adapter)}
}

func (r *Registry) Register(a Adapter) error {
	name := a.Name()
	if name == "" {
		return fmt.Errorf("adapter name is required")
	}
	if _, exists := r.adapters[name]; exists {
		return fmt.Errorf("adapter %q already registered", name)
	}
	r.adapters[name] = a
	return nil
}

func (r *Registry) Get(name string) (Adapter, bool) {
	a, ok := r.adapters[name]
	return a, ok
}

type DiscoveredCase struct {
	Case
	DiscoverError error
}

func (r *Registry) Discover(ctx context.Context) ([]DiscoveredCase, error) {
	names := make([]string, 0, len(r.adapters))
	for name := range r.adapters {
		names = append(names, name)
	}
	sort.Strings(names)

	var discovered []DiscoveredCase
	var discoveryErrors []string
	seen := make(map[string]string)
	discoveredIndex := make(map[string]int)
	for _, name := range names {
		cases, err := r.adapters[name].Discover(ctx)
		if err != nil {
			discoveryErrors = append(discoveryErrors, name+": "+err.Error())
			continue
		}
		for _, tc := range cases {
			tc.Adapter = name
			id := tc.ID()
			if tc.Identity != "" {
				id = tc.Identity
			}
			if previous, ok := seen[id]; ok {
				if previous == name {
					discoveryErrors = append(discoveryErrors, fmt.Sprintf("duplicate case %s from %s", id, previous))
					continue
				}
			}
			tc.Identity = id
			if index, exists := discoveredIndex[id]; exists {
				discovered[index].Case = tc
			} else {
				discoveredIndex[id] = len(discovered)
				discovered = append(discovered, DiscoveredCase{Case: tc})
			}
			seen[id] = name
		}
	}
	if len(discoveryErrors) > 0 {
		return discovered, fmt.Errorf("%s", strings.Join(discoveryErrors, "; "))
	}
	return discovered, nil
}
