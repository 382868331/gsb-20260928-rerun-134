package adapter

import (
	"context"
	"fmt"
	"sort"
)

type LocalFunc func(context.Context) Result

type LocalCatalog struct {
	name  string
	tests map[string]localTest
}

type localTest struct {
	fn         LocalFunc
	resources  []string
	skip       bool
	skipReason string
}

func NewLocalCatalog(name string) *LocalCatalog {
	if name == "" {
		name = "local"
	}
	return &LocalCatalog{name: name, tests: make(map[string]localTest)}
}

func (c *LocalCatalog) Name() string { return c.name }

func (c *LocalCatalog) Add(name string, resources []string, fn LocalFunc) error {
	if _, exists := c.tests[name]; exists {
		return fmt.Errorf("local test %q already exists", name)
	}
	c.tests[name] = localTest{fn: fn, resources: append([]string(nil), resources...)}
	return nil
}

func (c *LocalCatalog) AddSkipped(name string, reason string, resources []string) error {
	if _, exists := c.tests[name]; exists {
		return fmt.Errorf("local test %q already exists", name)
	}
	c.tests[name] = localTest{resources: append([]string(nil), resources...), skip: true, skipReason: reason}
	return nil
}

func (c *LocalCatalog) Discover(context.Context) ([]Case, error) {
	names := make([]string, 0, len(c.tests))
	for name := range c.tests {
		names = append(names, name)
	}
	sort.Strings(names)
	cases := make([]Case, 0, len(names))
	for _, name := range names {
		test := c.tests[name]
		cases = append(cases, Case{
			Adapter:    c.name,
			Name:       name,
			Resources:  append([]string(nil), test.resources...),
			Skip:       test.skip,
			SkipReason: test.skipReason,
		})
	}
	return cases, nil
}

func (c *LocalCatalog) Execute(ctx context.Context, tc Case) Result {
	test, ok := c.tests[tc.Name]
	if !ok {
		return Result{Status: StatusFailed, Err: fmt.Errorf("unknown local test %q", tc.Name)}
	}
	if test.skip {
		return Result{Status: StatusSkipped, Output: test.skipReason}
	}
	return test.fn(ctx)
}
