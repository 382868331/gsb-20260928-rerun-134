package adapter_test

import (
	"context"
	"testing"

	"github.com/example/testschedule/adapter"
)

type fixedAdapter struct {
	name  string
	cases []adapter.Case
}

func (a fixedAdapter) Name() string { return a.name }
func (a fixedAdapter) Discover(context.Context) ([]adapter.Case, error) {
	return a.cases, nil
}
func (a fixedAdapter) Execute(context.Context, adapter.Case) adapter.Result {
	return adapter.Result{Status: adapter.StatusPassed}
}

func TestLaterAdapterCanReuseStableIdentity(t *testing.T) {
	registry := adapter.NewRegistry()
	if err := registry.Register(fixedAdapter{
		name:  "local",
		cases: []adapter.Case{{Name: "old-name", Identity: "suite::same"}},
	}); err != nil {
		t.Fatal(err)
	}
	if err := registry.Register(fixedAdapter{
		name:  "remote",
		cases: []adapter.Case{{Name: "new-name", Identity: "suite::same"}},
	}); err != nil {
		t.Fatal(err)
	}

	discovered, err := registry.Discover(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(discovered) != 1 {
		t.Fatalf("discovered %d cases, want 1: %+v", len(discovered), discovered)
	}
	tc := discovered[0].Case
	if tc.ID() != "suite::same" || tc.Adapter != "remote" || tc.Name != "new-name" {
		t.Fatalf("stable identity was not rebound: %+v", tc)
	}
}
