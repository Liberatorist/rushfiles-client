package main

import (
	"strings"
	"testing"
)

func TestAutostartUnitRoundTrip(t *testing.T) {
	for _, cfg := range []autostartConfig{{Dir: "/home/a/RushFiles"}, {Dir: "/home/a/My Drive", RW: true}} {
		got, ok := parseAutostartUnit(renderAutostartUnit("/home/a/.local/bin/rushfiles", "/usr/bin/fusermount3", cfg))
		if !ok || got != cfg {
			t.Fatalf("got %+v, %v, want %+v", got, ok, cfg)
		}
	}
}

func TestReadOnlyUnitHasNoRWFlag(t *testing.T) {
	unit := renderAutostartUnit("/b", "/f", autostartConfig{Dir: "/d"})
	if strings.Contains(unit, "--rw") {
		t.Fatalf("read-only unit contains --rw:\n%s", unit)
	}
}

func TestExpandHome(t *testing.T) {
	t.Setenv("HOME", "/home/a")
	for in, want := range map[string]string{"~": "/home/a", "~/Drive": "/home/a/Drive", " /mnt/x ": "/mnt/x"} {
		if got, err := expandHome(in); err != nil || got != want {
			t.Fatalf("expandHome(%q) = %q, %v, want %q", in, got, err, want)
		}
	}
	if _, err := expandHome("  "); err == nil {
		t.Fatal("empty path was accepted")
	}
	if got := shortenHome("/home/a/Drive"); got != "~/Drive" {
		t.Fatalf("shortenHome = %q", got)
	}
	if got := shortenHome("/home/ab"); got != "/home/ab" {
		t.Fatalf("shortenHome matched a sibling folder: %q", got)
	}
}

func TestOnlyPackagedBinariesRunInPlace(t *testing.T) {
	for path, want := range map[string]bool{
		"/usr/bin/rushfiles":              true,
		"/home/a/.local/bin/rushfiles":    false,
		"/home/a/dev/rushfiles/rushfiles": false,
		"/tmp/usr/bin/rushfiles":          false,
	} {
		if got := managedBySystem(path); got != want {
			t.Errorf("%s: got %v", path, got)
		}
	}
}
