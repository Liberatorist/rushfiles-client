package main

import (
	"strings"
	"testing"

	tea "github.com/charmbracelet/bubbletea"
)

func testModel(t *testing.T) *model {
	t.Helper()
	t.Setenv("HOME", t.TempDir())
	t.Setenv("XDG_CONFIG_HOME", t.TempDir())
	return newModel()
}

func press(m *model, keys ...tea.KeyType) tea.Cmd {
	var cmd tea.Cmd
	for _, k := range keys {
		_, cmd = m.Update(tea.KeyMsg{Type: k})
	}
	return cmd
}

func moveTo(t *testing.T, m *model, want menuItem) {
	t.Helper()
	for i, item := range m.items() {
		if item == want {
			m.cursor = i
			return
		}
	}
	t.Fatalf("menu has no item %d: %v", want, m.items())
}

func TestDefaultsToReadOnly(t *testing.T) {
	m := testModel(t)
	if m.rw {
		t.Fatal("setup starts read-write")
	}
	if strings.Contains(m.View(), "Read-write has not") {
		t.Fatal("warning shown while read-only")
	}
	m.Update(accountMsg{domains: []string{"example.com"}})
	moveTo(t, m, itemAccess)
	press(m, tea.KeyEnter)
	if !m.rw || !strings.Contains(m.View(), "Read-write has not") {
		t.Fatal("switching to read-write must show the warning")
	}
}

func TestAutostartNeedsLogin(t *testing.T) {
	m := testModel(t)
	m.Update(accountMsg{})
	moveTo(t, m, itemAutostart)
	if cmd := press(m, tea.KeyEnter); cmd != nil || m.busy {
		t.Fatal("autostart ran without a login")
	}
	if !strings.Contains(m.View(), "Sign in first") {
		t.Fatal("no explanation shown")
	}
}

func TestDirEditExpandsHomeAndCancels(t *testing.T) {
	m := testModel(t)
	home := strings.TrimSuffix(m.dir, "/RushFiles")
	moveTo(t, m, itemDir)
	press(m, tea.KeyEnter)
	m.input.SetValue("~/Elsewhere")
	press(m, tea.KeyEsc)
	if m.dir != home+"/RushFiles" {
		t.Fatalf("esc changed the folder to %q", m.dir)
	}
	press(m, tea.KeyEnter)
	m.input.SetValue("~/Elsewhere")
	press(m, tea.KeyEnter)
	if m.dir != home+"/Elsewhere" || m.screen != screenMenu {
		t.Fatalf("dir %q, screen %d", m.dir, m.screen)
	}
}

func TestStaleLoginResultIgnored(t *testing.T) {
	m := testModel(t)
	m.Update(loginDoneMsg{login: &pendingLogin{}})
	if m.message != "" || m.checking != true {
		t.Fatalf("stale login changed state: %q", m.message)
	}
}

func TestSharesListHandlesShortTerminal(t *testing.T) {
	m := testModel(t)
	m.Update(tea.WindowSizeMsg{Width: 40, Height: 3})
	m.Update(accountMsg{domains: []string{"d"}, shares: []*Share{{Name: "a"}, {Name: "b"}, {Name: "c"}}})
	moveTo(t, m, itemShares)
	press(m, tea.KeyEnter, tea.KeyDown, tea.KeyDown, tea.KeyDown, tea.KeyDown)
	if !strings.Contains(m.View(), "c") {
		t.Fatal("could not scroll to the last share")
	}
}

func TestHardWrapKeepsLinkIntact(t *testing.T) {
	link := "http://a.test/x?y=1-2+3&z=" + strings.Repeat("q", 200)
	wrapped := hardWrap(link, 40)
	if strings.ReplaceAll(wrapped, "\n", "") != link {
		t.Fatalf("link changed: %q", wrapped)
	}
}

func TestRepeatedPasteDoesNotBlock(t *testing.T) {
	m := testModel(t)
	m.screen, m.login, m.pasted = screenLogin, &pendingLogin{}, make(chan string, 1)
	m.input.SetValue("rushfiles://signedin?code=1")
	press(m, tea.KeyEnter, tea.KeyEnter, tea.KeyEnter)
}
