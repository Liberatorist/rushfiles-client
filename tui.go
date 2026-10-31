package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"os/exec"
	"strings"

	"github.com/charmbracelet/bubbles/textinput"
	tea "github.com/charmbracelet/bubbletea"
	"github.com/charmbracelet/lipgloss"
)

type screen int

const (
	screenMenu screen = iota
	screenLogin
	screenShares
	screenDir
)

type menuItem int

const (
	itemLogin menuItem = iota
	itemShares
	itemDir
	itemAccess
	itemAutostart
	itemRemove
	itemLogout
	itemQuit
)

var (
	titleStyle  = lipgloss.NewStyle().Bold(true)
	labelStyle  = lipgloss.NewStyle().Faint(true).Width(12)
	cursorStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("6"))
	goodStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("2"))
	warnStyle   = lipgloss.NewStyle().Foreground(lipgloss.Color("3"))
	badStyle    = lipgloss.NewStyle().Foreground(lipgloss.Color("1"))
	hintStyle   = lipgloss.NewStyle().Faint(true)
)

type accountMsg struct {
	domains []string
	shares  []*Share
	err     error
}

type loginDoneMsg struct {
	login *pendingLogin
	err   error
}

type actionDoneMsg struct {
	text string
	err  error
}

type model struct {
	screen     screen
	cursor     int
	sharesTop  int
	width      int
	height     int
	checking   bool
	busy       bool
	account    accountMsg
	autostart  autostartState
	dir        string
	rw         bool
	hasFuse    bool
	input      textinput.Model
	login      *pendingLogin
	pasted     chan string
	stopLogin  context.CancelFunc
	message    string
	messageBad bool
}

func newModel() *model {
	state := readAutostart()
	_, fuseErr := exec.LookPath("fusermount3")
	return &model{
		checking:  true,
		autostart: state,
		dir:       state.Dir,
		rw:        state.RW,
		hasFuse:   fuseErr == nil,
		width:     80,
		height:    24,
		input:     textinput.New(),
	}
}

func runSetup() error {
	// The API client logs failures, which would draw over the screen.
	log.SetOutput(io.Discard)
	_, err := tea.NewProgram(newModel(), tea.WithAltScreen()).Run()
	return err
}

func checkAccount() tea.Msg {
	if _, err := os.Stat(tokenPath()); err != nil {
		return accountMsg{}
	}
	c, err := newAPIClient(true)
	if err != nil {
		return accountMsg{err: err}
	}
	shares, err := c.shares(context.Background())
	return accountMsg{domains: c.domains, shares: shares, err: err}
}

func (m *model) Init() tea.Cmd { return checkAccount }

func (m *model) signedIn() bool { return len(m.account.domains) > 0 }

func (m *model) items() []menuItem {
	items := []menuItem{itemLogin}
	if m.signedIn() {
		items = append(items, itemShares)
	}
	items = append(items, itemDir, itemAccess, itemAutostart)
	if m.autostart.Installed {
		items = append(items, itemRemove)
	}
	if m.signedIn() {
		items = append(items, itemLogout)
	}
	return append(items, itemQuit)
}

func (m *model) itemLabel(item menuItem) string {
	switch item {
	case itemLogin:
		if m.signedIn() {
			return "Sign in again"
		}
		return "Sign in"
	case itemShares:
		return "Show shares"
	case itemDir:
		return "Mount folder: " + shortenHome(m.dir)
	case itemAccess:
		if m.rw {
			return "Access: read-write"
		}
		return "Access: read-only"
	case itemAutostart:
		if m.autostart.Installed {
			return "Apply and restart the mount"
		}
		return "Mount now and at every login"
	case itemRemove:
		return "Stop mounting and remove the service"
	case itemLogout:
		return "Sign out"
	}
	return "Quit"
}

func (m *model) say(text string, bad bool) { m.message, m.messageBad = text, bad }

func (m *model) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.WindowSizeMsg:
		m.width, m.height = msg.Width, msg.Height
		return m, nil
	case accountMsg:
		m.checking, m.account = false, msg
		return m, nil
	case loginDoneMsg:
		if msg.login != m.login {
			return m, nil // a login that was cancelled
		}
		m.endLogin()
		if msg.err != nil {
			m.say(msg.err.Error(), true)
			return m, nil
		}
		m.say("Signed in.", false)
		m.checking = true
		return m, checkAccount
	case actionDoneMsg:
		m.busy = false
		m.autostart = readAutostart()
		if msg.err != nil {
			m.say(msg.err.Error(), true)
		} else {
			m.say(msg.text, false)
		}
		m.cursor = min(m.cursor, len(m.items())-1)
		m.checking = true
		return m, checkAccount
	case tea.KeyMsg:
		if msg.Type == tea.KeyCtrlC {
			m.endLogin()
			return m, tea.Quit
		}
		if m.busy {
			return m, nil
		}
		switch m.screen {
		case screenLogin:
			return m.updateLogin(msg)
		case screenShares:
			return m.updateShares(msg)
		case screenDir:
			return m.updateDir(msg)
		}
		return m.updateMenu(msg)
	}
	if m.screen == screenLogin || m.screen == screenDir {
		var cmd tea.Cmd
		m.input, cmd = m.input.Update(msg)
		return m, cmd
	}
	return m, nil
}

func (m *model) updateMenu(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	items := m.items()
	switch msg.String() {
	case "up", "k":
		m.cursor = (m.cursor + len(items) - 1) % len(items)
	case "down", "j":
		m.cursor = (m.cursor + 1) % len(items)
	case "q", "esc":
		return m, tea.Quit
	case "enter", " ":
		m.say("", false)
		return m.choose(items[m.cursor])
	}
	return m, nil
}

func (m *model) choose(item menuItem) (tea.Model, tea.Cmd) {
	switch item {
	case itemLogin:
		return m.beginLogin()
	case itemShares:
		m.screen, m.sharesTop = screenShares, 0
	case itemDir:
		m.screen = screenDir
		m.openInput(shortenHome(m.dir), "")
		return m, textinput.Blink
	case itemAccess:
		m.rw = !m.rw
	case itemAutostart:
		return m.applyAutostart()
	case itemRemove:
		m.busy = true
		return m, func() tea.Msg {
			return actionDoneMsg{text: "Removed the service. Your login and cache are still in place.", err: uninstallAutostart()}
		}
	case itemLogout:
		m.busy = true
		return m, func() tea.Msg { return actionDoneMsg{text: "Signed out.", err: os.Remove(tokenPath())} }
	case itemQuit:
		return m, tea.Quit
	}
	return m, nil
}

func (m *model) applyAutostart() (tea.Model, tea.Cmd) {
	if !m.signedIn() {
		m.say("Sign in first.", true)
		return m, nil
	}
	cfg := autostartConfig{Dir: m.dir, RW: m.rw}
	m.busy = true
	return m, func() tea.Msg {
		return actionDoneMsg{text: "Started the drive at " + shortenHome(cfg.Dir) + ". It will also start at every login.", err: installAutostart(cfg)}
	}
}

func (m *model) openInput(value, placeholder string) {
	m.input.SetValue(value)
	m.input.Placeholder = placeholder
	m.input.CursorEnd()
	m.input.Focus()
}

func (m *model) beginLogin() (tea.Model, tea.Cmd) {
	note := ""
	if err := installHandler(); err != nil {
		note = "Could not register the rushfiles:// link handler, so paste the link below."
	}
	login, pasted := newLogin(), make(chan string, 1)
	ctx, cancel := context.WithCancel(context.Background())
	m.login, m.pasted, m.stopLogin = login, pasted, cancel
	m.screen = screenLogin
	m.openInput("", "rushfiles://signedin...")
	m.say(note, note != "")
	login.openBrowser()
	return m, tea.Batch(textinput.Blink, func() tea.Msg {
		return loginDoneMsg{login: login, err: login.finish(ctx, pasted)}
	})
}

func (m *model) endLogin() {
	if m.stopLogin != nil {
		m.stopLogin()
	}
	m.login, m.stopLogin = nil, nil
	m.input.Blur()
	if m.screen == screenLogin {
		m.screen = screenMenu
	}
}

func (m *model) updateLogin(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyEsc:
		m.endLogin()
		m.say("Sign-in cancelled.", false)
		return m, nil
	case tea.KeyEnter:
		link := strings.TrimSpace(m.input.Value())
		if !strings.HasPrefix(link, "rushfiles://") {
			m.say("That is not a rushfiles:// link.", true)
			return m, nil
		}
		m.say("", false)
		select {
		case m.pasted <- link:
		default: // already submitted, the exchange is still running
		}
		return m, nil
	}
	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	return m, cmd
}

func (m *model) updateDir(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.Type {
	case tea.KeyEsc:
		m.screen = screenMenu
		m.input.Blur()
		return m, nil
	case tea.KeyEnter:
		dir, err := expandHome(m.input.Value())
		if err != nil {
			m.say(err.Error(), true)
			return m, nil
		}
		m.dir, m.screen = dir, screenMenu
		m.input.Blur()
		m.say("", false)
		return m, nil
	}
	var cmd tea.Cmd
	m.input, cmd = m.input.Update(msg)
	return m, cmd
}

func (m *model) sharesRows() int { return max(m.height-6, 1) }

func (m *model) updateShares(msg tea.KeyMsg) (tea.Model, tea.Cmd) {
	switch msg.String() {
	case "up", "k":
		m.sharesTop = max(m.sharesTop-1, 0)
	case "down", "j":
		m.sharesTop = min(m.sharesTop+1, max(len(m.account.shares)-m.sharesRows(), 0))
	case "q", "esc", "enter":
		m.screen = screenMenu
	}
	return m, nil
}

func statusRow(label, value string) string {
	return labelStyle.Render(label) + value + "\n"
}

func (m *model) accountStatus() string {
	switch {
	case m.checking:
		return hintStyle.Render("checking...")
	case m.account.err != nil && !m.signedIn():
		return badStyle.Render("sign-in problem, try signing in again")
	case !m.signedIn():
		return warnStyle.Render("not signed in")
	}
	return goodStyle.Render("signed in") + " to " + strings.Join(m.account.domains, ", ")
}

func (m *model) autostartStatus() string {
	if !m.autostart.Installed {
		return warnStyle.Render("not set up")
	}
	mode := "read-only"
	if m.autostart.RW {
		mode = "read-write"
	}
	text := shortenHome(m.autostart.Dir) + ", " + mode
	if m.autostart.Active {
		return goodStyle.Render("running") + " at " + text
	}
	return badStyle.Render("not running") + " at " + text
}

func (m *model) View() string {
	var b strings.Builder
	b.WriteString(titleStyle.Render("RushFiles setup") + "\n\n")
	switch m.screen {
	case screenLogin:
		m.viewLogin(&b)
	case screenShares:
		m.viewShares(&b)
	case screenDir:
		b.WriteString("Where should the shares appear?\n\n" + m.input.View() + "\n\n")
		b.WriteString(hintStyle.Render("enter save  esc cancel"))
	default:
		m.viewMenu(&b)
	}
	if m.message != "" {
		style := goodStyle
		if m.messageBad {
			style = badStyle
		}
		b.WriteString("\n\n" + style.Width(max(m.width-1, 20)).Render(m.message))
	}
	return b.String()
}

func (m *model) viewMenu(b *strings.Builder) {
	b.WriteString(statusRow("Account", m.accountStatus()))
	if m.signedIn() {
		b.WriteString(statusRow("Shares", fmt.Sprint(len(m.account.shares))))
	}
	b.WriteString(statusRow("Drive", m.autostartStatus()))
	if !m.hasFuse {
		b.WriteString(statusRow("FUSE", badStyle.Render("fusermount3 is missing, install fuse3")))
	}
	b.WriteString("\n")
	for i, item := range m.items() {
		if i == m.cursor {
			b.WriteString(cursorStyle.Render("> "+m.itemLabel(item)) + "\n")
		} else {
			b.WriteString("  " + m.itemLabel(item) + "\n")
		}
	}
	if m.rw {
		b.WriteString("\n" + warnStyle.Render("Read-write has not been verified against the live server yet. Try it on a throwaway file first."))
		b.WriteString("\n")
	}
	if m.busy {
		b.WriteString("\n" + hintStyle.Render("Working..."))
	} else {
		b.WriteString("\n" + hintStyle.Render("up/down move  enter select  q quit"))
	}
}

func (m *model) viewLogin(b *strings.Builder) {
	b.WriteString("Finish signing in with your browser. If it did not open, use this link,\n")
	b.WriteString("or quit and run `rushfiles login` to get it on one line:\n\n")
	b.WriteString(hardWrap(m.login.url, max(m.width-1, 20)) + "\n\n")
	b.WriteString("If the browser does not return here, paste the final link:\n\n")
	b.WriteString(m.input.View() + "\n\n")
	b.WriteString(hintStyle.Render("enter submit  esc cancel"))
}

// hardWrap splits at exact widths: word wrapping would add spaces to a link being copied.
func hardWrap(s string, width int) string {
	var lines []string
	for len(s) > width {
		lines = append(lines, s[:width])
		s = s[width:]
	}
	return strings.Join(append(lines, s), "\n")
}

func (m *model) viewShares(b *strings.Builder) {
	shares := m.account.shares
	if m.account.err != nil {
		b.WriteString(badStyle.Render("Some shares could not be listed: "+m.account.err.Error()) + "\n\n")
	}
	end := min(m.sharesTop+m.sharesRows(), len(shares))
	for _, s := range shares[m.sharesTop:end] {
		b.WriteString(s.Name + "  " + hintStyle.Render(s.Domain) + "\n")
	}
	b.WriteString("\n" + hintStyle.Render("up/down scroll  esc back"))
}
