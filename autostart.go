package main

import (
	"flag"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
)

const autostartService = "rushfiles.service"

type autostartConfig struct {
	Dir string
	RW  bool
}

type autostartState struct {
	autostartConfig
	Installed, Active bool
}

func defaultMountDir() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, "RushFiles")
}

func autostartBinary() string {
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".local", "bin", "rushfiles")
}

func autostartUnitPath() string {
	dir := os.Getenv("XDG_CONFIG_HOME")
	if dir == "" {
		home, _ := os.UserHomeDir()
		dir = filepath.Join(home, ".config")
	}
	return filepath.Join(dir, "systemd", "user", autostartService)
}

func expandHome(path string) (string, error) {
	path = strings.TrimSpace(path)
	if path == "" {
		return "", fmt.Errorf("the mount folder is empty")
	}
	if path == "~" || strings.HasPrefix(path, "~/") {
		home, err := os.UserHomeDir()
		if err != nil {
			return "", err
		}
		path = filepath.Join(home, path[1:])
	}
	return filepath.Abs(path)
}

func shortenHome(path string) string {
	home, _ := os.UserHomeDir()
	if path == home || strings.HasPrefix(path, home+"/") {
		return "~" + path[len(home):]
	}
	return path
}

func renderAutostartUnit(bin, fusermount string, cfg autostartConfig) string {
	mountArgs := ""
	if cfg.RW {
		mountArgs = "--rw "
	}
	return fmt.Sprintf(`[Unit]
Description=RushFiles network drive
# Retry for as long as it takes; the network is often not up yet at login.
StartLimitIntervalSec=0

[Service]
# Clears a dead mount left behind by a crash or power loss.
ExecStartPre=-%[2]s -uz "%[4]s"
ExecStart="%[1]s" mount %[3]s"%[4]s"
ExecStopPost=-%[2]s -uz "%[4]s"
Restart=on-failure
RestartSec=15

[Install]
WantedBy=default.target
`, bin, fusermount, mountArgs, cfg.Dir)
}

var execStartLine = regexp.MustCompile(`(?m)^ExecStart="[^"]*" mount (--rw )?"(.*)"$`)

func parseAutostartUnit(unit string) (autostartConfig, bool) {
	m := execStartLine.FindStringSubmatch(unit)
	if m == nil {
		return autostartConfig{}, false
	}
	return autostartConfig{Dir: m[2], RW: m[1] != ""}, true
}

func readAutostart() autostartState {
	state := autostartState{autostartConfig: autostartConfig{Dir: defaultMountDir()}}
	unit, err := os.ReadFile(autostartUnitPath())
	if err != nil {
		return state
	}
	state.Installed = true
	if cfg, ok := parseAutostartUnit(string(unit)); ok {
		state.autostartConfig = cfg
	}
	out, _ := exec.Command("systemctl", "--user", "is-active", autostartService).Output()
	state.Active = strings.TrimSpace(string(out)) == "active"
	return state
}

func systemctl(args ...string) error {
	out, err := exec.Command("systemctl", append([]string{"--user"}, args...)...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("systemctl %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

// copyBinary replaces dst through a rename, because the service may be running it.
func copyBinary(dst string) error {
	src, err := os.Executable()
	if err != nil {
		return err
	}
	if src, err = filepath.EvalSymlinks(src); err != nil {
		return err
	}
	if resolved, err := filepath.EvalSymlinks(dst); err == nil && resolved == src {
		return nil
	}
	data, err := os.ReadFile(src)
	if err != nil {
		return err
	}
	tmp := dst + ".tmp"
	if err := os.WriteFile(tmp, data, 0o755); err != nil {
		return err
	}
	return os.Rename(tmp, dst)
}

func installAutostart(cfg autostartConfig) error {
	if _, err := loadTokens(); err != nil {
		return err
	}
	fusermount, err := exec.LookPath("fusermount3")
	if err != nil {
		return fmt.Errorf("fusermount3 not found, install fuse3")
	}
	bin, unit := autostartBinary(), autostartUnitPath()
	for _, dir := range []string{cfg.Dir, filepath.Dir(unit), filepath.Dir(bin)} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			return err
		}
	}
	if err := copyBinary(bin); err != nil {
		return err
	}
	if err := os.WriteFile(unit, []byte(renderAutostartUnit(bin, fusermount, cfg)), 0o644); err != nil {
		return err
	}
	if err := systemctl("daemon-reload"); err != nil {
		return err
	}
	if err := systemctl("enable", autostartService); err != nil {
		return err
	}
	return systemctl("restart", autostartService)
}

func uninstallAutostart() error {
	_ = systemctl("disable", "--now", autostartService)
	for _, path := range []string{autostartUnitPath(), autostartBinary()} {
		if err := os.Remove(path); err != nil && !os.IsNotExist(err) {
			return err
		}
	}
	return systemctl("daemon-reload")
}

func autostartCommand(args []string) error {
	flags := flag.NewFlagSet("autostart", flag.ContinueOnError)
	rw := flags.Bool("rw", false, "allow changes to be written to the server")
	dir := flags.String("dir", defaultMountDir(), "where to mount")
	uninstall := flags.Bool("uninstall", false, "stop and remove the service")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if *uninstall {
		if err := uninstallAutostart(); err != nil {
			return err
		}
		fmt.Println("Removed the service. Your login and cache are still in place.")
		return nil
	}
	path, err := expandHome(*dir)
	if err != nil {
		return err
	}
	if err := installAutostart(autostartConfig{Dir: path, RW: *rw}); err != nil {
		return err
	}
	fmt.Printf("RushFiles will mount at %s on login. Status: systemctl --user status rushfiles\n", path)
	return nil
}
