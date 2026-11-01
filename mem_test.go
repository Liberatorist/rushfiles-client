package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"testing"
	"time"
)

// bigMock approximates a real account: one share of 46,000 files in 500 folders,
// with rows carrying the same fields the live server sends.
func bigMock(t *testing.T) *mockServer {
	m := &mockServer{t: t, files: map[string]*mockFile{}, uploads: map[string]string{}}
	row := func(id, parent, name string, attrs int) {
		m.add(id, parent, name, attrs, nil)
		vf := m.files[id].vf
		vf["EndOfFile"], vf["UploadName"] = 123456, id+"v"
		vf["User"] = "0123456789abcdef0123456789abcdef"
		vf["CreationTime"], vf["LastAccessTime"] = "2024-05-01T10:00:00.1234567Z", "2024-05-01T10:00:00.1234567Z"
		vf["FileLock"] = map[string]any{"IsLocked": false, "LockTime": 0}
		for _, k := range []string{"UpdateTick", "CreateTick", "NoOfChildren", "AllocationSize", "DeleteTimestamp", "RetensionTimestamp"} {
			vf[k] = 0
		}
		for _, k := range []string{"Local", "IsOffline", "HasHistory", "InRetension"} {
			vf[k] = false
		}
		vf["Path"], vf["SubShareIds"] = "", []string{}
	}
	for d := 0; d < 500; d++ {
		dir := fmt.Sprintf("%032x", d+1)
		row(dir, testShare, fmt.Sprintf("Project folder %04d", d), 16)
		for f := 0; f < 92; f++ {
			row(fmt.Sprintf("%016x%016x", d+1, f+1), dir, fmt.Sprintf("Quarterly report %04d final version.xlsx", f), 128)
		}
	}
	return m
}

func vmRSS(pid int) int {
	data, _ := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "VmRSS:") {
			var kb int
			fmt.Sscanf(strings.TrimPrefix(line, "VmRSS:"), "%d", &kb)
			return kb / 1024
		}
	}
	return 0
}

// TestMemoryProfile prints resident memory per stage. Run it with RUSHFILES_MEM=1.
func TestMemoryProfile(t *testing.T) {
	if os.Getenv("RUSHFILES_MEM") == "" {
		t.Skip("set RUSHFILES_MEM=1 to measure")
	}
	m := bigMock(t)
	srv := httptest.NewServer(m)
	defer srv.Close()
	bin := filepath.Join(t.TempDir(), "rushfiles")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	base := t.TempDir()
	mnt := filepath.Join(base, "mnt")
	cfg := filepath.Join(base, "config", "rushfiles-linux")
	_ = os.MkdirAll(mnt, 0o755)
	_ = os.MkdirAll(cfg, 0o700)
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"user1","primary_domain":"example.test"}`))
	tok, _ := json.Marshal(tokenSet{AccessToken: "x." + payload + ".y", RefreshToken: "r", Expiry: time.Now().Add(time.Hour), DeviceID: "dev1"})
	_ = os.WriteFile(filepath.Join(cfg, "tokens.json"), tok, 0o600)

	start := func() (*exec.Cmd, *bytes.Buffer) {
		cmd := exec.Command(bin, "mount", "--sync-interval", "5s", "--trim-interval", "6s", mnt)
		cmd.Env = append(os.Environ(), "XDG_CONFIG_HOME="+filepath.Join(base, "config"), "XDG_CACHE_HOME="+filepath.Join(base, "cache"),
			"RUSHFILES_GATEWAY_URL="+srv.URL, "RUSHFILES_FILECACHE_URL="+srv.URL)
		var logs bytes.Buffer
		cmd.Stdout, cmd.Stderr = &logs, &logs
		if err := cmd.Start(); err != nil {
			t.Fatal(err)
		}
		return cmd, &logs
	}
	stop := func(cmd *exec.Cmd) {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		_ = cmd.Wait()
	}
	settle := func() { runtime.Gosched(); time.Sleep(4 * time.Second) }
	peak := func(pid int) int {
		data, _ := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
		for _, line := range strings.Split(string(data), "\n") {
			if strings.HasPrefix(line, "VmHWM:") {
				var kb int
				fmt.Sscanf(strings.TrimPrefix(line, "VmHWM:"), "%d", &kb)
				return kb / 1024
			}
		}
		return 0
	}

	cmd, logs := start()
	began := time.Now()
	for time.Since(began) < 2*time.Minute && !strings.Contains(logs.String(), "entries in") {
		time.Sleep(100 * time.Millisecond)
	}
	fmt.Printf("first sync: %s\n", time.Since(began).Round(time.Millisecond))
	settle()
	fmt.Printf("after first sync: %d MB (peak %d MB)\n", vmRSS(cmd.Process.Pid), peak(cmd.Process.Pid))
	stop(cmd)

	cmd, logs = start()
	for i := 0; i < 200 && !exists(filepath.Join(mnt, "Company")); i++ {
		time.Sleep(50 * time.Millisecond)
	}
	settle()
	fmt.Printf("after restart on saved tree: %d MB\n", vmRSS(cmd.Process.Pid))
	began = time.Now()
	walk := exec.Command("find", mnt, "-mindepth", "1")
	var found bytes.Buffer
	walk.Stdout = &found
	if err := walk.Start(); err != nil {
		t.Fatal(err)
	}
	timer := time.AfterFunc(20*time.Second, func() {
		// A stuck walk: dump the mount's goroutines and let everything go.
		_ = cmd.Process.Signal(syscall.SIGQUIT)
		time.Sleep(time.Second)
		_ = exec.Command("fusermount3", "-uz", mnt).Run()
		_ = walk.Process.Kill()
	})
	err := walk.Wait()
	if !timer.Stop() {
		t.Fatalf("walk stuck after %d bytes:\n%s", found.Len(), logs.String())
	}
	if err != nil {
		t.Fatal(err)
	}
	out := found.Bytes()
	fmt.Printf("walk: %d paths in %s\n", bytes.Count(out, []byte("\n")), time.Since(began).Round(time.Millisecond))
	settle()
	fmt.Printf("after walk: %d MB (peak %d MB)\n", vmRSS(cmd.Process.Pid), peak(cmd.Process.Pid))
	time.Sleep(10 * time.Second)
	fmt.Printf("after trim: %d MB\n", vmRSS(cmd.Process.Pid))
	stop(cmd)
}
