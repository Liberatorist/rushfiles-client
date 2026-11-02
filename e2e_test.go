package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

const testShare = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

type mockFile struct {
	vf      map[string]any
	content []byte
}

// mockServer implements the gateway and file cache as decoded from the Windows client.
type mockServer struct {
	mu      sync.Mutex
	t       *testing.T
	files   map[string]*mockFile
	next    int
	tick    int
	gone    []map[string]any // tombstones
	mutated []string
	uploads map[string]string
}

func newMock(t *testing.T) *mockServer {
	m := &mockServer{t: t, files: map[string]*mockFile{}, uploads: map[string]string{}}
	m.add("f1", testShare, "readme.txt", 128, []byte("hello from the server\n"))
	m.add("d1", testShare, "Docs", 16, nil)
	m.add("f2", "d1", "nested.txt", 128, []byte("nested\n"))
	return m
}

// bump records a change the way the server does, by moving the row to a new tick.
func (m *mockServer) bump(id string) {
	m.tick++
	m.files[id].vf["Tick"] = m.tick
}

func (m *mockServer) add(id, parent, name string, attrs int, content []byte) {
	m.files[id] = &mockFile{content: content, vf: map[string]any{
		"InternalName": id, "ParrentId": parent, "PublicName": name, "ShareId": testShare,
		"Attributes": attrs, "IsFile": attrs != 16, "EndOfFile": len(content), "Tick": 1, "ShareTick": 1,
		"LastWriteTime": "2024-05-01T10:00:00", "FilehHash": "h1", "Deleted": false, "UploadName": "v-" + id,
	}}
	m.bump(id)
}

func (m *mockServer) catchup(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	if q.Get("token") != "acl-"+testShare {
		http.Error(w, "bad acl token", 403)
		return
	}
	from, _ := strconv.Atoi(q.Get("tickFrom"))
	take, _ := strconv.Atoi(q.Get("tickTake"))
	rows := []map[string]any{}
	all := m.gone
	for _, f := range m.files {
		all = append(all, f.vf)
	}
	sort.Slice(all, func(i, j int) bool { return all[i]["Tick"].(int) < all[j]["Tick"].(int) })
	largest := 0
	for _, vf := range all {
		if vf["Tick"].(int) > from && len(rows) < take {
			rows = append(rows, vf)
			largest = vf["Tick"].(int)
		}
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"RfVirtualFiles": rows, "ShareTick": m.tick, "LargestTick": largest})
}

func (m *mockServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if !strings.HasPrefix(r.Header.Get("Authorization"), "Bearer ") {
		http.Error(w, "no auth", 401)
		return
	}
	write := func(v any) { _ = json.NewEncoder(w).Encode(v) }
	path := r.URL.Path
	switch {
	case regexp.MustCompile(`^/api/users/[^/]+/shares$`).MatchString(path):
		write(map[string]any{"Data": []map[string]any{{"Id": testShare, "Name": "Company", "ShareType": 0}}})
	case path == "/api/client/fullprofile":
		write(map[string]any{"Data": map[string]any{"ManagedShares": []map[string]any{
			{"ShareId": testShare, "ShareAcl": map[string]any{"AclToken": "acl-" + testShare}},
		}}})
	case path == "/api/client/catchup":
		m.catchup(w, r)
	case strings.HasSuffix(path, "/children"):
		parent := strings.Split(path, "/")[5]
		out := []map[string]any{}
		for _, f := range m.files {
			if f.vf["ParrentId"] == parent {
				out = append(out, f.vf)
			}
		}
		write(map[string]any{"Data": out})
	case strings.Contains(path, "/virtualFiles/"):
		id := path[strings.LastIndex(path, "/")+1:]
		f := m.files[id]
		if f == nil {
			// The live server keeps answering for deleted files.
			for _, row := range m.gone {
				if row["InternalName"] == id {
					write(map[string]any{"Data": row})
					return
				}
			}
			http.NotFound(w, r)
			return
		}
		write(map[string]any{"Data": f.vf})
	case strings.HasPrefix(path, "/upload/"):
		m.chunk(w, r)
	case strings.Contains(path, "/files"):
		m.files_(w, r)
	default:
		m.t.Errorf("unexpected request %s %s", r.Method, r.URL)
		http.NotFound(w, r)
	}
}

func (m *mockServer) chunk(w http.ResponseWriter, r *http.Request) {
	id := strings.TrimPrefix(r.URL.Path, "/upload/")
	var start, end, total int
	if _, err := fmt.Sscanf(r.Header.Get("Content-Range"), "bytes %d-%d/%d", &start, &end, &total); err != nil {
		m.t.Errorf("bad Content-Range %q", r.Header.Get("Content-Range"))
	}
	body, _ := io.ReadAll(r.Body)
	if r.Header.Get("Content-Type") != "application/octet-stream" || len(body) != end-start+1 || len(body) > uploadChunkSize {
		m.t.Errorf("bad chunk: type=%s len=%d range=%s", r.Header.Get("Content-Type"), len(body), r.Header.Get("Content-Range"))
	}
	f := m.files[id]
	if start == 0 {
		f.content = nil
	}
	f.content = append(f.content, body...)
	f.vf["EndOfFile"] = len(f.content)
	f.vf["FilehHash"] = fmt.Sprintf("h%d", time.Now().UnixNano())
	f.vf["UploadName"] = fmt.Sprintf("v%d", time.Now().UnixNano())
	m.bump(id)
	_ = json.NewEncoder(w).Encode(map[string]any{"Data": map[string]any{"ClientJournalEvent": map[string]any{"RfVirtualFile": f.vf}}})
}

func (m *mockServer) files_(w http.ResponseWriter, r *http.Request) {
	id := ""
	if parts := strings.Split(r.URL.Path, "/"); len(parts) == 6 {
		id = parts[5]
	}
	if r.Method == http.MethodGet {
		for _, f := range m.files {
			if f.vf["UploadName"] == id {
				_, _ = w.Write(f.content)
				return
			}
		}
		http.NotFound(w, r) // the live server rejects InternalName here
		return
	}
	var j struct {
		ClientJournalEventType *int
		DeviceId, TransmitId   string
		RfVirtualFile          map[string]any
	}
	if err := json.NewDecoder(r.Body).Decode(&j); err != nil || j.ClientJournalEventType == nil || j.DeviceId == "" || j.TransmitId == "" || j.RfVirtualFile == nil {
		m.t.Errorf("malformed journal %s %s: %v %+v", r.Method, r.URL, err, j)
		return
	}
	event, vf := *j.ClientJournalEventType, j.RfVirtualFile
	if vf["ShareId"] != testShare {
		m.t.Errorf("journal without ShareId: %+v", vf)
	}
	m.mutated = append(m.mutated, fmt.Sprintf("%s:%d:%v", r.Method, event, vf["PublicName"]))
	resp := map[string]any{}
	switch {
	case r.Method == http.MethodPost && event == eventCreate:
		m.next++
		id = "new" + strconv.Itoa(m.next)
		m.add(id, vf["ParrentId"].(string), vf["PublicName"].(string), int(vf["Attributes"].(float64)), nil)
		resp["ClientJournalEvent"] = map[string]any{"RfVirtualFile": m.files[id].vf}
		if vf["EndOfFile"].(float64) > 0 {
			resp["Url"] = "/upload/" + id
		}
	case r.Method == http.MethodPut && event == eventUpdate:
		if vf["EndOfFile"].(float64) > 0 {
			resp["Url"] = "/upload/" + id
		} else {
			m.files[id].content, m.files[id].vf["EndOfFile"] = nil, 0
			m.bump(id)
		}
		resp["ClientJournalEvent"] = map[string]any{"RfVirtualFile": m.files[id].vf}
	case r.Method == http.MethodPut && event == eventRename:
		m.files[id].vf["PublicName"] = vf["PublicName"]
		m.bump(id)
	case r.Method == http.MethodPut && event == eventMove:
		m.files[id].vf["ParrentId"] = vf["ParrentId"]
		m.bump(id)
	case r.Method == http.MethodDelete && event == eventDelete:
		m.tombstone(id)
	default:
		m.t.Errorf("wrong verb for event: %s %d", r.Method, event)
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"Data": resp, "Code": 1})
}

// tombstone deletes a file, or a folder with everything in it.
func (m *mockServer) tombstone(id string) {
	for kid, f := range m.files {
		if f.vf["ParrentId"] == id {
			m.tombstone(kid)
		}
	}
	row := m.files[id].vf
	delete(m.files, id)
	m.tick++
	row["Deleted"], row["Tick"] = true, m.tick
	m.gone = append(m.gone, row)
}

func (m *mockServer) events() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := strings.Join(m.mutated, " ")
	m.mutated = nil
	return out
}

func (m *mockServer) content(name string) []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, f := range m.files {
		if f.vf["PublicName"] == name {
			return f.content
		}
	}
	return nil
}

func (m *mockServer) names() string {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []string
	for _, f := range m.files {
		out = append(out, fmt.Sprint(f.vf["PublicName"]))
	}
	return strings.Join(out, ",")
}

func startMount(t *testing.T, srv *httptest.Server, args ...string) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "rushfiles")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	return mountAt(t, srv, bin, t.TempDir(), args...)
}

// mountAt mounts with the state in base, so a test can restart on the same cache.
func mountAt(t *testing.T, srv *httptest.Server, bin, base string, args ...string) string {
	t.Helper()
	mnt := filepath.Join(base, "mnt")
	_ = os.Mkdir(mnt, 0o755)
	payload := base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"user1","primary_domain":"example.test"}`))
	cfg := filepath.Join(base, "config", "rushfiles-linux")
	_ = os.MkdirAll(cfg, 0o700)
	tok, _ := json.Marshal(tokenSet{AccessToken: "x." + payload + ".y", RefreshToken: "r", Expiry: time.Now().Add(time.Hour), DeviceID: "dev1"})
	_ = os.WriteFile(filepath.Join(cfg, "tokens.json"), tok, 0o600)
	cmd := exec.Command(bin, append(append([]string{"mount"}, args...), mnt)...)
	cmd.Env = append(os.Environ(), "XDG_CONFIG_HOME="+filepath.Join(base, "config"), "XDG_CACHE_HOME="+filepath.Join(base, "cache"),
		"RUSHFILES_GATEWAY_URL="+srv.URL, "RUSHFILES_FILECACHE_URL="+srv.URL)
	var logs bytes.Buffer
	cmd.Stdout, cmd.Stderr = &logs, &logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = cmd.Process.Signal(syscall.SIGTERM)
		done := make(chan struct{})
		go func() { _ = cmd.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = exec.Command("fusermount3", "-uz", mnt).Run()
			_ = cmd.Process.Kill()
		}
		t.Log(logs.String())
	})
	for i := 0; i < 100; i++ {
		if _, err := os.Stat(filepath.Join(mnt, "Company")); err == nil {
			return filepath.Join(mnt, "Company")
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("mount did not come up:\n%s", logs.String())
	return ""
}

func TestReadOnlyMount(t *testing.T) {
	m := newMock(t)
	srv := httptest.NewServer(m)
	defer srv.Close()
	dir := startMount(t, srv)

	got, err := os.ReadFile(filepath.Join(dir, "Docs", "nested.txt"))
	if err != nil || string(got) != "nested\n" {
		t.Fatalf("read: %q %v", got, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 2 {
		t.Fatalf("readdir: %v %v", entries, err)
	}
	st, err := os.Stat(filepath.Join(dir, "readme.txt"))
	if err != nil || st.Size() != 22 {
		t.Fatalf("stat: %v %v", st, err)
	}
	if uint32(st.Sys().(*syscall.Stat_t).Uid) != uint32(os.Getuid()) {
		t.Fatalf("wrong owner %d", st.Sys().(*syscall.Stat_t).Uid)
	}
	for name, try := range map[string]func() error{
		"write":  func() error { return os.WriteFile(filepath.Join(dir, "x"), []byte("x"), 0o644) },
		"edit":   func() error { return os.WriteFile(filepath.Join(dir, "readme.txt"), []byte("x"), 0o644) },
		"delete": func() error { return os.Remove(filepath.Join(dir, "readme.txt")) },
		"mkdir":  func() error { return os.Mkdir(filepath.Join(dir, "d"), 0o755) },
		"rename": func() error { return os.Rename(filepath.Join(dir, "readme.txt"), filepath.Join(dir, "y")) },
	} {
		if err := try(); err == nil {
			t.Errorf("%s succeeded on a read-only mount", name)
		}
	}
	if ev := m.events(); ev != "" {
		t.Errorf("read-only mount sent mutations: %s", ev)
	}
}

func TestReadWriteMount(t *testing.T) {
	m := newMock(t)
	srv := httptest.NewServer(m)
	defer srv.Close()
	dir := startMount(t, srv, "--rw", "--list-ttl", "1ms")

	// Create, spanning several upload chunks.
	big := bytes.Repeat([]byte("0123456789abcdef"), 2*1024*1024) // 32 MiB
	if err := os.WriteFile(filepath.Join(dir, "big.bin"), big, 0o644); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "big upload", func() bool { return bytes.Equal(m.content("big.bin"), big) })
	if ev := m.events(); ev != "POST:0:big.bin PUT:3:big.bin" {
		t.Errorf("create events: %s", ev)
	}

	// Shell-style overwrite must publish one version, never an empty one.
	sh := exec.Command("sh", "-c", "echo edited > "+filepath.Join(dir, "readme.txt"))
	if out, err := sh.CombinedOutput(); err != nil {
		t.Fatalf("%v %s", err, out)
	}
	waitContent(t, m, "readme.txt", "edited\n")
	time.Sleep(truncateGrace + 200*time.Millisecond)
	if ev := m.events(); ev != "PUT:3:readme.txt" {
		t.Errorf("overwrite events: %s", ev)
	}

	// Appending keeps the existing bytes, and editing after a rename is no conflict.
	if err := os.Rename(filepath.Join(dir, "readme.txt"), filepath.Join(dir, "renamed.txt")); err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(filepath.Join(dir, "renamed.txt"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = f.WriteString("more\n")
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	waitContent(t, m, "renamed.txt", "edited\nmore\n")
	if strings.Contains(m.names(), "conflict") {
		t.Errorf("false conflict after rename: %s", m.names())
	}
	m.events()

	// Folders, moves, and rmdir's protection of non-empty folders.
	sub := filepath.Join(dir, "New Folder")
	if err := os.Mkdir(sub, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(dir, "renamed.txt"), filepath.Join(sub, "moved.txt")); err != nil {
		t.Fatal(err)
	}
	if ev := m.events(); ev != "POST:0:New Folder PUT:16:renamed.txt PUT:2:moved.txt" {
		t.Errorf("move events: %s", ev)
	}
	if err := os.Remove(sub); err == nil || !strings.Contains(err.Error(), "not empty") {
		t.Fatalf("rmdir of non-empty folder: %v", err)
	}
	if ev := m.events(); ev != "" {
		t.Errorf("rmdir reached the server: %s", ev)
	}

	// Editor-style atomic save over an existing file, renamed before its upload ran.
	if err := os.WriteFile(filepath.Join(sub, ".tmp"), []byte("saved"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(sub, ".tmp"), filepath.Join(sub, "moved.txt")); err != nil {
		t.Fatal(err)
	}
	if got, err := os.ReadFile(filepath.Join(sub, "moved.txt")); err != nil || string(got) != "saved" {
		t.Fatalf("read back before upload: %q %v", got, err)
	}
	waitContent(t, m, "moved.txt", "saved")
	if strings.Contains(m.names(), ".tmp") {
		t.Errorf("upload undid the rename: %s", m.names())
	}
	if err := os.Remove(filepath.Join(sub, "moved.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(sub); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "deletes", func() bool {
		return !strings.Contains(m.names(), "New Folder") && !strings.Contains(m.names(), "moved.txt")
	})
}

func TestConflictKeepsBothVersions(t *testing.T) {
	m := newMock(t)
	srv := httptest.NewServer(m)
	defer srv.Close()
	dir := startMount(t, srv, "--rw", "--list-ttl", "1ms")

	f, err := os.OpenFile(filepath.Join(dir, "readme.txt"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	m.files["f1"].content = []byte("someone else\n")
	m.files["f1"].vf["EndOfFile"], m.files["f1"].vf["FilehHash"], m.files["f1"].vf["UploadName"] = 13, "other", "v-other"
	m.mu.Unlock()
	_, _ = f.WriteString("mine\n")
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "conflict copy", func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		for _, v := range m.files {
			if strings.Contains(fmt.Sprint(v.vf["PublicName"]), "conflict") && strings.HasSuffix(string(v.content), "mine\n") {
				return true
			}
		}
		return false
	})
	if got := string(m.content("readme.txt")); got != "someone else\n" {
		t.Fatalf("server version overwritten: %q", got)
	}
	got, err := os.ReadFile(filepath.Join(dir, "readme.txt"))
	if err != nil || string(got) != "someone else\n" {
		t.Fatalf("cache served the wrong version: %q %v", got, err)
	}
}

// mountStoppable mounts and returns a function that unmounts and waits for exit.
func mountStoppable(t *testing.T, srv *httptest.Server, bin, base string, args ...string) (string, func()) {
	t.Helper()
	mnt := filepath.Join(base, "mnt")
	_ = os.MkdirAll(mnt, 0o755)
	cfg := filepath.Join(base, "config", "rushfiles-linux")
	_ = os.MkdirAll(cfg, 0o700)
	if !exists(filepath.Join(cfg, "tokens.json")) {
		payload := base64.RawURLEncoding.EncodeToString([]byte(`{"sub":"user1","primary_domain":"example.test"}`))
		tok, _ := json.Marshal(tokenSet{AccessToken: "x." + payload + ".y", RefreshToken: "r", Expiry: time.Now().Add(time.Hour), DeviceID: "dev1"})
		_ = os.WriteFile(filepath.Join(cfg, "tokens.json"), tok, 0o600)
	}
	cmd := exec.Command(bin, append(append([]string{"mount", "--sync-interval", "50ms"}, args...), mnt)...)
	cmd.Env = append(os.Environ(), "XDG_CONFIG_HOME="+filepath.Join(base, "config"), "XDG_CACHE_HOME="+filepath.Join(base, "cache"),
		"RUSHFILES_GATEWAY_URL="+srv.URL, "RUSHFILES_FILECACHE_URL="+srv.URL)
	var logs bytes.Buffer
	cmd.Stdout, cmd.Stderr = &logs, &logs
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	stopped := false
	stop := func() {
		if !stopped {
			stopped = true
			_ = cmd.Process.Signal(syscall.SIGTERM)
			_ = cmd.Wait()
			t.Log(logs.String())
		}
	}
	t.Cleanup(stop)
	waitFor(t, "mount", func() bool { return exists(filepath.Join(mnt, "Company")) })
	return filepath.Join(mnt, "Company"), stop
}

func waitContent(t *testing.T, m *mockServer, name, want string) {
	t.Helper()
	waitFor(t, name+" upload", func() bool { return string(m.content(name)) == want })
}

func waitFor(t *testing.T, what string, ok func() bool) {
	t.Helper()
	for i := 0; i < 200; i++ {
		if ok() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func TestTreeFollowsRemoteChanges(t *testing.T) {
	m := newMock(t)
	srv := httptest.NewServer(m)
	defer srv.Close()
	dir := startMount(t, srv, "--sync-interval", "50ms", "--list-ttl", "1h")

	remote := func(change func()) {
		m.mu.Lock()
		defer m.mu.Unlock()
		change()
	}
	waitFor(t, "initial tree", func() bool { return exists(filepath.Join(dir, "Docs", "nested.txt")) })

	remote(func() { m.add("r1", "d1", "added.txt", 128, []byte("added")) })
	// Entries the kernel has not cached show up as soon as the tree has them.
	waitFor(t, "remote add", func() bool { return exists(filepath.Join(dir, "Docs", "added.txt")) })
	if got, err := os.ReadFile(filepath.Join(dir, "Docs", "added.txt")); err != nil || string(got) != "added" {
		t.Fatalf("read remote add: %q %v", got, err)
	}

	remote(func() {
		m.files["r1"].content = []byte("changed!")
		m.files["r1"].vf["EndOfFile"], m.files["r1"].vf["UploadName"] = 8, "v-r1-2"
		m.bump("r1")
	})
	waitFor(t, "remote edit", func() bool {
		got, _ := os.ReadFile(filepath.Join(dir, "Docs", "added.txt"))
		return string(got) == "changed!"
	})

	remote(func() {
		m.files["f2"].vf["PublicName"] = "renamed.txt"
		m.bump("f2")
		m.tombstone("r1")
	})
	waitFor(t, "remote rename and delete", func() bool {
		names := []string{}
		entries, _ := os.ReadDir(filepath.Join(dir, "Docs"))
		for _, e := range entries {
			names = append(names, e.Name())
		}
		return strings.Join(names, ",") == "renamed.txt"
	})
}

func TestPermanentDeleteIsReconciled(t *testing.T) {
	m := newMock(t)
	srv := httptest.NewServer(m)
	defer srv.Close()
	dir := startMount(t, srv, "--sync-interval", "50ms")
	waitFor(t, "initial tree", func() bool { return exists(filepath.Join(dir, "Docs", "nested.txt")) })
	time.Sleep(200 * time.Millisecond)

	// No tombstone: the row just vanishes and the tick moves, leaving a gap.
	m.mu.Lock()
	delete(m.files, "f2")
	m.tick += 5
	m.mu.Unlock()
	waitFor(t, "permanent delete", func() bool {
		entries, _ := os.ReadDir(filepath.Join(dir, "Docs"))
		return len(entries) == 0
	})
	if got := m.names(); !strings.Contains(got, "readme.txt") {
		t.Fatalf("mock lost data: %s", got)
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 2 {
		t.Fatalf("reconcile dropped live rows: %v", entries)
	}
}

func TestOfflineAfterRestart(t *testing.T) {
	m := newMock(t)
	srv := httptest.NewServer(m)
	bin := filepath.Join(t.TempDir(), "rushfiles")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	base := t.TempDir()
	first, stop := mountStoppable(t, srv, bin, base)
	waitFor(t, "initial tree", func() bool { return exists(filepath.Join(first, "Docs", "nested.txt")) })
	if got, err := os.ReadFile(filepath.Join(first, "readme.txt")); err != nil || len(got) == 0 {
		t.Fatalf("read: %q %v", got, err)
	}
	time.Sleep(200 * time.Millisecond)
	stop()
	srv.Close()

	// The server is gone: mount, browse, and open the file that was cached.
	dir, stop2 := mountStoppable(t, srv, bin, base)
	defer stop2()
	entries, err := os.ReadDir(filepath.Join(dir, "Docs"))
	if err != nil || len(entries) != 1 {
		t.Fatalf("offline browse: %v %v", entries, err)
	}
	if got, err := os.ReadFile(filepath.Join(dir, "readme.txt")); err != nil || string(got) != "hello from the server\n" {
		t.Fatalf("offline cached read: %q %v", got, err)
	}
	if _, err := os.ReadFile(filepath.Join(dir, "Docs", "nested.txt")); err == nil {
		t.Fatal("an uncached file should fail offline")
	}
}

func testTreeDB(t *testing.T) *treeDB {
	t.Helper()
	db, err := openTreeDB(filepath.Join(t.TempDir(), "tree.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.db.Close() })
	return db
}

func TestFullListingNeverWipesTree(t *testing.T) {
	share := &Share{ID: "S-1", Name: "s"}
	tr := newShareTree(testTreeDB(t), share.ID)
	attrs := fileAttrs(attrNormal)
	file := func(id, parent, name string) *treeEntry {
		return entryOf(&VirtualFile{InternalName: id, ParrentID: parent, PublicName: name, Attributes: &attrs})
	}
	// Parents and shares are written with and without dashes.
	tr.finish(share, []*treeEntry{file("a", "s1", "one"), file("b", "S-1", "two"), file("s-1", "s1", "self")}, 5, true)
	if got := tr.children("S-1"); len(got) != 2 {
		t.Fatalf("children: %v", got)
	}
	tr.finish(share, nil, 6, true)
	if got := tr.children("s1"); len(got) != 2 {
		t.Fatalf("empty listing wiped the tree: %v", got)
	}

	// A change made during a catch-up beats the older rows.
	tr.begin()
	tr.put(VirtualFile{InternalName: "a", ParrentID: "s1", PublicName: "renamed", Attributes: &attrs})
	tr.finish(share, []*treeEntry{file("a", "s1", "one")}, 7, true)
	if tr.child("s1", "renamed") == nil {
		t.Fatal("local rename lost")
	}
	if tr.child("s1", "two") != nil {
		t.Fatal("full listing kept a row it no longer has")
	}
}

func TestTreeSurvivesReopen(t *testing.T) {
	share := &Share{ID: "s1", Name: "s"}
	path := filepath.Join(t.TempDir(), "tree.db")
	db, err := openTreeDB(path)
	if err != nil {
		t.Fatal(err)
	}
	attrs := fileAttrs(attrNormal)
	created := time.Date(2023, 2, 3, 4, 5, 6, 0, time.UTC)
	newShareTree(db, share.ID).finish(share, []*treeEntry{entryOf(&VirtualFile{
		InternalName: "a", ParrentID: "s1", PublicName: "one", User: "someone", Attributes: &attrs,
		CreationTime: &netTime{created},
	})}, 5, true)
	db.db.Close()

	if db, err = openTreeDB(path); err != nil {
		t.Fatal(err)
	}
	defer db.db.Close()
	trees, err := db.loadTrees()
	if err != nil {
		t.Fatal(err)
	}
	tr := trees[share.ID]
	if tr == nil || !tr.isReady() || tr.begin() != 5 {
		t.Fatalf("share state lost: %+v", tr)
	}
	vf, ok := tr.get("a")
	if !ok || vf.User != "someone" || vf.CreationTime == nil || !vf.CreationTime.Equal(created) {
		t.Fatalf("lost across reopen: %+v", vf)
	}
	var writeAhead int
	if err := db.db.QueryRow("SELECT COUNT(*) FROM files").Scan(&writeAhead); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{path, path + "-wal", path + "-shm"} {
		if st, err := os.Stat(name); err == nil && st.Mode().Perm() != 0o600 {
			t.Fatalf("%s is not private: %v", name, st.Mode())
		} else if err != nil && name == path {
			t.Fatal(err)
		}
	}
}

func TestOldTreeVersionIsRebuilt(t *testing.T) {
	path := filepath.Join(t.TempDir(), "tree.db")
	db, err := openTreeDB(path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.db.Exec("PRAGMA user_version = 1; INSERT INTO shares VALUES ('old', 9, 0)"); err != nil {
		t.Fatal(err)
	}
	db.db.Close()
	if db, err = openTreeDB(path); err != nil {
		t.Fatal(err)
	}
	defer db.db.Close()
	if trees, err := db.loadTrees(); err != nil || len(trees) != 0 {
		t.Fatalf("old tree kept: %v %v", trees, err)
	}
}

func TestTrimLeavesOpenFilesAlone(t *testing.T) {
	m := newMock(t)
	srv := httptest.NewServer(m)
	defer srv.Close()
	dir := startMount(t, srv, "--rw", "--sync-interval", "50ms", "--trim-interval", "50ms")
	waitFor(t, "initial tree", func() bool { return exists(filepath.Join(dir, "Docs", "nested.txt")) })

	// A file held open for reading and one mid-edit, with trims running throughout.
	reader, err := os.Open(filepath.Join(dir, "Docs", "nested.txt"))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	editor, err := os.OpenFile(filepath.Join(dir, "readme.txt"), os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := editor.WriteString("one\n"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	if _, err := editor.WriteString("two\n"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	got, err := io.ReadAll(reader)
	if err != nil || string(got) != "nested\n" {
		t.Fatalf("read across trims: %q %v", got, err)
	}
	if err := editor.Close(); err != nil {
		t.Fatal(err)
	}
	waitContent(t, m, "readme.txt", "hello from the server\none\ntwo\n")

	// Paths still resolve after their entries were dropped, also from inside a folder.
	sub := exec.Command("sh", "-c", "cd Docs && sleep 0.5 && cat nested.txt && pwd >/dev/null")
	sub.Dir = dir
	if out, err := sub.CombinedOutput(); err != nil || string(out) != "nested\n" {
		t.Fatalf("cwd across trims: %q %v", out, err)
	}
	for i := 0; i < 20; i++ {
		if got, err := os.ReadFile(filepath.Join(dir, "readme.txt")); err != nil || !strings.HasSuffix(string(got), "two\n") {
			t.Fatalf("read after trims: %q %v", got, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
}

func TestIdlePollDoesNotWrite(t *testing.T) {
	share := &Share{ID: "s1", Name: "s"}
	db := testTreeDB(t)
	tr := newShareTree(db, share.ID)
	attrs := fileAttrs(attrNormal)
	tr.finish(share, []*treeEntry{entryOf(&VirtualFile{InternalName: "a", ParrentID: "s1", PublicName: "one", Attributes: &attrs})}, 5, true)
	before := tr.reconciledAt
	var changes int
	read := func() int {
		var n int
		if err := db.db.QueryRow("SELECT total_changes()").Scan(&n); err != nil {
			t.Fatal(err)
		}
		return n
	}
	changes = read()
	tr.begin()
	tr.finish(share, nil, 5, false)
	if read() != changes || tr.reconciledAt != before {
		t.Fatal("an idle poll wrote to the database")
	}
}

func TestLocksBindOtherUsersOnly(t *testing.T) {
	m := newMock(t)
	lock := func(id, holder string) {
		m.mu.Lock()
		defer m.mu.Unlock()
		m.files[id].vf["FileLock"] = map[string]any{"IsLocked": true, "LockedBy": holder}
		m.bump(id)
	}
	m.add("mine", testShare, "mine.txt", 128, []byte("mine\n"))
	m.add("spare", testShare, "spare.txt", 128, []byte("spare\n"))
	lock("f1", "someone-else")
	lock("mine", "user1")
	srv := httptest.NewServer(m)
	defer srv.Close()
	dir := startMount(t, srv, "--rw", "--sync-interval", "50ms")
	readme := filepath.Join(dir, "readme.txt")
	waitFor(t, "tree with the lock", func() bool {
		st, err := os.Stat(readme)
		return err == nil && st.Mode().Perm() == 0o444
	})
	m.events()

	spare := filepath.Join(dir, "spare.txt")
	tmp := filepath.Join(dir, "saved.tmp")
	if err := os.WriteFile(tmp, []byte("new"), 0o644); err != nil {
		t.Fatal(err)
	}
	m.events()
	for name, try := range map[string]func() error{
		"write":             func() error { return os.WriteFile(readme, []byte("x"), 0o644) },
		"truncate":          func() error { return os.Truncate(readme, 0) },
		"delete":            func() error { return os.Remove(readme) },
		"rename":            func() error { return os.Rename(readme, filepath.Join(dir, "gone.txt")) },
		"move":              func() error { return os.Rename(readme, filepath.Join(dir, "Docs", "gone.txt")) },
		"replace by rename": func() error { return os.Rename(tmp, readme) },
	} {
		if err := try(); !errors.Is(err, os.ErrPermission) {
			t.Errorf("%s of a file locked by someone else: %v", name, err)
		}
	}
	if ev := m.events(); ev != "" {
		t.Errorf("locked file reached the server: %s", ev)
	}
	if got := string(m.content("readme.txt")); got != "hello from the server\n" {
		t.Fatalf("locked content changed: %q", got)
	}
	// Reading stays allowed, and so does everything for the lock's holder.
	if got, err := os.ReadFile(readme); err != nil || len(got) == 0 {
		t.Fatalf("read of a locked file: %q %v", got, err)
	}
	mine := filepath.Join(dir, "mine.txt")
	if st, err := os.Stat(mine); err != nil || st.Mode().Perm() != 0o644 {
		t.Fatalf("own lock changed the mode: %v %v", st, err)
	}
	if err := os.WriteFile(mine, []byte("edited\n"), 0o644); err != nil {
		t.Fatalf("edit under own lock: %v", err)
	}
	if err := os.Rename(mine, filepath.Join(dir, "mine2.txt")); err != nil {
		t.Fatalf("rename under own lock: %v", err)
	}

	// A lock taken after the last sync is caught by the fresh server record before
	// the queued delete goes out, and the file comes back.
	m.mu.Lock()
	m.files["spare"].vf["FileLock"] = map[string]any{"IsLocked": true, "LockedBy": "someone-else"}
	m.mu.Unlock() // no tick: the tree doesn't know yet
	m.events()
	if err := os.Remove(spare); err != nil {
		t.Fatalf("queued delete: %v", err)
	}
	waitFor(t, "refused delete to come back", func() bool { return exists(spare) })
	if ev := m.events(); strings.Contains(ev, "DELETE") {
		t.Errorf("delete of a freshly locked file reached the server: %s", ev)
	}
}

func TestEditOfRemotelyDeletedFileDoesNotRevive(t *testing.T) {
	m := newMock(t)
	srv := httptest.NewServer(m)
	defer srv.Close()
	// Uploading during close is what lets the error reach the editor.
	dir := startMount(t, srv, "--rw", "--sync-interval", "1h", "--upload-delay", "0")
	readme := filepath.Join(dir, "readme.txt")
	f, err := os.OpenFile(readme, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	m.mu.Lock()
	m.tombstone("f1")
	m.mu.Unlock()
	m.events()
	_, _ = f.WriteString("late edit\n")
	if err := f.Close(); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("closing an edit of a deleted file: %v", err)
	}
	if ev := m.events(); ev != "" {
		t.Fatalf("edit of a deleted file reached the server: %s", ev)
	}
}

func TestSavesUploadAfterClose(t *testing.T) {
	m := newMock(t)
	srv := httptest.NewServer(m)
	defer srv.Close()
	dir := startMount(t, srv, "--rw", "--sync-interval", "1h", "--upload-delay", "300ms")
	readme := filepath.Join(dir, "readme.txt")

	// Quick saves, each with an fsync, become one upload, and reads see the latest.
	for _, text := range []string{"one\n", "two\n", "three\n"} {
		f, err := os.OpenFile(readme, os.O_WRONLY|os.O_TRUNC, 0)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = f.WriteString(text)
		if err := f.Sync(); err != nil {
			t.Fatal(err)
		}
		if err := f.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if got, err := os.ReadFile(readme); err != nil || string(got) != "three\n" {
		t.Fatalf("read before upload: %q %v", got, err)
	}
	if ev := m.events(); ev != "" {
		t.Fatalf("close waited for the server: %s", ev)
	}
	waitContent(t, m, "readme.txt", "three\n")
	time.Sleep(500 * time.Millisecond)
	if ev := m.events(); ev != "PUT:3:readme.txt" {
		t.Errorf("saves were not combined: %s", ev)
	}

	// A swap file deleted before its upload never uploads.
	swap := filepath.Join(dir, ".readme.txt.swp")
	if err := os.WriteFile(swap, []byte("swap"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(swap); err != nil {
		t.Fatal(err)
	}
	time.Sleep(500 * time.Millisecond)
	if ev := m.events(); ev != "POST:0:.readme.txt.swp DELETE:1:.readme.txt.swp" {
		t.Errorf("deleted swap file: %s", ev)
	}
}

func TestUnmountFinishesUploads(t *testing.T) {
	m := newMock(t)
	srv := httptest.NewServer(m)
	defer srv.Close()
	bin := filepath.Join(t.TempDir(), "rushfiles")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	dir, stop := mountStoppable(t, srv, bin, t.TempDir(), "--rw", "--upload-delay", "1s")
	if err := os.WriteFile(filepath.Join(dir, "late.txt"), []byte("late\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	stop()
	if got := string(m.content("late.txt")); got != "late\n" {
		t.Fatalf("edit lost on unmount: %q", got)
	}
}

func TestFolderDeleteIsCombined(t *testing.T) {
	m := newMock(t)
	m.add("d2", "d1", "Sub", 16, nil)
	m.add("f3", "d2", "deep.txt", 128, []byte("deep\n"))
	m.add("f4", "d1", "other.txt", 128, []byte("other\n"))
	srv := httptest.NewServer(m)
	defer srv.Close()
	dir := startMount(t, srv, "--rw", "--sync-interval", "50ms", "--upload-delay", "500ms")
	docs := filepath.Join(dir, "Docs")
	waitFor(t, "initial tree", func() bool { return exists(filepath.Join(docs, "Sub", "deep.txt")) })
	m.events()

	if err := os.RemoveAll(docs); err != nil {
		t.Fatal(err)
	}
	if ev := m.events(); ev != "" {
		t.Fatalf("rm -r waited for the server: %s", ev)
	}
	// Catch-ups before the delete goes out must not bring the folder back.
	for i := 0; i < 8; i++ {
		if exists(docs) {
			t.Fatal("queued folder came back before its delete")
		}
		time.Sleep(40 * time.Millisecond)
	}
	waitFor(t, "folder delete", func() bool { return !strings.Contains(m.names(), "Docs") })
	if ev := m.events(); ev != "DELETE:1:Docs" {
		t.Errorf("deletes were not combined: %s", ev)
	}
	if got := m.names(); got != "readme.txt" {
		t.Errorf("server kept: %s", got)
	}
}

func TestRemoteAddKeepsQueuedFolder(t *testing.T) {
	m := newMock(t)
	srv := httptest.NewServer(m)
	defer srv.Close()
	dir := startMount(t, srv, "--rw", "--sync-interval", "50ms", "--upload-delay", "500ms")
	docs := filepath.Join(dir, "Docs")
	waitFor(t, "initial tree", func() bool { return exists(filepath.Join(docs, "nested.txt")) })
	m.events()

	if err := os.RemoveAll(docs); err != nil {
		t.Fatal(err)
	}
	// Someone adds a file before the delete goes out: only what was deleted here goes.
	m.mu.Lock()
	m.add("late", "d1", "late.txt", 128, []byte("late\n"))
	m.mu.Unlock()
	waitFor(t, "folder to come back", func() bool { return exists(filepath.Join(docs, "late.txt")) })
	if ev := m.events(); ev != "DELETE:1:nested.txt" {
		t.Errorf("events: %s", ev)
	}
	if exists(filepath.Join(docs, "nested.txt")) {
		t.Error("deleted file came back")
	}
}

func TestQueuedDeleteBeforeSameName(t *testing.T) {
	m := newMock(t)
	srv := httptest.NewServer(m)
	defer srv.Close()
	dir := startMount(t, srv, "--rw", "--sync-interval", "1h", "--upload-delay", "1s")
	readme := filepath.Join(dir, "readme.txt")
	waitFor(t, "initial tree", func() bool { return exists(readme) })
	time.Sleep(200 * time.Millisecond)
	m.events()

	if err := os.Remove(readme); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(readme, []byte("new\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	waitContent(t, m, "readme.txt", "new\n")
	if ev := m.events(); ev != "DELETE:1:readme.txt POST:0:readme.txt PUT:3:readme.txt" {
		t.Errorf("events: %s", ev)
	}
}

func TestUnmountFinishesDeletes(t *testing.T) {
	m := newMock(t)
	srv := httptest.NewServer(m)
	defer srv.Close()
	bin := filepath.Join(t.TempDir(), "rushfiles")
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	dir, stop := mountStoppable(t, srv, bin, t.TempDir(), "--rw", "--upload-delay", "10s")
	time.Sleep(200 * time.Millisecond) // the tree is ready
	if err := os.Remove(filepath.Join(dir, "readme.txt")); err != nil {
		t.Fatal(err)
	}
	stop()
	if strings.Contains(m.names(), "readme.txt") {
		t.Fatalf("delete lost on unmount: %s", m.names())
	}
}
