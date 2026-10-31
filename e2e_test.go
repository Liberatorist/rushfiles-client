package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
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

func (m *mockServer) add(id, parent, name string, attrs int, content []byte) {
	m.files[id] = &mockFile{content: content, vf: map[string]any{
		"InternalName": id, "ParrentId": parent, "PublicName": name, "ShareId": testShare,
		"Attributes": attrs, "IsFile": attrs != 16, "EndOfFile": len(content), "Tick": 1, "ShareTick": 1,
		"LastWriteTime": "2024-05-01T10:00:00", "FilehHash": "h1", "Deleted": false, "UploadName": "v-" + id,
	}}
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
		f := m.files[path[strings.LastIndex(path, "/")+1:]]
		if f == nil {
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
		}
		resp["ClientJournalEvent"] = map[string]any{"RfVirtualFile": m.files[id].vf}
	case r.Method == http.MethodPut && event == eventRename:
		m.files[id].vf["PublicName"], m.files[id].vf["Tick"] = vf["PublicName"], 2
	case r.Method == http.MethodPut && event == eventMove:
		m.files[id].vf["ParrentId"] = vf["ParrentId"]
	case r.Method == http.MethodDelete && event == eventDelete:
		delete(m.files, id)
	default:
		m.t.Errorf("wrong verb for event: %s %d", r.Method, event)
	}
	_ = json.NewEncoder(w).Encode(map[string]any{"Data": resp, "Code": 1})
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
	base := t.TempDir()
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
	if !bytes.Equal(m.content("big.bin"), big) {
		t.Fatalf("big upload mismatch: %d bytes", len(m.content("big.bin")))
	}
	if ev := m.events(); ev != "POST:0:big.bin PUT:3:big.bin" {
		t.Errorf("create events: %s", ev)
	}

	// Shell-style overwrite must publish one version, never an empty one.
	sh := exec.Command("sh", "-c", "echo edited > "+filepath.Join(dir, "readme.txt"))
	if out, err := sh.CombinedOutput(); err != nil {
		t.Fatalf("%v %s", err, out)
	}
	if got := m.content("readme.txt"); string(got) != "edited\n" {
		t.Fatalf("overwrite: %q", got)
	}
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
	if got := string(m.content("renamed.txt")); got != "edited\nmore\n" {
		t.Fatalf("append: %q", got)
	}
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

	// Editor-style atomic save over an existing file.
	if err := os.WriteFile(filepath.Join(sub, ".tmp"), []byte("saved"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(filepath.Join(sub, ".tmp"), filepath.Join(sub, "moved.txt")); err != nil {
		t.Fatal(err)
	}
	if got := string(m.content("moved.txt")); got != "saved" {
		t.Fatalf("atomic save: %q", got)
	}
	if got, err := os.ReadFile(filepath.Join(sub, "moved.txt")); err != nil || string(got) != "saved" {
		t.Fatalf("read back: %q %v", got, err)
	}
	if err := os.Remove(filepath.Join(sub, "moved.txt")); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(sub); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(m.names(), "New Folder") || strings.Contains(m.names(), "moved.txt") {
		t.Errorf("deletes did not reach the server: %s", m.names())
	}
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
	if got := string(m.content("readme.txt")); got != "someone else\n" {
		t.Fatalf("server version overwritten: %q", got)
	}
	found := false
	m.mu.Lock()
	for _, v := range m.files {
		if strings.Contains(fmt.Sprint(v.vf["PublicName"]), "conflict") {
			found = strings.HasSuffix(string(v.content), "mine\n")
		}
	}
	m.mu.Unlock()
	if !found {
		t.Fatalf("edit was lost: %s", m.names())
	}
	got, err := os.ReadFile(filepath.Join(dir, "readme.txt"))
	if err != nil || string(got) != "someone else\n" {
		t.Fatalf("cache served the wrong version: %q %v", got, err)
	}
}
