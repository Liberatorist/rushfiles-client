package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"time"
)

// Values taken from the Windows client (AuthenticationService, Skinning/config.json).
const (
	oidcClientID     = "rushfiles.PCClient"
	oidcClientSecret = "secret"
	oidcScope        = "openid profile domain_api offline_access"
	oidcRedirectURI  = "rushfiles://signedin"
)

func authority() string { return envOr("RUSHFILES_AUTHORITY", "https://auth.rushfiles.com") }

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

func configDir() string {
	if d := os.Getenv("XDG_CONFIG_HOME"); d != "" {
		return filepath.Join(d, "rushfiles-linux")
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".config", "rushfiles-linux")
}

func cacheBaseDir() string {
	if d := os.Getenv("XDG_CACHE_HOME"); d != "" {
		return filepath.Join(d, "rushfiles-linux")
	}
	home, _ := os.UserHomeDir()
	return filepath.Join(home, ".cache", "rushfiles-linux")
}

type tokenSet struct {
	AccessToken  string    `json:"access_token"`
	RefreshToken string    `json:"refresh_token"`
	Expiry       time.Time `json:"expiry"`
	DeviceID     string    `json:"device_id"`
}

func tokenPath() string { return filepath.Join(configDir(), "tokens.json") }

func loadTokens() (*tokenSet, error) {
	data, err := os.ReadFile(tokenPath())
	if err != nil {
		return nil, fmt.Errorf("not logged in, run `rushfiles login` first: %w", err)
	}
	var t tokenSet
	if err := json.Unmarshal(data, &t); err != nil {
		return nil, err
	}
	return &t, nil
}

func saveTokens(t *tokenSet) error {
	if err := os.MkdirAll(configDir(), 0o700); err != nil {
		return err
	}
	data, err := json.MarshalIndent(t, "", "  ")
	if err != nil {
		return err
	}
	tmp := tokenPath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, tokenPath())
}

// deviceID persists a random id; the Windows client hashes hardware serials instead.
func deviceID() string {
	if t, err := loadTokens(); err == nil && t.DeviceID != "" {
		return t.DeviceID
	}
	return randomHex(16)
}

func randomHex(n int) string {
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		panic(err)
	}
	return hex.EncodeToString(b)
}

func acrValues(device string) string {
	host, _ := os.Hostname()
	return "localAuth:False deviceId:" + url.QueryEscape(device) +
		" deviceName:" + url.QueryEscape(host) + " deviceType:0 deviceOs:Linux"
}

type tokenResponse struct {
	AccessToken  string `json:"access_token"`
	RefreshToken string `json:"refresh_token"`
	ExpiresIn    int    `json:"expires_in"`
	Error        string `json:"error"`
}

func requestToken(ctx context.Context, form url.Values) (*tokenResponse, error) {
	form.Set("client_id", oidcClientID)
	form.Set("client_secret", oidcClientSecret)
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, authority()+"/connect/token", strings.NewReader(form.Encode()))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var out tokenResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("token endpoint returned %s: %w", resp.Status, err)
	}
	if out.Error != "" {
		return nil, fmt.Errorf("token endpoint: %s", out.Error)
	}
	if out.AccessToken == "" {
		return nil, fmt.Errorf("token endpoint returned %s without an access token", resp.Status)
	}
	return &out, nil
}

type tokenSource struct {
	mu sync.Mutex
	t  *tokenSet
}

func (s *tokenSource) deviceID() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.t.DeviceID
}

// get returns a valid access token. Passing the rejected token forces a refresh
// unless another request has replaced it already.
func (s *tokenSource) get(ctx context.Context, rejected string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.t.AccessToken != rejected && time.Until(s.t.Expiry) > time.Minute {
		return s.t.AccessToken, nil
	}
	out, err := requestToken(ctx, url.Values{"grant_type": {"refresh_token"}, "refresh_token": {s.t.RefreshToken}})
	if err != nil {
		return "", err
	}
	s.t.AccessToken = out.AccessToken
	if out.RefreshToken != "" {
		s.t.RefreshToken = out.RefreshToken
	}
	s.t.Expiry = time.Now().Add(time.Duration(out.ExpiresIn) * time.Second)
	if err := saveTokens(s.t); err != nil {
		return "", err
	}
	return s.t.AccessToken, nil
}

type claims struct {
	Sub     string
	Domains []string
}

// parseClaims reads the JWT payload. The token came straight from the token
// endpoint and is only used to learn our own user id and domains.
func parseClaims(token string) (*claims, error) {
	parts := strings.Split(token, ".")
	if len(parts) != 3 {
		return nil, errors.New("access token is not a JWT")
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[1])
	if err != nil {
		return nil, err
	}
	var p struct {
		Sub     string          `json:"sub"`
		Primary string          `json:"primary_domain"`
		Domains json.RawMessage `json:"domains"`
	}
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, err
	}
	c := &claims{Sub: p.Sub}
	if p.Primary != "" {
		c.Domains = append(c.Domains, p.Primary)
	}
	var many []string
	var one string
	if json.Unmarshal(p.Domains, &many) != nil && json.Unmarshal(p.Domains, &one) == nil && one != "" {
		many = []string{one}
	}
	for _, d := range many {
		if d != p.Primary {
			c.Domains = append(c.Domains, d)
		}
	}
	return c, nil
}

func loginSocket() string {
	dir := os.Getenv("XDG_RUNTIME_DIR")
	if dir == "" {
		dir = configDir()
	}
	return filepath.Join(dir, "rushfiles-login.sock")
}

// installHandler registers this binary for rushfiles:// links, which is how the
// browser hands the login result back.
func installHandler() error {
	exe, err := os.Executable()
	if err != nil {
		return err
	}
	home, _ := os.UserHomeDir()
	dir := filepath.Join(home, ".local", "share", "applications")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	desktop := fmt.Sprintf("[Desktop Entry]\nType=Application\nName=RushFiles Login Handler\nExec=%q callback %%u\nNoDisplay=true\nTerminal=false\nMimeType=x-scheme-handler/rushfiles;\n", exe)
	if err := os.WriteFile(filepath.Join(dir, "rushfiles-linux.desktop"), []byte(desktop), 0o644); err != nil {
		return err
	}
	return exec.Command("xdg-mime", "default", "rushfiles-linux.desktop", "x-scheme-handler/rushfiles").Run()
}

func sendCallback(uri string) error {
	conn, err := net.Dial("unix", loginSocket())
	if err != nil {
		return err
	}
	defer conn.Close()
	_, err = fmt.Fprintln(conn, uri)
	return err
}

// waitForRedirect takes the redirect from the link handler or from pasted input.
func waitForRedirect(ctx context.Context, pasted <-chan string) (string, error) {
	got := make(chan string, 2)
	_ = os.Remove(loginSocket())
	if l, err := net.Listen("unix", loginSocket()); err == nil {
		defer l.Close()
		defer os.Remove(loginSocket())
		go func() {
			for {
				conn, err := l.Accept()
				if err != nil {
					return
				}
				line, _ := bufio.NewReader(conn).ReadString('\n')
				conn.Close()
				got <- strings.TrimSpace(line)
			}
		}()
	}
	select {
	case uri := <-got:
		return uri, nil
	case uri := <-pasted:
		return uri, nil
	case <-ctx.Done():
		return "", errors.New("timed out waiting for the login to finish")
	}
}

// pendingLogin is a sign-in waiting for the browser to come back.
type pendingLogin struct {
	url, verifier, state, device string
}

func newLogin() *pendingLogin {
	p := &pendingLogin{verifier: randomHex(32), state: randomHex(16), device: deviceID()}
	sum := sha256.Sum256([]byte(p.verifier))
	q := url.Values{
		"response_type":         {"code"},
		"client_id":             {oidcClientID},
		"redirect_uri":          {oidcRedirectURI},
		"scope":                 {oidcScope},
		"state":                 {p.state},
		"nonce":                 {randomHex(16)},
		"code_challenge":        {base64.RawURLEncoding.EncodeToString(sum[:])},
		"code_challenge_method": {"S256"},
		"acr_values":            {acrValues(p.device)},
	}
	p.url = authority() + "/connect/authorize?" + q.Encode()
	return p
}

func (p *pendingLogin) openBrowser() { _ = exec.Command("xdg-open", p.url).Start() }

func (p *pendingLogin) finish(ctx context.Context, pasted <-chan string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Minute)
	defer cancel()
	uri, err := waitForRedirect(ctx, pasted)
	if err != nil {
		return err
	}
	u, err := url.Parse(uri)
	if err != nil {
		return err
	}
	params := u.Query()
	if u.Fragment != "" && params.Get("code") == "" {
		params, _ = url.ParseQuery(u.Fragment)
	}
	if e := params.Get("error"); e != "" {
		return fmt.Errorf("login failed: %s %s", e, params.Get("error_description"))
	}
	if params.Get("state") != p.state {
		return errors.New("login failed: state mismatch")
	}
	out, err := requestToken(ctx, url.Values{
		"grant_type":    {"authorization_code"},
		"code":          {params.Get("code")},
		"redirect_uri":  {oidcRedirectURI},
		"code_verifier": {p.verifier},
	})
	if err != nil {
		return err
	}
	return saveTokens(&tokenSet{
		AccessToken:  out.AccessToken,
		RefreshToken: out.RefreshToken,
		Expiry:       time.Now().Add(time.Duration(out.ExpiresIn) * time.Second),
		DeviceID:     p.device,
	})
}

func login(ctx context.Context) error {
	if err := installHandler(); err != nil {
		fmt.Fprintln(os.Stderr, "note: could not register the rushfiles:// handler:", err)
	}
	p := newLogin()
	fmt.Println("Sign in with this URL (opening your browser):\n\n" + p.url)
	fmt.Println("\nIf the browser does not return here, paste the final rushfiles://signedin... URL below.")
	p.openBrowser()

	pasted := make(chan string, 1)
	go func() {
		s := bufio.NewScanner(os.Stdin)
		for s.Scan() {
			if strings.HasPrefix(strings.TrimSpace(s.Text()), "rushfiles://") {
				pasted <- strings.TrimSpace(s.Text())
				return
			}
		}
	}()
	return p.finish(ctx, pasted)
}
