package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"syscall"
	"time"

	"github.com/google/uuid"
	"golang.org/x/text/unicode/norm"
)

const (
	eventCreate = 0
	eventDelete = 1
	eventRename = 2
	eventUpdate = 3
	eventMove   = 16

	attrDirectory = 16
	attrNormal    = 128

	shareTypeSubShare = 1
	uploadChunkSize   = 10 * 1024 * 1024
	catchupBatch      = 10000
)

// netTime reads the timestamp styles .NET emits and writes UTC.
type netTime struct{ time.Time }

func (t *netTime) UnmarshalJSON(b []byte) error {
	var s string
	if json.Unmarshal(b, &s) != nil || s == "" {
		return nil
	}
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05.9999999", "2006-01-02T15:04:05"} {
		if parsed, err := time.Parse(layout, s); err == nil {
			t.Time = parsed
			return nil
		}
	}
	return nil
}

func (t netTime) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.UTC().Format(time.RFC3339Nano))
}

// fileAttrs is a System.IO.FileAttributes value, number or enum name.
type fileAttrs int

func (a *fileAttrs) UnmarshalJSON(b []byte) error {
	var n int
	if json.Unmarshal(b, &n) == nil {
		*a = fileAttrs(n)
		return nil
	}
	var s string
	if json.Unmarshal(b, &s) == nil {
		*a = attrNormal
		if strings.Contains(s, "Directory") {
			*a = attrDirectory
		}
	}
	return nil
}

// VirtualFile mirrors VirtualFileDto. Newtonsoft's default PascalCase is what the
// server gets from the Windows client, misspelled ParrentId included.
type VirtualFile struct {
	InternalName       string          `json:"InternalName"`
	UploadName         string          `json:"UploadName,omitempty"`
	PublicName         string          `json:"PublicName"`
	User               string          `json:"User,omitempty"`
	IsFile             bool            `json:"IsFile"`
	FileLock           json.RawMessage `json:"FileLock,omitempty"`
	Path               string          `json:"Path,omitempty"`
	NewPath            string          `json:"NewPath,omitempty"`
	ShareID            string          `json:"ShareId"`
	ShareTick          int64           `json:"ShareTick"`
	Tick               *int64          `json:"Tick"`
	SubShareIDs        []string        `json:"SubShareIds,omitempty"`
	ParrentID          string          `json:"ParrentId"`
	EndOfFile          *int64          `json:"EndOfFile"`
	CreationTime       *netTime        `json:"CreationTime"`
	LastAccessTime     *netTime        `json:"LastAccessTime"`
	LastWriteTime      *netTime        `json:"LastWriteTime"`
	Attributes         *fileAttrs      `json:"Attributes"`
	Deleted            bool            `json:"Deleted"`
	FilehHash          string          `json:"FilehHash,omitempty"`
	CopyLargeFileState json.RawMessage `json:"CopyLargeFileStatus,omitempty"`
}

func (v *VirtualFile) isDir() bool {
	if v.Attributes != nil {
		return int(*v.Attributes)&attrDirectory != 0
	}
	return !v.IsFile
}

func (v *VirtualFile) size() int64 {
	if v.EndOfFile == nil {
		return 0
	}
	return *v.EndOfFile
}

func (v *VirtualFile) mtime() time.Time {
	if v.LastWriteTime != nil && !v.LastWriteTime.IsZero() {
		return v.LastWriteTime.Time
	}
	return time.Unix(0, 0)
}

// contentID is the id file bytes are fetched by: the Windows client downloads by
// VersionId, which it maps from UploadName, not by InternalName.
func (v *VirtualFile) contentID() string {
	if v.UploadName != "" {
		return v.UploadName
	}
	return v.InternalName
}

// version identifies file content. Tick is no use here: renames and moves bump it too.
func (v *VirtualFile) version() string {
	if v.UploadName != "" {
		return v.UploadName
	}
	if v.FilehHash != "" {
		return fmt.Sprintf("%d-%s", v.size(), v.FilehHash)
	}
	return fmt.Sprintf("%d-%d", v.size(), v.mtime().UnixMilli())
}

// lockedByOther follows the Windows client: a lock only binds other users, and
// its holder may keep working on any device.
func (v *VirtualFile) lockedByOther(userID string) bool {
	if len(v.FileLock) == 0 {
		return false
	}
	var lock struct {
		IsLocked bool
		LockedBy string
	}
	return json.Unmarshal(v.FileLock, &lock) == nil && lock.IsLocked && lock.LockedBy != userID
}

type Share struct {
	ID            string `json:"Id"`
	Name          string `json:"Name"`
	VirtualFileID string `json:"VirtualFileId"`
	ShareType     int    `json:"ShareType"`
	Domain        string `json:"Domain"`
}

// rootParentID matches OnFileCreateLocalDelegate: subshares hang off their shared folder.
func (s *Share) rootParentID() string {
	if s.ShareType == shareTypeSubShare && s.VirtualFileID != "" {
		return s.VirtualFileID
	}
	return s.ID
}

type journal struct {
	TransmitID    string       `json:"TransmitId"`
	TransmitTime  string       `json:"TransmitTimeout"`
	EventType     int          `json:"ClientJournalEventType"`
	DeviceID      string       `json:"DeviceId"`
	RfVirtualFile *VirtualFile `json:"RfVirtualFile"`
}

type uploadResponse struct {
	ClientJournalEvent *struct {
		RfVirtualFile *VirtualFile `json:"RfVirtualFile"`
	} `json:"ClientJournalEvent"`
	URL string `json:"Url"`
}

func (r *uploadResponse) uploadURL() string {
	if r == nil {
		return ""
	}
	return r.URL
}

func (r *uploadResponse) file() *VirtualFile {
	if r == nil || r.ClientJournalEvent == nil {
		return nil
	}
	return r.ClientJournalEvent.RfVirtualFile
}

// fileCode is FileResponseCode, number or enum name. A missing value counts as success.
type fileCode int

var fileCodeNames = []string{"DeniedAccess", "Success", "Conflict", "Duplicate", "NoRequest", "NotYetImplemented",
	"InsufficientData", "AlreadyExist", "FailedTalkingToServer", "IgnoreDelete", "EntityNotFound", "StorageLimitExceeded",
	"IllegalCharacters", "ParentNotFound", "JournalAlreadyExists", "FileIsLocked", "BadRange", "MissingCreateEvent",
	"LargeFile", "EndsWithDot"}

func (c *fileCode) UnmarshalJSON(b []byte) error {
	*c = 1
	var n int
	if json.Unmarshal(b, &n) == nil {
		*c = fileCode(n)
		return nil
	}
	var s string
	if json.Unmarshal(b, &s) == nil {
		for i, name := range fileCodeNames {
			if name == s {
				*c = fileCode(i)
			}
		}
	}
	return nil
}

func (c fileCode) err() error {
	switch fileCodeNames[c] {
	case "Success", "LargeFile", "IgnoreDelete":
		return nil
	case "DeniedAccess":
		return syscall.EACCES
	case "StorageLimitExceeded":
		return syscall.ENOSPC
	case "FileIsLocked":
		return syscall.EBUSY
	case "AlreadyExist", "Duplicate", "JournalAlreadyExists", "Conflict":
		return syscall.EEXIST
	case "EntityNotFound", "ParentNotFound":
		return syscall.ENOENT
	case "IllegalCharacters", "EndsWithDot":
		return syscall.EINVAL
	}
	return fmt.Errorf("server refused the change: %s", fileCodeNames[c])
}

type apiClient struct {
	tokens   *tokenSource
	http     *http.Client
	userID   string
	domains  []string
	readOnly bool
}

func newAPIClient(readOnly bool) (*apiClient, error) {
	t, err := loadTokens()
	if err != nil {
		return nil, err
	}
	src := &tokenSource{t: t}
	token, err := src.get(context.Background(), "")
	if errors.Is(err, errLoginRejected) {
		return nil, fmt.Errorf("refreshing the login failed, run `rushfiles login`: %w", err)
	}
	if err != nil {
		// Probably offline. The old token still says who we are, and refreshing can wait.
		log.Printf("could not refresh the login: %v", err)
		token = t.AccessToken
	}
	c, err := parseClaims(token)
	if err != nil {
		return nil, err
	}
	if len(c.Domains) == 0 {
		return nil, errors.New("the login token lists no domains")
	}
	return &apiClient{tokens: src, http: &http.Client{}, userID: c.Sub, domains: c.Domains, readOnly: readOnly}, nil
}

func gatewayURL(domain string) string {
	return envOr("RUSHFILES_GATEWAY_URL", "https://clientgateway."+domain)
}

func filecacheURL(domain string) string {
	return envOr("RUSHFILES_FILECACHE_URL", "https://filecache01."+domain)
}

// do sends a request with auth, refreshing once on 401 and backing off on overload
// the way BaseApiClient does for GETs. Mutations are not replayed on overload.
func (c *apiClient) do(ctx context.Context, method, target string, body []byte, header map[string]string) (*http.Response, error) {
	backoff := []time.Duration{2 * time.Second, 5 * time.Second, 15 * time.Second, 30 * time.Second}
	rejected := ""
	for attempt := 0; ; attempt++ {
		token, err := c.tokens.get(ctx, rejected)
		if err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, method, target, bytes.NewReader(body))
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", "Bearer "+token)
		req.Header.Set("Accept", "application/json")
		for k, v := range header {
			req.Header.Set(k, v)
		}
		resp, err := c.http.Do(req)
		if err != nil {
			return nil, err
		}
		if resp.StatusCode == http.StatusUnauthorized && rejected == "" {
			resp.Body.Close()
			rejected = token
			continue
		}
		overload := resp.StatusCode == 429 || resp.StatusCode == 502 || resp.StatusCode == 503 || resp.StatusCode == 504
		if method == http.MethodGet && overload && attempt < len(backoff) {
			resp.Body.Close()
			select {
			case <-time.After(backoff[attempt]):
				continue
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}
		return resp, nil
	}
}

func (c *apiClient) getJSON(ctx context.Context, target string, out any) error {
	resp, err := c.do(ctx, http.MethodGet, target, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNotFound {
		return syscall.ENOENT
	}
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("GET %s: %s", target, resp.Status)
	}
	return json.NewDecoder(resp.Body).Decode(out)
}

// shares lists what it can reach, failing only when no domain answered.
func (c *apiClient) shares(ctx context.Context) ([]*Share, error) {
	all, err := c.allShares(ctx)
	if len(all) > 0 {
		return all, nil
	}
	return nil, err
}

// allShares also returns the first error when some domains could not be listed.
func (c *apiClient) allShares(ctx context.Context) ([]*Share, error) {
	var all []*Share
	seen := map[string]bool{}
	var firstErr error
	for _, domain := range c.domains {
		var out struct{ Data []*Share }
		target := fmt.Sprintf("%s/api/users/%s/shares?includeAssociations=True", gatewayURL(domain), url.PathEscape(c.userID))
		if err := c.getJSON(ctx, target, &out); err != nil {
			log.Printf("listing shares on %s: %v", domain, err)
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		for _, s := range out.Data {
			if !seen[s.ID] {
				seen[s.ID] = true
				if s.Domain == "" {
					s.Domain = domain
				}
				all = append(all, s)
			}
		}
	}
	return all, firstErr
}

type catchupPage struct {
	RfVirtualFiles []VirtualFile
	ShareTick      int64
	LargestTick    int64
	FullCatchup    bool
}

func (c *apiClient) catchup(ctx context.Context, s *Share, token string, tickFrom, tickStart int64) (*catchupPage, error) {
	q := url.Values{
		"token": {token}, "shareId": {s.ID}, "tickFrom": {fmt.Sprint(tickFrom)},
		"tickTake": {fmt.Sprint(catchupBatch)}, "tickStart": {fmt.Sprint(tickStart)},
	}
	var out catchupPage
	if err := c.getJSON(ctx, gatewayURL(s.Domain)+"/api/client/catchup?"+q.Encode(), &out); err != nil {
		return nil, err
	}
	return &out, nil
}

// shareTokens returns the per-share tokens catch-up needs, keyed by share id.
func (c *apiClient) shareTokens(ctx context.Context, domain string) (map[string]string, error) {
	var out struct {
		Data struct {
			ManagedShares []struct {
				ShareID  string `json:"ShareId"`
				ShareACL struct {
					ACLToken string `json:"AclToken"`
				} `json:"ShareAcl"`
			}
		}
	}
	target := gatewayURL(domain) + "/api/client/fullprofile?tick=1&api=1&associationRequired=false"
	if err := c.getJSON(ctx, target, &out); err != nil {
		return nil, err
	}
	tokens := make(map[string]string, len(out.Data.ManagedShares))
	for _, m := range out.Data.ManagedShares {
		tokens[m.ShareID] = m.ShareACL.ACLToken
	}
	return tokens, nil
}

func (c *apiClient) children(ctx context.Context, s *Share, parentID string) ([]VirtualFile, error) {
	var out struct{ Data []VirtualFile }
	target := fmt.Sprintf("%s/api/shares/%s/virtualfiles/%s/children?includeAssociations=false&includeDeleted=false",
		gatewayURL(s.Domain), url.PathEscape(s.ID), url.PathEscape(parentID))
	if err := c.getJSON(ctx, target, &out); err != nil {
		return nil, err
	}
	return out.Data, nil
}

func (c *apiClient) virtualFile(ctx context.Context, s *Share, id string) (*VirtualFile, error) {
	var out struct{ Data *VirtualFile }
	target := fmt.Sprintf("%s/api/shares/%s/virtualFiles/%s", gatewayURL(s.Domain), url.PathEscape(s.ID), url.PathEscape(id))
	if err := c.getJSON(ctx, target, &out); err != nil {
		return nil, err
	}
	// Deleted files stay readable here, with Deleted set, instead of giving a 404.
	if out.Data != nil && out.Data.Deleted {
		return nil, syscall.ENOENT
	}
	return out.Data, nil
}

func (c *apiClient) download(ctx context.Context, s *Share, id string, dst *os.File) error {
	target := fmt.Sprintf("%s/api/shares/%s/files/%s", filecacheURL(s.Domain), url.PathEscape(s.ID), url.PathEscape(id))
	resp, err := c.do(ctx, http.MethodGet, target, nil, nil)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("download %s: %s", id, resp.Status)
	}
	n, err := io.Copy(dst, resp.Body)
	if err != nil {
		return err
	}
	if resp.ContentLength >= 0 && n != resp.ContentLength {
		return fmt.Errorf("download %s: got %d of %d bytes", id, n, resp.ContentLength)
	}
	return nil
}

func (c *apiClient) newJournal(event int, vf *VirtualFile) *journal {
	vf.PublicName = norm.NFC.String(vf.PublicName)
	return &journal{
		TransmitID:    uuid.NewString(),
		TransmitTime:  "0001-01-01T00:00:00",
		EventType:     event,
		DeviceID:      c.tokens.deviceID(),
		RfVirtualFile: vf,
	}
}

// send issues one journal event, the single path every server mutation takes.
func (c *apiClient) send(ctx context.Context, method, target string, j *journal) (*uploadResponse, error) {
	if c.readOnly {
		return nil, syscall.EROFS
	}
	body, err := json.Marshal(j)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(ctx, method, target, body, map[string]string{"Content-Type": "application/json; charset=utf-8"})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	raw, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var out struct {
		Data    *uploadResponse
		Message string
		Code    *fileCode
	}
	_ = json.Unmarshal(raw, &out)
	if out.Code != nil {
		if err := out.Code.err(); err != nil {
			log.Printf("%s %s: %v (%s)", method, target, err, out.Message)
			return nil, err
		}
	}
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return nil, fmt.Errorf("%s %s: %s: %s", method, target, resp.Status, out.Message)
	}
	return out.Data, nil
}

func (c *apiClient) fileURL(s *Share, id string) string {
	base := fmt.Sprintf("%s/api/shares/%s/files", filecacheURL(s.Domain), url.PathEscape(s.ID))
	if id != "" {
		base += "/" + url.PathEscape(id)
	}
	return base
}

func newFileRecord(s *Share, parentID, name string, attrs fileAttrs, size int64, mtime time.Time) *VirtualFile {
	now := &netTime{time.Now()}
	zero := int64(0)
	return &VirtualFile{
		ShareID: s.ID, ParrentID: parentID, PublicName: name, EndOfFile: &size, Tick: &zero, Attributes: &attrs,
		CreationTime: now, LastAccessTime: now, LastWriteTime: &netTime{mtime},
	}
}

// create registers a new file or folder and returns the server's record, plus the
// content upload URL when size is non-zero.
func (c *apiClient) create(ctx context.Context, s *Share, parentID, name string, attrs fileAttrs, size int64, mtime time.Time) (*VirtualFile, string, error) {
	rec := newFileRecord(s, parentID, name, attrs, size, mtime)
	out, err := c.send(ctx, http.MethodPost, c.fileURL(s, ""), c.newJournal(eventCreate, rec))
	if err != nil {
		return nil, "", err
	}
	if vf := out.file(); vf != nil && vf.InternalName != "" {
		return vf, out.uploadURL(), nil
	}
	// Same recovery as TryResolveServerFileByNameAsync.
	kids, err := c.children(ctx, s, parentID)
	if err != nil {
		return nil, "", err
	}
	for i := range kids {
		if strings.EqualFold(kids[i].PublicName, rec.PublicName) {
			return &kids[i], out.uploadURL(), nil
		}
	}
	return nil, "", fmt.Errorf("server did not return an id for %q", name)
}

// update announces new content for an existing file and uploads it.
func (c *apiClient) update(ctx context.Context, s *Share, vf *VirtualFile, src io.ReaderAt, size int64, mtime time.Time) (*VirtualFile, error) {
	rec := newFileRecord(s, vf.ParrentID, vf.PublicName, attrNormal, size, mtime)
	rec.InternalName = vf.InternalName
	out, err := c.send(ctx, http.MethodPut, c.fileURL(s, vf.InternalName), c.newJournal(eventUpdate, rec))
	if err != nil {
		return nil, err
	}
	if size == 0 {
		return out.file(), nil
	}
	if out.uploadURL() == "" {
		return nil, fmt.Errorf("server gave no upload URL for %q", vf.PublicName)
	}
	return c.uploadContent(ctx, s, out.uploadURL(), src, size)
}

func (c *apiClient) uploadContent(ctx context.Context, s *Share, uploadURL string, src io.ReaderAt, size int64) (*VirtualFile, error) {
	if c.readOnly {
		return nil, syscall.EROFS
	}
	base, err := url.Parse(filecacheURL(s.Domain) + "/")
	if err != nil {
		return nil, err
	}
	ref, err := url.Parse(uploadURL)
	if err != nil {
		return nil, err
	}
	target := base.ResolveReference(ref).String()
	buf := make([]byte, min(uploadChunkSize, size))
	var last *VirtualFile
	for pos := int64(0); pos < size; {
		n, err := src.ReadAt(buf, pos)
		if err != nil && !(errors.Is(err, io.EOF) && n > 0) {
			return nil, err
		}
		resp, err := c.do(ctx, http.MethodPut, target, buf[:n], map[string]string{
			"Content-Type":  "application/octet-stream",
			"Content-Range": fmt.Sprintf("bytes %d-%d/%d", pos, pos+int64(n)-1, size),
		})
		if err != nil {
			return nil, err
		}
		var out struct{ Data *uploadResponse }
		decodeErr := json.NewDecoder(resp.Body).Decode(&out)
		resp.Body.Close()
		if resp.StatusCode < 200 || resp.StatusCode > 299 {
			return nil, fmt.Errorf("upload chunk at %d: %s", pos, resp.Status)
		}
		if decodeErr == nil {
			last = out.Data.file()
		}
		pos += int64(n)
	}
	return last, nil
}

func (c *apiClient) remove(ctx context.Context, s *Share, vf VirtualFile) error {
	_, err := c.send(ctx, http.MethodDelete, c.fileURL(s, vf.InternalName), c.newJournal(eventDelete, &vf))
	return err
}

func (c *apiClient) rename(ctx context.Context, s *Share, vf VirtualFile, newName string) error {
	vf.ShareID, vf.PublicName = s.ID, newName
	_, err := c.send(ctx, http.MethodPut, c.fileURL(s, vf.InternalName), c.newJournal(eventRename, &vf))
	return err
}

func (c *apiClient) move(ctx context.Context, s *Share, vf VirtualFile, newParentID string) error {
	vf.ShareID, vf.ParrentID = s.ID, newParentID
	_, err := c.send(ctx, http.MethodPut, c.fileURL(s, vf.InternalName), c.newJournal(eventMove, &vf))
	return err
}
