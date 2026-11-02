package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/text/unicode/norm"
	_ "modernc.org/sqlite"
)

const (
	treeFormat        = 3
	treeConnections   = 2
	shareListInterval = 15 * time.Minute
	reconcileInterval = 24 * time.Hour
	wakeDelay         = 2 * time.Second
	freeMemoryRows    = 1000             // a batch big enough to be worth handing back to the OS
	catchupTimeout    = 15 * time.Minute // a whole share; the server takes about 2ms per entry
)

func treeDir() string { return filepath.Join(cacheBaseDir(), "tree") }

// normID compares ids that the server writes both with and without dashes.
func normID(id string) string { return strings.ToLower(strings.ReplaceAll(id, "-", "")) }

// treeEntry holds only what the mount reads. Entries are replaced, never modified,
// so they can be used without the tree's lock. Rename, move and delete send the full
// record, which they fetch from the server.
type treeEntry struct {
	ID, Parent, Name, Version string
	User, LockedBy            string
	Size, MTime, Created      int64 // times in Unix milliseconds
	Dir, Locked, Deleted      bool
}

func entryOf(vf *VirtualFile) *treeEntry {
	e := &treeEntry{
		ID: vf.InternalName, Parent: vf.ParrentID, Name: norm.NFC.String(vf.PublicName), Version: vf.UploadName,
		User: vf.User, Size: vf.size(), MTime: unixMilli(vf.LastWriteTime), Created: unixMilli(vf.CreationTime),
		Dir: vf.isDir(), Deleted: vf.Deleted,
	}
	var lock struct {
		IsLocked bool
		LockedBy string
	}
	if len(vf.FileLock) > 0 && json.Unmarshal(vf.FileLock, &lock) == nil && lock.IsLocked {
		e.Locked, e.LockedBy = true, lock.LockedBy
	}
	return e
}

func unixMilli(t *netTime) int64 {
	if t == nil || t.IsZero() {
		return 0
	}
	return t.UnixMilli()
}

func fromMilli(ms int64) *netTime {
	if ms == 0 {
		return nil
	}
	return &netTime{time.UnixMilli(ms).UTC()}
}

func (e *treeEntry) record() VirtualFile {
	attrs := fileAttrs(attrNormal)
	if e.Dir {
		attrs = attrDirectory
	}
	size := e.Size
	vf := VirtualFile{
		InternalName: e.ID, ParrentID: e.Parent, PublicName: e.Name, UploadName: e.Version,
		User: e.User, IsFile: !e.Dir, EndOfFile: &size, Attributes: &attrs,
		LastWriteTime: fromMilli(e.MTime), CreationTime: fromMilli(e.Created),
	}
	if e.Locked {
		vf.FileLock, _ = json.Marshal(map[string]any{"IsLocked": true, "LockedBy": e.LockedBy})
	}
	return vf
}

// treeDB is the on-disk tree of all shares. Other programs may read it; the schema is
// versioned through PRAGMA user_version, and a tree of another version is rebuilt.
type treeDB struct {
	db                   *sql.DB
	child, kids, get     *sql.Stmt
	upsert, drop         *sql.Stmt
	count, state, forget *sql.Stmt
}

const treeSchema = `
CREATE TABLE files (
	share_id   TEXT NOT NULL,
	key        TEXT NOT NULL, -- id without dashes, lower case
	id         TEXT NOT NULL,
	parent_key TEXT NOT NULL,
	parent_id  TEXT NOT NULL,
	name       TEXT NOT NULL,
	version    TEXT NOT NULL,
	user       TEXT NOT NULL,
	size       INTEGER NOT NULL,
	mtime      INTEGER NOT NULL, -- Unix milliseconds
	created    INTEGER NOT NULL, -- Unix milliseconds
	is_dir     INTEGER NOT NULL,
	locked     INTEGER NOT NULL,
	locked_by  TEXT NOT NULL,
	PRIMARY KEY (share_id, key)
) WITHOUT ROWID;
CREATE INDEX files_children ON files (share_id, parent_key, name);
CREATE INDEX files_mtime ON files (mtime);
CREATE TABLE shares (
	share_id      TEXT PRIMARY KEY,
	tick          INTEGER NOT NULL,
	reconciled_at INTEGER NOT NULL
) WITHOUT ROWID;
`

const treeColumns = "id, parent_id, name, version, user, size, mtime, created, is_dir, locked, locked_by"

func scanEntry(row interface{ Scan(...any) error }) (*treeEntry, error) {
	e := &treeEntry{}
	err := row.Scan(&e.ID, &e.Parent, &e.Name, &e.Version, &e.User, &e.Size, &e.MTime, &e.Created, &e.Dir, &e.Locked, &e.LockedBy)
	return e, err
}

func openTreeDB(path string) (*treeDB, error) {
	// SQLite gives its -wal and -shm files the permissions of the database.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	f.Close()
	dsn := "file:" + path + "?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)&_pragma=cache_size(-512)"
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	db.SetMaxOpenConns(treeConnections)
	db.SetMaxIdleConns(treeConnections) // reopening a connection costs milliseconds per lookup
	if err := migrateTree(db); err != nil {
		db.Close()
		return nil, err
	}
	t := &treeDB{db: db}
	// Named indexes: without statistics the planner walks the whole share instead.
	for _, st := range []struct {
		to    **sql.Stmt
		query string
	}{
		{&t.child, "SELECT " + treeColumns + " FROM files INDEXED BY files_children WHERE share_id = ? AND parent_key = ? AND name = ? LIMIT 1"},
		{&t.kids, "SELECT " + treeColumns + " FROM files INDEXED BY files_children WHERE share_id = ? AND parent_key = ?"},
		{&t.get, "SELECT " + treeColumns + " FROM files WHERE share_id = ? AND key = ?"},
		{&t.upsert, "INSERT OR REPLACE INTO files (share_id, key, id, parent_key, parent_id, name, version, user, size, mtime, created, is_dir, locked, locked_by) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"},
		{&t.drop, "DELETE FROM files WHERE share_id = ? AND key = ?"},
		{&t.count, "SELECT COUNT(*) FROM files WHERE share_id = ?"},
		{&t.state, "INSERT OR REPLACE INTO shares (share_id, tick, reconciled_at) VALUES (?, ?, ?)"},
		{&t.forget, "DELETE FROM files WHERE share_id = ?"},
	} {
		if *st.to, err = db.Prepare(st.query); err != nil {
			db.Close()
			return nil, err
		}
	}
	return t, nil
}

func migrateTree(db *sql.DB) error {
	var version int
	if err := db.QueryRow("PRAGMA user_version").Scan(&version); err != nil {
		return err
	}
	if version == treeFormat {
		return nil
	}
	script := "DROP TABLE IF EXISTS files; DROP TABLE IF EXISTS shares;" + treeSchema + fmt.Sprintf("PRAGMA user_version = %d;", treeFormat)
	_, err := db.Exec(script)
	return err
}

func (d *treeDB) put(exec *sql.Stmt, share string, e *treeEntry) error {
	_, err := exec.Exec(share, normID(e.ID), e.ID, normID(e.Parent), e.Parent, e.Name, e.Version, e.User,
		e.Size, e.MTime, e.Created, e.Dir, e.Locked, e.LockedBy)
	return err
}

// removeShare forgets a share the user lost access to.
func (d *treeDB) removeShare(share string) error {
	if _, err := d.forget.Exec(share); err != nil {
		return err
	}
	_, err := d.db.Exec("DELETE FROM shares WHERE share_id = ?", share)
	return err
}

// shareTree is one share's part of the tree. Only sync state lives in memory.
type shareTree struct {
	db    *treeDB
	share string
	ready atomic.Bool

	mu   sync.Mutex
	tick int64

	// Local changes made during a catch-up win over its rows, which may predate them.
	syncing bool
	touched map[string]bool
	// Entries whose delete is queued. Catch-up would bring them back until it is sent.
	hidden map[string]bool

	reconciledAt time.Time
}

func newShareTree(db *treeDB, share string) *shareTree {
	return &shareTree{db: db, share: share, touched: map[string]bool{}, hidden: map[string]bool{}}
}

// loadTrees returns the trees of all shares the database knows.
func (d *treeDB) loadTrees() (map[string]*shareTree, error) {
	rows, err := d.db.Query("SELECT share_id, tick, reconciled_at FROM shares")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	trees := map[string]*shareTree{}
	for rows.Next() {
		var id string
		var tick, reconciled int64
		if err := rows.Scan(&id, &tick, &reconciled); err != nil {
			return nil, err
		}
		t := newShareTree(d, id)
		t.tick, t.reconciledAt = tick, time.UnixMilli(reconciled)
		t.ready.Store(true)
		trees[id] = t
	}
	return trees, rows.Err()
}

func (t *shareTree) isReady() bool { return t.ready.Load() }

// children lists a folder. If rows ever disagree on a name, one of them is shown.
func (t *shareTree) children(parentID string) []*treeEntry {
	rows, err := t.db.kids.Query(t.share, normID(parentID))
	if err != nil {
		log.Printf("tree: listing a folder: %v", err)
		return nil
	}
	defer rows.Close()
	var out []*treeEntry
	seen := map[string]bool{}
	for rows.Next() {
		e, err := scanEntry(rows)
		if err != nil {
			log.Printf("tree: listing a folder: %v", err)
			return out
		}
		if !seen[e.Name] {
			seen[e.Name] = true
			out = append(out, e)
		}
	}
	return out
}

func (t *shareTree) child(parentID, name string) *treeEntry {
	e, err := scanEntry(t.db.child.QueryRow(t.share, normID(parentID), name))
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			log.Printf("tree: finding a file: %v", err)
		}
		return nil
	}
	return e
}

func (t *shareTree) entry(key string) *treeEntry {
	e, err := scanEntry(t.db.get.QueryRow(t.share, key))
	if err != nil {
		if !errors.Is(err, sql.ErrNoRows) {
			log.Printf("tree: reading a file: %v", err)
		}
		return nil
	}
	return e
}

func (t *shareTree) get(id string) (VirtualFile, bool) {
	e := t.entry(normID(id))
	if e == nil {
		return VirtualFile{}, false
	}
	return e.record(), true
}

func (t *shareTree) put(vf VirtualFile) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if err := t.db.put(t.db.upsert, t.share, entryOf(&vf)); err != nil {
		log.Printf("tree: saving a change: %v", err)
	}
	t.noteLocal(vf.InternalName)
}

func (t *shareTree) remove(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, err := t.db.drop.Exec(t.share, normID(id)); err != nil {
		log.Printf("tree: saving a delete: %v", err)
	}
	t.noteLocal(id)
}

// hide removes an entry whose delete is queued, and keeps it out until unhide.
func (t *shareTree) hide(id string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if _, err := t.db.drop.Exec(t.share, normID(id)); err != nil {
		log.Printf("tree: saving a delete: %v", err)
	}
	t.hidden[normID(id)] = true
	t.noteLocal(id)
}

// unhide ends a queued delete. A refused one passes its entry back.
func (t *shareTree) unhide(id string, restore *VirtualFile) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.hidden, normID(id))
	if restore != nil {
		if err := t.db.put(t.db.upsert, t.share, entryOf(restore)); err != nil {
			log.Printf("tree: saving a change: %v", err)
		}
	}
	t.noteLocal(id)
}

func (t *shareTree) noteLocal(id string) {
	if t.syncing {
		t.touched[normID(id)] = true
	}
}

func (t *shareTree) begin() int64 {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.syncing = true
	return t.tick
}

func (t *shareTree) abort() {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.syncing = false
	clear(t.touched)
}

func (t *shareTree) reconcileDue() bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	return time.Since(t.reconciledAt) > reconcileInterval
}

// finish applies catch-up rows and the new tick in one transaction, so a crash can
// not leave them out of step. A full listing also drops whatever it no longer
// contains, which is the only way to learn of permanent deletes.
func (t *shareTree) finish(s *Share, rows []*treeEntry, tick int64, full bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	defer func() {
		t.syncing = false
		clear(t.touched)
	}()
	if err := t.commit(s, rows, tick, full); err != nil {
		log.Printf("sync: %q: saving the tree: %v", s.Name, err) // the next pass retries from the old tick
	}
}

func (t *shareTree) commit(s *Share, rows []*treeEntry, tick int64, full bool) error {
	if !full && len(rows) == 0 && tick == t.tick && t.ready.Load() {
		return nil // an idle poll, which should not touch the disk
	}
	if full {
		live, known := 0, 0
		for _, e := range rows {
			if !e.Deleted {
				live++
			}
		}
		if err := t.db.count.QueryRow(t.share).Scan(&known); err != nil {
			return err
		}
		if live == 0 && known > 0 {
			// Same guard as the Windows client: never wipe a tree on an empty answer.
			log.Printf("sync: %q: full listing came back empty, keeping the local tree", s.Name)
			full = false
		}
	}
	reconciled := t.reconciledAt
	if full {
		reconciled = time.Now()
	}
	var kept []*treeEntry
	if full {
		for key := range t.touched {
			if e := t.entry(key); e != nil {
				kept = append(kept, e)
			}
		}
	}
	tx, err := t.db.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	upsert, drop := tx.Stmt(t.db.upsert), tx.Stmt(t.db.drop)
	if full {
		if _, err := tx.Stmt(t.db.forget).Exec(t.share); err != nil {
			return err
		}
		for _, e := range kept {
			if err := t.db.put(upsert, t.share, e); err != nil {
				return err
			}
		}
	}
	root := normID(s.rootParentID())
	for _, e := range rows {
		key := normID(e.ID)
		if key == "" || key == root || key == normID(e.Parent) || t.touched[key] || t.hidden[key] {
			continue
		}
		if e.Deleted {
			if _, err := drop.Exec(t.share, key); err != nil {
				return err
			}
		} else if e.Name != "" && !strings.Contains(e.Name, "/") {
			if err := t.db.put(upsert, t.share, e); err != nil {
				return err
			}
		}
	}
	if _, err := tx.Stmt(t.db.state).Exec(t.share, tick, reconciled.UnixMilli()); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	t.tick, t.reconciledAt = tick, reconciled
	t.ready.Store(true)
	return nil
}

type savedShares struct {
	Shares []*Share
	Tokens map[string]string
}

// treeSync keeps the trees of all shares current by polling catch-up. The server
// here uses the legacy SignalR hub, so changes arrive with the next poll.
type treeSync struct {
	c        *apiClient
	dir      string
	db       *treeDB
	interval time.Duration

	mu       sync.Mutex
	shares   []*Share
	tokens   map[string]string
	trees    map[string]*shareTree
	woken    map[string]bool
	listedAt time.Time
	offline  bool

	wakeCh chan struct{}
}

func newTreeSync(c *apiClient, interval time.Duration) (*treeSync, error) {
	s := &treeSync{
		c: c, dir: treeDir(), interval: interval,
		tokens: map[string]string{}, trees: map[string]*shareTree{}, woken: map[string]bool{},
		wakeCh: make(chan struct{}, 1),
	}
	if err := os.MkdirAll(s.dir, 0o700); err != nil {
		return nil, err
	}
	// Trees of earlier versions were one file per share.
	if old, err := filepath.Glob(filepath.Join(s.dir, "*.json.gz")); err == nil {
		for _, path := range old {
			_ = os.Remove(path)
		}
	}
	var err error
	if s.db, err = openTreeDB(filepath.Join(s.dir, "tree.db")); err != nil {
		return nil, fmt.Errorf("opening the file tree: %w", err)
	}
	if s.trees, err = s.db.loadTrees(); err != nil {
		return nil, fmt.Errorf("reading the file tree: %w", err)
	}
	var saved savedShares
	if data, err := os.ReadFile(s.sharesPath()); err == nil && json.Unmarshal(data, &saved) == nil {
		s.shares = saved.Shares
		if saved.Tokens != nil {
			s.tokens = saved.Tokens
		}
	}
	debug.FreeOSMemory()
	return s, nil
}

func (s *treeSync) sharesPath() string { return filepath.Join(s.dir, "shares.json") }

func (s *treeSync) sharesSnapshot() []*Share {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]*Share(nil), s.shares...)
}

// tree returns the share's tree once it can answer for the whole share.
func (s *treeSync) tree(id string) *shareTree {
	s.mu.Lock()
	t := s.trees[id]
	s.mu.Unlock()
	if t == nil || !t.isReady() {
		return nil
	}
	return t
}

// wake asks for a catch-up soon, after a local change.
func (s *treeSync) wake(id string) {
	s.mu.Lock()
	s.woken[id] = true
	s.mu.Unlock()
	select {
	case s.wakeCh <- struct{}{}:
	default:
	}
}

func (s *treeSync) refreshShares(ctx context.Context) error {
	shares, err := s.c.allShares(ctx)
	if len(shares) == 0 {
		return err
	}
	tokens := map[string]string{}
	var tokenErr error
	for _, domain := range s.c.domains {
		found, err := s.c.shareTokens(ctx, domain)
		if err != nil {
			tokenErr = err
		}
		for id, token := range found {
			tokens[id] = token
		}
	}
	s.mu.Lock()
	for id, token := range s.tokens {
		if _, ok := tokens[id]; !ok && tokenErr != nil {
			tokens[id] = token
		}
	}
	gone := map[string]bool{}
	if err == nil {
		for _, old := range s.shares {
			gone[old.ID] = true
		}
		for _, share := range shares {
			delete(gone, share.ID)
		}
	}
	for id := range gone {
		delete(s.trees, id)
		if err := s.db.removeShare(id); err != nil {
			log.Printf("sync: forgetting a share: %v", err)
		}
	}
	for _, old := range s.shares {
		if err != nil && !containsShare(shares, old.ID) {
			shares = append(shares, old)
		}
	}
	s.shares, s.tokens, s.listedAt = shares, tokens, time.Now()
	saved := savedShares{Shares: shares, Tokens: tokens}
	s.mu.Unlock()

	data, marshalErr := json.Marshal(saved)
	if marshalErr != nil {
		return marshalErr
	}
	tmp := s.sharesPath() + ".tmp"
	if err := os.WriteFile(tmp, data, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmp, s.sharesPath()); err != nil {
		return err
	}
	if tokenErr != nil {
		return tokenErr
	}
	return err
}

func containsShare(shares []*Share, id string) bool {
	for _, s := range shares {
		if s.ID == id {
			return true
		}
	}
	return false
}

// pull fetches rows after a tick, page by page, and reports how far it got.
func (s *treeSync) pull(ctx context.Context, share *Share, token string, from, start int64) (rows []*treeEntry, shareTick, reached int64, stalled bool, err error) {
	reached = from
	for {
		page, err := s.c.catchup(ctx, share, token, reached, start)
		if err != nil {
			return nil, 0, 0, false, err
		}
		// Compact each page now, so the raw rows of a big share never pile up.
		for i := range page.RfVirtualFiles {
			rows = append(rows, entryOf(&page.RfVirtualFiles[i]))
		}
		shareTick = page.ShareTick
		switch {
		case len(page.RfVirtualFiles) == 0:
			return rows, shareTick, shareTick, false, nil
		case page.LargestTick >= shareTick:
			return rows, shareTick, shareTick, false, nil
		case page.LargestTick <= reached:
			log.Printf("sync: %q: catch-up stopped advancing at tick %d of %d", share.Name, reached, shareTick)
			return rows, shareTick, reached, true, nil
		}
		reached = page.LargestTick
		if start != 0 {
			start = reached
		}
	}
}

// syncShare mirrors SyncVirtualFilesCommandHandler. Permanent deletes leave gaps in
// the ticks instead of tombstones, so a gap triggers a full listing, at most daily.
func (s *treeSync) syncShare(ctx context.Context, share *Share) error {
	s.mu.Lock()
	token := s.tokens[share.ID]
	t := s.trees[share.ID]
	if t == nil && token != "" {
		t = newShareTree(s.db, share.ID)
		s.trees[share.ID] = t
	}
	s.mu.Unlock()
	if token == "" {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, catchupTimeout)
	defer cancel()

	began := time.Now()
	start := t.begin()
	rows, shareTick, reached, stalled, err := s.pull(ctx, share, token, start, start)
	if err != nil {
		t.abort()
		return err
	}
	full := false
	distinct := map[string]bool{}
	for _, e := range rows {
		distinct[e.ID] = true
	}
	if !stalled && shareTick-start > int64(len(distinct)) {
		if start == 0 {
			full = true
		} else if t.reconcileDue() {
			all, allTick, _, allStalled, err := s.pull(ctx, share, token, 0, 0)
			if err != nil {
				t.abort()
				return err
			}
			if !allStalled {
				rows, reached, full = all, allTick, true
			}
		}
	}
	first := !t.isReady()
	t.finish(share, rows, reached, full)
	if first {
		log.Printf("sync: %q: %d entries in %s", share.Name, len(rows), time.Since(began).Round(time.Millisecond))
	}
	if len(rows) > freeMemoryRows {
		rows = nil
		debug.FreeOSMemory()
	}
	return nil
}

func (s *treeSync) syncShares(ctx context.Context, ids map[string]bool) {
	var firstErr error
	for _, share := range s.sharesSnapshot() {
		if ids != nil && !ids[share.ID] {
			continue
		}
		if err := s.syncShare(ctx, share); err != nil && firstErr == nil && ctx.Err() == nil {
			firstErr = err
		}
	}
	s.report(firstErr)
}

func (s *treeSync) report(err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err != nil && !s.offline {
		log.Printf("sync: server unreachable, serving the local tree: %v", err)
		s.listedAt = time.Time{} // catch-up tokens may be stale, so fetch new ones first
	}
	if err == nil && s.offline {
		log.Printf("sync: back online")
	}
	s.offline = err != nil
}

func (s *treeSync) takeWoken() map[string]bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	ids := s.woken
	s.woken = map[string]bool{}
	return ids
}

// run blocks until ctx ends, then closes the tree.
func (s *treeSync) run(ctx context.Context) {
	defer s.db.db.Close()
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		var ids map[string]bool
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		case <-s.wakeCh:
			select {
			case <-ctx.Done():
				return
			case <-time.After(wakeDelay):
			}
			ids = s.takeWoken()
		}
		s.mu.Lock()
		listDue := time.Since(s.listedAt) > shareListInterval
		s.mu.Unlock()
		if listDue {
			if err := s.refreshShares(ctx); err != nil {
				s.report(err)
			}
		}
		s.syncShares(ctx, ids)
		if ids == nil {
			timer.Reset(s.interval)
		}
	}
}
