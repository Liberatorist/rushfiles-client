package main

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type driveOptions struct {
	listTTL   time.Duration
	cacheDir  string
	cacheSize int64
	sync      *treeSync // nil with --no-sync
	// uploadDelay is how long after a save the edit uploads; zero uploads during close.
	uploadDelay time.Duration
	uploads     sync.WaitGroup // background uploads not yet finished
}

// node is the mount root (share == nil), a share root, a folder or a file.
type node struct {
	fs.Inode
	c     *apiClient
	opts  *driveOptions
	share *Share

	mu       sync.Mutex
	vf       VirtualFile
	children map[string]VirtualFile
	shares   map[string]*Share
	listedAt time.Time

	// Local edit state. io blocks writes while a snapshot uploads.
	io       sync.RWMutex
	work     *os.File
	handles  int
	dirty    atomic.Bool
	base     string
	modified time.Time
	// scrapWork marks a work file that holds no server version, so it must not be cached.
	scrapWork bool
	// pending is the scheduled background upload; it holds one of the handles.
	pending  *time.Timer
	failures int
}

var (
	_ fs.NodeLookuper  = (*node)(nil)
	_ fs.NodeReaddirer = (*node)(nil)
	_ fs.NodeGetattrer = (*node)(nil)
	_ fs.NodeSetattrer = (*node)(nil)
	_ fs.NodeOpener    = (*node)(nil)
	_ fs.NodeCreater   = (*node)(nil)
	_ fs.NodeMkdirer   = (*node)(nil)
	_ fs.NodeUnlinker  = (*node)(nil)
	_ fs.NodeRmdirer   = (*node)(nil)
	_ fs.NodeRenamer   = (*node)(nil)
	_ fs.NodeStatfser  = (*node)(nil)
)

func toErrno(err error) syscall.Errno {
	if err == nil {
		return 0
	}
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno
	}
	if errors.Is(err, context.Canceled) {
		return syscall.EINTR
	}
	log.Printf("error: %v", err)
	return syscall.EIO
}

func inoFor(id string) uint64 {
	sum := sha256.Sum256([]byte(id))
	return binary.LittleEndian.Uint64(sum[:8]) | 1<<63
}

// parentID is the id children of this node hang off; empty for files.
func (n *node) parentID() string {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.parentIDLocked()
}

func (n *node) parentIDLocked() string {
	if n.vf.InternalName == "" {
		return n.share.rootParentID()
	}
	if n.vf.isDir() {
		return n.vf.InternalName
	}
	return ""
}

// tree returns this share's local tree when it is ready to be used.
func (n *node) tree() *shareTree {
	if n.opts.sync == nil || n.share == nil {
		return nil
	}
	return n.opts.sync.tree(n.share.ID)
}

// changed records a local change in the tree and asks for a catch-up.
func (n *node) changed(update func(t *shareTree)) {
	if t := n.tree(); t != nil {
		update(t)
		n.opts.sync.wake(n.share.ID)
	}
}

// list loads the live listing, used until the first catch-up is done and with
// --no-sync. Remote changes then appear once listTTL lapses. With a tree, folders
// keep no listing of their own, since that would be a second copy of the tree.
func (n *node) list(ctx context.Context) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.opts.sync != nil {
		if n.share == nil {
			if shares := n.opts.sync.sharesSnapshot(); len(shares) > 0 {
				n.setShares(shares)
				return nil
			}
		} else if n.tree() != nil {
			n.children = nil
			return nil
		}
	}
	if !n.listedAt.IsZero() && time.Since(n.listedAt) < n.opts.listTTL {
		return nil
	}
	if n.share == nil {
		return n.listShares(ctx)
	}
	kids, err := n.c.children(ctx, n.share, n.parentIDLocked())
	if err != nil {
		return err
	}
	byName := make(map[string]VirtualFile, len(kids))
	for _, k := range kids {
		if !k.Deleted && k.PublicName != "" && !strings.Contains(k.PublicName, "/") {
			byName[k.PublicName] = k
		}
	}
	n.children, n.listedAt = byName, time.Now()
	return nil
}

func (n *node) listShares(ctx context.Context) error {
	shares, err := n.c.shares(ctx)
	if err != nil {
		return err
	}
	n.setShares(shares)
	return nil
}

func (n *node) setShares(shares []*Share) {
	sort.Slice(shares, func(i, j int) bool { return shares[i].ID < shares[j].ID })
	n.shares = make(map[string]*Share, len(shares))
	for _, s := range shares {
		name := strings.ReplaceAll(s.Name, "/", "_")
		if _, taken := n.shares[name]; taken {
			name += " (" + s.ID[:min(8, len(s.ID))] + ")"
		}
		n.shares[name] = s
	}
	n.listedAt = time.Now()
}

// trim asks the kernel to drop entries of this subtree, so that anything scanning
// the drive does not pin a node per path. It only drops what nothing is using, and
// the next access looks the entry up again. Nodes being edited stay. It returns how
// many entries it dropped, deepest first, as a folder can't go before its contents.
func (n *node) trim() int {
	dropped := 0
	for name, child := range n.Children() {
		c := child.Operations().(*node)
		dropped += c.trim()
		c.mu.Lock()
		editing := c.work != nil
		c.mu.Unlock()
		if !editing && n.NotifyEntry(name) == 0 {
			dropped++
		}
	}
	return dropped
}

func (n *node) invalidate() {
	n.mu.Lock()
	n.listedAt = time.Time{}
	n.mu.Unlock()
}

func (n *node) fillAttr(a *fuse.Attr) {
	n.mu.Lock()
	defer n.mu.Unlock()
	a.Blksize = 1 << 16
	if n.share == nil || n.vf.InternalName == "" || n.vf.isDir() {
		a.Mode = syscall.S_IFDIR | 0o755
		a.Nlink = 2
		if !n.vf.mtime().IsZero() {
			a.SetTimes(nil, ptr(n.vf.mtime()), nil)
		}
		return
	}
	a.Mode = syscall.S_IFREG | 0o644
	if n.vf.lockedByOther(n.c.userID) {
		a.Mode = syscall.S_IFREG | 0o444
	}
	a.Nlink = 1
	a.Size = uint64(n.vf.size())
	mtime := n.vf.mtime()
	if n.work != nil {
		if st, err := n.work.Stat(); err == nil {
			a.Size = uint64(st.Size())
		}
		if !n.modified.IsZero() {
			mtime = n.modified
		}
	}
	a.Blocks = (a.Size + 511) / 512
	a.SetTimes(nil, &mtime, nil)
}

func ptr[T any](v T) *T { return &v }

func (n *node) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	n.fillAttr(&out.Attr)
	return 0
}

func (n *node) childNode(ctx context.Context, name string, share *Share, vf VirtualFile, out *fuse.EntryOut) *fs.Inode {
	id, mode := "share:"+share.ID, uint32(syscall.S_IFDIR)
	if vf.InternalName != "" {
		id = vf.InternalName
		if !vf.isDir() {
			mode = syscall.S_IFREG
		}
	}
	child := &node{c: n.c, opts: n.opts, share: share, vf: vf}
	inode := n.NewInode(ctx, child, fs.StableAttr{Mode: mode, Ino: inoFor(id)})
	// NewInode hands back the existing node for a known id; refresh its record.
	live := inode.Operations().(*node)
	if live != child {
		live.mu.Lock()
		if !live.dirty.Load() {
			live.vf = vf
		}
		live.mu.Unlock()
	}
	live.fillAttr(&out.Attr)
	return inode
}

func (n *node) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if n.share != nil && n.parentID() == "" {
		return nil, syscall.ENOTDIR
	}
	if err := n.list(ctx); err != nil {
		return nil, toErrno(err)
	}
	n.mu.Lock()
	share, isShare := n.shares[name]
	n.mu.Unlock()
	if isShare {
		return n.childNode(ctx, name, share, VirtualFile{}, out), 0
	}
	if vf, ok := n.child(name); ok {
		return n.childNode(ctx, name, n.share, vf, out), 0
	}
	return nil, syscall.ENOENT
}

// child finds an entry after list, in the tree or else in the live listing.
func (n *node) child(name string) (VirtualFile, bool) {
	if t := n.tree(); t != nil {
		if e := t.child(n.parentID(), name); e != nil {
			return e.record(), true
		}
		return VirtualFile{}, false
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	vf, ok := n.children[name]
	return vf, ok
}

func (n *node) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	if err := n.list(ctx); err != nil {
		return nil, toErrno(err)
	}
	var entries []fuse.DirEntry
	add := func(name, id string, dir bool) {
		mode := uint32(syscall.S_IFREG)
		if dir {
			mode = syscall.S_IFDIR
		}
		entries = append(entries, fuse.DirEntry{Name: name, Mode: mode, Ino: inoFor(id)})
	}
	if t := n.tree(); t != nil {
		for _, e := range t.children(n.parentID()) {
			add(e.Name, e.ID, e.Dir)
		}
	}
	n.mu.Lock()
	defer n.mu.Unlock()
	for name, s := range n.shares {
		add(name, "share:"+s.ID, true)
	}
	for name, vf := range n.children {
		add(name, vf.InternalName, vf.isDir())
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name < entries[j].Name })
	return fs.NewListDirStream(entries), 0
}

func (n *node) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	// The API exposes no single free-space figure; report a large volume so tools proceed.
	const blocks = 1 << 36
	out.Bsize, out.Frsize, out.NameLen = 4096, 4096, 255
	out.Blocks, out.Bfree, out.Bavail = blocks, blocks, blocks
	return 0
}

func (n *node) cachePath(vf *VirtualFile) string {
	sum := sha256.Sum256([]byte(vf.InternalName + "|" + vf.version()))
	return filepath.Join(n.opts.cacheDir, hex.EncodeToString(sum[:]))
}

// fetch returns a cached copy of the current server version, downloading the
// whole file on first use like the Windows client's hydration does.
func (n *node) fetch(ctx context.Context) (string, error) {
	n.mu.Lock()
	vf := n.vf
	n.mu.Unlock()
	path := n.cachePath(&vf)
	if _, err := os.Stat(path); err == nil {
		now := time.Now()
		_ = os.Chtimes(path, now, now)
		return path, nil
	}
	tmp, err := os.CreateTemp(n.opts.cacheDir, "download-*")
	if err != nil {
		return "", err
	}
	defer os.Remove(tmp.Name())
	defer tmp.Close()
	if vf.size() > 0 {
		if err := n.c.download(ctx, n.share, vf.contentID(), tmp); err != nil {
			return "", err
		}
	}
	if err := os.Rename(tmp.Name(), path); err != nil {
		return "", err
	}
	go pruneCache(n.opts.cacheDir, n.opts.cacheSize)
	return path, nil
}

func pruneCache(dir string, limit int64) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return
	}
	type item struct {
		path string
		size int64
		at   time.Time
	}
	var items []item
	var total int64
	for _, e := range entries {
		if info, err := e.Info(); err == nil && info.Mode().IsRegular() && !strings.HasPrefix(e.Name(), "download-") {
			items = append(items, item{filepath.Join(dir, e.Name()), info.Size(), info.ModTime()})
			total += info.Size()
		}
	}
	sort.Slice(items, func(i, j int) bool { return items[i].at.Before(items[j].at) })
	for _, it := range items {
		if total <= limit {
			return
		}
		if os.Remove(it.path) == nil {
			total -= it.size
		}
	}
}

// openWork gives the node a private writable copy shared by all its handles.
func (n *node) openWork(ctx context.Context, truncate bool) error {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.work != nil {
		return nil
	}
	f, err := os.CreateTemp(filepath.Join(n.opts.cacheDir, "work"), "edit-*")
	if err != nil {
		return err
	}
	if !truncate && n.vf.size() > 0 {
		n.mu.Unlock()
		path, err := n.fetch(ctx)
		n.mu.Lock()
		if err == nil {
			var src *os.File
			if src, err = os.Open(path); err == nil {
				defer src.Close()
				_, err = io.Copy(f, src)
			}
		}
		if err != nil {
			f.Close()
			os.Remove(f.Name())
			return err
		}
		if n.work != nil { // another handle won while mu was released
			f.Close()
			os.Remove(f.Name())
			return nil
		}
	}
	n.work, n.base, n.scrapWork = f, n.vf.version(), false
	return nil
}

const (
	writeFlags    = syscall.O_WRONLY | syscall.O_RDWR
	truncateGrace = 2 * time.Second
)

func (n *node) Open(ctx context.Context, flags uint32) (fs.FileHandle, uint32, syscall.Errno) {
	// The kernel may still hold an older entry than the tree does.
	if t := n.tree(); t != nil {
		if vf, ok := t.get(n.vf.InternalName); ok {
			n.mu.Lock()
			if n.work == nil {
				n.vf = vf
			}
			n.mu.Unlock()
		}
	}
	if flags&writeFlags == 0 {
		// A scrapped work file lingers until the async release; it isn't the file's content.
		n.mu.Lock()
		editing := n.work != nil && !n.scrapWork
		n.mu.Unlock()
		if !editing {
			path, err := n.fetch(ctx)
			if err != nil {
				return nil, 0, toErrno(err)
			}
			f, err := os.Open(path)
			if err != nil {
				return nil, 0, toErrno(err)
			}
			return &readHandle{f: f}, 0, 0
		}
	} else {
		if n.c.readOnly {
			return nil, 0, syscall.EROFS
		}
		if n.locked() {
			return nil, 0, syscall.EACCES
		}
	}
	if err := n.openWork(ctx, flags&syscall.O_TRUNC != 0); err != nil {
		return nil, 0, toErrno(err)
	}
	if flags&syscall.O_TRUNC != 0 {
		n.touch()
	}
	n.mu.Lock()
	n.handles++
	n.mu.Unlock()
	return &editHandle{n: n}, 0, 0
}

// locked reports whether another user holds a lock, as of the last sync. Rename,
// move and delete check the fresh server record instead, and the server has the
// final word on uploads.
func (n *node) locked() bool {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.vf.lockedByOther(n.c.userID)
}

func (n *node) touch() {
	n.mu.Lock()
	n.modified = time.Now()
	n.mu.Unlock()
	n.dirty.Store(true)
}

// conflictName mirrors the usual "name (conflict).ext" sibling convention.
func conflictName(name string) string {
	ext := filepath.Ext(name)
	return fmt.Sprintf("%s (linux conflict %s)%s", strings.TrimSuffix(name, ext), time.Now().Format("2006-01-02 150405"), ext)
}

// flush uploads pending edits. When the server copy moved on since the edit
// started, the edit goes to a conflict sibling so neither version is lost.
func (n *node) flush() error {
	if !n.dirty.Load() {
		return nil
	}
	n.io.Lock()
	defer n.io.Unlock()
	if !n.dirty.Swap(false) {
		return nil
	}
	ctx := context.Background()
	n.mu.Lock()
	vf, work, base, mtime := n.vf, n.work, n.base, n.modified
	n.mu.Unlock()
	if mtime.IsZero() {
		mtime = time.Now()
	}
	st, err := work.Stat()
	if err == nil {
		var remote *VirtualFile
		if remote, err = n.c.virtualFile(ctx, n.share, vf.InternalName); err == nil && remote != nil && remote.version() != base {
			name := conflictName(vf.PublicName)
			log.Printf("%q changed on the server while being edited; saving this edit as %q", vf.PublicName, name)
			err = n.uploadConflict(ctx, &vf, name, work, st.Size(), mtime)
			if err == nil {
				n.mu.Lock()
				n.vf, n.base, n.scrapWork = *remote, remote.version(), true
				n.mu.Unlock()
				n.changed(func(t *shareTree) { t.put(*remote) })
				return nil
			}
		} else if err == nil {
			var uploaded *VirtualFile
			if uploaded, err = n.c.update(ctx, n.share, &vf, work, st.Size(), mtime); err == nil {
				n.afterUpload(uploaded, st.Size(), mtime)
				return nil
			}
		}
	}
	n.dirty.Store(true)
	return err
}

const (
	uploadRetries = 5
	retryDelay    = 2 * time.Second
)

// save uploads pending edits after opts.uploadDelay, so closing a file doesn't wait
// on the network. With a zero delay it uploads now and reports failures to the caller.
func (n *node) save() error {
	if !n.dirty.Load() {
		return nil
	}
	if n.opts.uploadDelay <= 0 {
		return n.flush()
	}
	n.schedule(n.opts.uploadDelay)
	return nil
}

// schedule uploads after delay, restarting the wait when one is already pending
// so a burst of saves becomes one upload. The pending upload holds a handle, which
// keeps the work file, and what reads see, until it is done.
func (n *node) schedule(delay time.Duration) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.pending == nil || !n.pending.Stop() {
		n.handles++
		n.opts.uploads.Add(1)
	}
	var t *time.Timer
	t = time.AfterFunc(delay, func() {
		n.mu.Lock()
		if n.pending == t {
			n.pending = nil
		}
		n.mu.Unlock()
		n.upload()
	})
	n.pending = t
}

// upload runs a scheduled upload, retrying when the server could not be reached.
// Errors the server answered with are final.
func (n *node) upload() {
	defer n.opts.uploads.Done()
	err := n.flush()
	n.mu.Lock()
	name := n.vf.PublicName
	if err == nil {
		n.failures = 0
	} else {
		n.failures++
	}
	failures := n.failures
	n.mu.Unlock()
	if err != nil {
		var errno syscall.Errno
		if failures < uploadRetries && !errors.As(err, &errno) {
			log.Printf("upload of %q: %v; retrying", name, err)
			n.schedule(retryDelay * time.Duration(failures))
		} else {
			log.Printf("upload of %q: %v", name, err)
			n.mu.Lock()
			n.failures = 0
			n.mu.Unlock()
		}
	}
	n.release()
}

// discard cancels a scheduled upload of a file that is being deleted or replaced.
func (n *node) discard() {
	n.mu.Lock()
	cancelled := n.pending != nil && n.pending.Stop()
	if cancelled {
		n.pending, n.scrapWork = nil, true
		n.dirty.Store(false)
	}
	n.mu.Unlock()
	if cancelled {
		n.release()
		n.opts.uploads.Done()
	}
}

func (n *node) uploadConflict(ctx context.Context, vf *VirtualFile, name string, src io.ReaderAt, size int64, mtime time.Time) error {
	_, uploadURL, err := n.c.create(ctx, n.share, vf.ParrentID, name, attrNormal, size, mtime)
	if err != nil || size == 0 {
		return err
	}
	if uploadURL == "" {
		return fmt.Errorf("server gave no upload URL for %q", name)
	}
	_, err = n.c.uploadContent(ctx, n.share, uploadURL, src, size)
	return err
}

func (n *node) afterUpload(uploaded *VirtualFile, size int64, mtime time.Time) {
	// Compare later edits against what the server now reports.
	n.mu.Lock()
	id := n.vf.InternalName
	n.mu.Unlock()
	remote, err := n.c.virtualFile(context.Background(), n.share, id)
	n.mu.Lock()
	n.vf.EndOfFile, n.vf.LastWriteTime = &size, &netTime{mtime}
	if uploaded != nil {
		n.vf.Tick, n.vf.FilehHash, n.vf.UploadName = uploaded.Tick, uploaded.FilehHash, uploaded.UploadName
		if uploaded.EndOfFile != nil {
			n.vf.EndOfFile = uploaded.EndOfFile
		}
	}
	if err == nil && remote != nil {
		n.vf.Tick, n.vf.FilehHash, n.vf.UploadName, n.vf.EndOfFile, n.vf.LastWriteTime = remote.Tick, remote.FilehHash, remote.UploadName, remote.EndOfFile, remote.LastWriteTime
	}
	n.base = n.vf.version()
	vf := n.vf
	n.mu.Unlock()
	n.changed(func(t *shareTree) { t.put(vf) })
}

// release drops the work file with the last handle. Edits that failed to upload
// stay on disk; they are the only copy.
func (n *node) release() {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.handles--
	if n.handles > 0 || n.work == nil {
		return
	}
	path := n.work.Name()
	n.work.Close()
	n.work, n.modified = nil, time.Time{}
	if n.dirty.Load() {
		log.Printf("upload of %q failed; unsaved copy kept at %s", n.vf.PublicName, path)
		n.dirty.Store(false)
		return
	}
	if n.scrapWork || os.Rename(path, n.cachePath(&n.vf)) != nil {
		os.Remove(path)
	}
}

type readHandle struct{ f *os.File }

func (h *readHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	return fuse.ReadResultFd(h.f.Fd(), off, len(dest)), 0
}

func (h *readHandle) Release(ctx context.Context) syscall.Errno { return toErrno(h.f.Close()) }

type editHandle struct{ n *node }

func (h *editHandle) Read(ctx context.Context, dest []byte, off int64) (fuse.ReadResult, syscall.Errno) {
	h.n.mu.Lock()
	f := h.n.work
	h.n.mu.Unlock()
	got, err := f.ReadAt(dest, off)
	if err != nil && !errors.Is(err, io.EOF) {
		return nil, toErrno(err)
	}
	return fuse.ReadResultData(dest[:got]), 0
}

func (h *editHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	h.n.io.RLock()
	defer h.n.io.RUnlock()
	h.n.mu.Lock()
	f := h.n.work
	h.n.mu.Unlock()
	got, err := f.WriteAt(data, off)
	if err != nil {
		return uint32(got), toErrno(err)
	}
	h.n.touch()
	return uint32(got), 0
}

// Flush runs on every close of a descriptor, and shells close a dup of the file
// before writing to it. An empty file waits for Release so no blank version is
// published on the way to the real content.
func (h *editHandle) Flush(ctx context.Context) syscall.Errno {
	h.n.mu.Lock()
	st, err := h.n.work.Stat()
	h.n.mu.Unlock()
	if err == nil && st.Size() == 0 {
		return 0
	}
	return toErrno(h.n.save())
}

// Fsync doesn't wait for the server either: editors sync on every save.
func (h *editHandle) Fsync(ctx context.Context, flags uint32) syscall.Errno {
	return toErrno(h.n.save())
}

func (h *editHandle) Release(ctx context.Context) syscall.Errno {
	h.n.mu.Lock()
	last := h.n.handles == 1
	h.n.mu.Unlock()
	if last {
		if err := h.n.save(); err != nil {
			log.Printf("upload of %q: %v", h.n.vf.PublicName, err)
		}
	}
	h.n.release()
	return 0
}

func (n *node) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if size, ok := in.GetSize(); ok {
		if n.c.readOnly {
			return syscall.EROFS
		}
		if n.locked() {
			return syscall.EACCES
		}
		if err := n.openWork(ctx, size == 0); err != nil {
			return toErrno(err)
		}
		n.io.RLock()
		n.mu.Lock()
		err := n.work.Truncate(int64(size))
		n.mu.Unlock()
		n.io.RUnlock()
		if err != nil {
			return toErrno(err)
		}
		n.touch()
		if _, open := f.(*editHandle); !open {
			// Shell redirection arrives as truncate then open, since go-fuse lacks
			// atomic O_TRUNC. Wait so the writer's close uploads one complete version.
			n.schedule(truncateGrace)
		}
	}
	if mtime, ok := in.GetMTime(); ok {
		n.mu.Lock()
		if n.work != nil {
			n.modified = mtime
		}
		n.mu.Unlock()
	}
	n.fillAttr(&out.Attr)
	return 0
}

func (n *node) writable() syscall.Errno {
	if n.share == nil {
		return syscall.EACCES
	}
	if n.c.readOnly {
		return syscall.EROFS
	}
	return 0
}

func (n *node) add(name string, vf *VirtualFile) {
	if n.tree() != nil {
		n.changed(func(t *shareTree) { t.put(*vf) })
		return
	}
	n.mu.Lock()
	if n.children == nil {
		n.children = map[string]VirtualFile{}
	}
	n.children[name] = *vf
	n.mu.Unlock()
}

func (n *node) Create(ctx context.Context, name string, flags, mode uint32, out *fuse.EntryOut) (*fs.Inode, fs.FileHandle, uint32, syscall.Errno) {
	if errno := n.writable(); errno != 0 {
		return nil, nil, 0, errno
	}
	vf, _, err := n.c.create(ctx, n.share, n.parentID(), name, attrNormal, 0, time.Now())
	if err != nil {
		return nil, nil, 0, toErrno(err)
	}
	if vf.Attributes == nil {
		vf.Attributes = ptr(fileAttrs(attrNormal))
	}
	vf.ParrentID, vf.PublicName = n.parentID(), name
	n.add(name, vf)
	inode := n.childNode(ctx, name, n.share, *vf, out)
	child := inode.Operations().(*node)
	if err := child.openWork(ctx, true); err != nil {
		return nil, nil, 0, toErrno(err)
	}
	child.mu.Lock()
	child.handles++
	child.mu.Unlock()
	return inode, &editHandle{n: child}, 0, 0
}

func (n *node) Mkdir(ctx context.Context, name string, mode uint32, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	if errno := n.writable(); errno != 0 {
		return nil, errno
	}
	vf, _, err := n.c.create(ctx, n.share, n.parentID(), name, attrDirectory, 0, time.Now())
	if err != nil {
		return nil, toErrno(err)
	}
	vf.Attributes, vf.PublicName = ptr(fileAttrs(attrDirectory)), name
	n.add(name, vf)
	return n.childNode(ctx, name, n.share, *vf, out), 0
}

func (n *node) find(ctx context.Context, name string) (VirtualFile, syscall.Errno) {
	n.invalidate()
	if err := n.list(ctx); err != nil {
		return VirtualFile{}, toErrno(err)
	}
	vf, ok := n.child(name)
	if !ok {
		return vf, syscall.ENOENT
	}
	return vf, 0
}

// fullRecord returns the complete server record. Tree entries are trimmed, and
// changes must echo the whole record the way the Windows client does.
func (n *node) fullRecord(ctx context.Context, vf VirtualFile) (VirtualFile, syscall.Errno) {
	if n.tree() == nil {
		return vf, 0
	}
	full, err := n.c.virtualFile(ctx, n.share, vf.InternalName)
	if err != nil {
		return vf, toErrno(err)
	}
	if full == nil {
		return vf, syscall.ENOENT
	}
	return *full, 0
}

func (n *node) drop(ctx context.Context, name string) syscall.Errno {
	vf, errno := n.find(ctx, name)
	if errno != 0 {
		return errno
	}
	if vf, errno = n.fullRecord(ctx, vf); errno != 0 {
		return errno
	}
	if vf.lockedByOther(n.c.userID) {
		return syscall.EACCES
	}
	// Editors delete swap and temp files right after writing them.
	if child := n.GetChild(name); child != nil {
		child.Operations().(*node).discard()
	}
	if err := n.c.remove(ctx, n.share, vf); err != nil {
		return toErrno(err)
	}
	n.mu.Lock()
	delete(n.children, name)
	n.mu.Unlock()
	n.changed(func(t *shareTree) { t.remove(vf.InternalName) })
	return 0
}

func (n *node) Unlink(ctx context.Context, name string) syscall.Errno {
	if errno := n.writable(); errno != 0 {
		return errno
	}
	return n.drop(ctx, name)
}

// Rmdir checks emptiness itself: the server's delete would take the whole subtree.
func (n *node) Rmdir(ctx context.Context, name string) syscall.Errno {
	if errno := n.writable(); errno != 0 {
		return errno
	}
	vf, errno := n.find(ctx, name)
	if errno != 0 {
		return errno
	}
	kids, err := n.c.children(ctx, n.share, vf.InternalName)
	if err != nil {
		return toErrno(err)
	}
	for _, k := range kids {
		if !k.Deleted {
			return syscall.ENOTEMPTY
		}
	}
	return n.drop(ctx, name)
}

func (n *node) Rename(ctx context.Context, name string, newParent fs.InodeEmbedder, newName string, flags uint32) syscall.Errno {
	if errno := n.writable(); errno != 0 {
		return errno
	}
	if flags != 0 {
		return syscall.ENOTSUP
	}
	dst := newParent.EmbeddedInode().Operations().(*node)
	if dst.share == nil {
		return syscall.EACCES
	}
	if dst.share.ID != n.share.ID {
		return syscall.EXDEV
	}
	vf, errno := n.find(ctx, name)
	if errno != 0 {
		return errno
	}
	if vf, errno = n.fullRecord(ctx, vf); errno != 0 {
		return errno
	}
	if vf.lockedByOther(n.c.userID) {
		return syscall.EACCES
	}
	// An upload in flight would send the old name and undo the rename. One that is
	// only scheduled picks up the new name.
	var moved *node
	if child := n.GetChild(name); child != nil {
		moved = child.Operations().(*node)
		moved.io.Lock()
		defer moved.io.Unlock()
	}
	if existing, errno := dst.find(ctx, newName); errno == 0 {
		if existing.isDir() {
			return syscall.EISDIR
		}
		if existing.InternalName != vf.InternalName {
			if errno := dst.drop(ctx, newName); errno != 0 {
				return errno
			}
		}
	}
	if dst.parentID() != n.parentID() {
		if err := n.c.move(ctx, n.share, vf, dst.parentID()); err != nil {
			return toErrno(err)
		}
		vf.ParrentID = dst.parentID()
	}
	if newName != name {
		if err := n.c.rename(ctx, n.share, vf, newName); err != nil {
			n.invalidate()
			dst.invalidate()
			n.changed(func(t *shareTree) { t.put(vf) })
			return toErrno(err)
		}
		vf.PublicName = newName
	}
	n.mu.Lock()
	delete(n.children, name)
	n.mu.Unlock()
	dst.add(newName, &vf)
	if moved != nil {
		moved.mu.Lock()
		moved.vf.PublicName, moved.vf.ParrentID = vf.PublicName, vf.ParrentID
		moved.mu.Unlock()
	}
	return 0
}
