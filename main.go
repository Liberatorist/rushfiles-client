// rushfiles mounts RushFiles shares on Linux through FUSE.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"runtime/debug"
	"strings"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/term"
)

const usage = `usage:
  rushfiles                       open the setup screen
  rushfiles login                 sign in through your browser
  rushfiles logout                forget the stored login
  rushfiles shares                list the shares you can access
  rushfiles mount [options] DIR   mount all shares at DIR (read-only by default)
  rushfiles autostart [options]   mount at login through a systemd user service
  rushfiles callback URL          internal: receives the browser's login redirect

mount options:
  --rw                allow changes to be written to the server
  --sync-interval 30s how often to fetch remote changes for the local file tree
  --no-sync           keep no local tree; list folders live from the server instead
  --trim-interval 5m  how often to free memory held for paths nothing is using; 0 turns it off
  --list-ttl 15s      how long live listings are reused, before the tree is ready or with --no-sync
  --cache-gb 10       local download cache limit
  --upload-delay 1s   how long after a save edits upload; 0 uploads while the file closes

autostart options:
  --rw                allow changes to be written to the server
  --dir ~/RushFiles   where to mount
  --uninstall         stop and remove the service
`

func main() {
	if len(os.Args) < 2 && !term.IsTerminal(int(os.Stdin.Fd())) {
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	var err error
	command := "setup"
	if len(os.Args) > 1 {
		command = os.Args[1]
	}
	switch command {
	case "setup":
		err = runSetup()
	case "login":
		if err = login(ctx); err == nil {
			fmt.Println("Logged in.")
		}
	case "logout":
		err = os.Remove(tokenPath())
		if treeErr := os.RemoveAll(treeDir()); err == nil {
			err = treeErr
		}
	case "shares":
		err = listShares(ctx)
	case "mount":
		err = mount(ctx, os.Args[2:])
	case "autostart":
		err = autostartCommand(os.Args[2:])
	case "callback":
		if len(os.Args) != 3 {
			err = fmt.Errorf("callback takes one URL")
		} else {
			err = sendCallback(os.Args[2])
		}
	default:
		fmt.Fprint(os.Stderr, usage)
		os.Exit(2)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, "error:", err)
		os.Exit(1)
	}
}

// trimLoop keeps memory from tracking how much of the drive has ever been visited.
func trimLoop(ctx context.Context, root *node, every time.Duration) {
	ticker := time.NewTicker(every)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		if root.trim() < trimWorthFreeing {
			continue
		}
		// The kernel's forgets arrive shortly after; hand the memory back once they have.
		select {
		case <-ctx.Done():
			return
		case <-time.After(trimSettle):
			debug.FreeOSMemory()
		}
	}
}

func listShares(ctx context.Context) error {
	c, err := newAPIClient(true)
	if err != nil {
		return err
	}
	shares, err := c.shares(ctx)
	if err != nil {
		return err
	}
	for _, s := range shares {
		fmt.Printf("%s\t%s\t%s\n", s.ID, s.Domain, s.Name)
	}
	return nil
}

// gcPercent trades a little CPU for a smaller heap. The mount is idle most of the
// time, so collections are rare either way.
const gcPercent = 25

const (
	trimWorthFreeing = 1000
	trimSettle       = 2 * time.Second
)

func mount(ctx context.Context, args []string) error {
	debug.SetGCPercent(gcPercent)
	flags := flag.NewFlagSet("mount", flag.ContinueOnError)
	rw := flags.Bool("rw", false, "allow changes to be written to the server")
	ttl := flags.Duration("list-ttl", 15*time.Second, "live folder listing lifetime")
	interval := flags.Duration("sync-interval", 30*time.Second, "how often to fetch remote changes")
	noSync := flags.Bool("no-sync", false, "keep no local file tree")
	trimEvery := flags.Duration("trim-interval", 5*time.Minute, "how often to free memory for unused paths")
	cacheGB := flags.Int64("cache-gb", 10, "download cache limit in GiB")
	uploadDelay := flags.Duration("upload-delay", time.Second, "how long after a save edits upload")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 1 {
		return fmt.Errorf("mount needs exactly one directory")
	}
	dir, err := filepath.Abs(flags.Arg(0))
	if err != nil {
		return err
	}
	c, err := newAPIClient(!*rw)
	if err != nil {
		return err
	}
	opts := &driveOptions{listTTL: *ttl, cacheDir: filepath.Join(cacheBaseDir(), "files"), cacheSize: *cacheGB << 30, uploadDelay: *uploadDelay}
	if err := os.MkdirAll(filepath.Join(opts.cacheDir, "work"), 0o700); err != nil {
		return err
	}
	if !*noSync {
		if opts.sync, err = newTreeSync(c, *interval); err != nil {
			return err
		}
	}
	mountOpts := []string{"default_permissions"}
	if !*rw {
		mountOpts = append(mountOpts, "ro")
	}
	entry, attr := 5*time.Second, 5*time.Second
	uid, gid := uint32(os.Getuid()), uint32(os.Getgid())
	root := &node{c: c, opts: opts}
	server, err := fs.Mount(dir, root, &fs.Options{
		EntryTimeout: &entry, AttrTimeout: &attr, UID: uid, GID: gid,
		MountOptions: fuse.MountOptions{FsName: "rushfiles", Name: "rushfiles", Options: mountOpts, MaxWrite: 1 << 20},
	})
	if err != nil {
		return fmt.Errorf("mounting %s: %w", dir, err)
	}
	mode := "read-only"
	if *rw {
		mode = "read-write"
	}
	log.Printf("mounted %s (%s) for domains %s; press Ctrl-C to unmount", dir, mode, strings.Join(c.domains, ", "))
	go func() {
		<-ctx.Done()
		if err := server.Unmount(); err != nil {
			log.Printf("unmount: %v (files still open?)", err)
		}
	}()
	synced := make(chan struct{})
	syncCtx, stopSync := context.WithCancel(context.Background())
	go func() {
		defer close(synced)
		if opts.sync != nil {
			opts.sync.run(syncCtx)
		}
	}()
	if *trimEvery > 0 {
		go trimLoop(syncCtx, root, *trimEvery)
	}
	server.Wait()
	// Saved edits may still be on their way to the server.
	opts.uploads.Wait()
	stopSync()
	<-synced
	return nil
}
