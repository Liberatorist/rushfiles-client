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
  --list-ttl 15s      how long folder listings are reused; remote changes appear after this
  --cache-gb 10       local download cache limit

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

func mount(ctx context.Context, args []string) error {
	flags := flag.NewFlagSet("mount", flag.ContinueOnError)
	rw := flags.Bool("rw", false, "allow changes to be written to the server")
	ttl := flags.Duration("list-ttl", 15*time.Second, "folder listing lifetime")
	cacheGB := flags.Int64("cache-gb", 10, "download cache limit in GiB")
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
	opts := &driveOptions{listTTL: *ttl, cacheDir: filepath.Join(cacheBaseDir(), "files"), cacheSize: *cacheGB << 30}
	if err := os.MkdirAll(filepath.Join(opts.cacheDir, "work"), 0o700); err != nil {
		return err
	}
	mountOpts := []string{"default_permissions"}
	if !*rw {
		mountOpts = append(mountOpts, "ro")
	}
	entry, attr := 5*time.Second, 5*time.Second
	uid, gid := uint32(os.Getuid()), uint32(os.Getgid())
	server, err := fs.Mount(dir, &node{c: c, opts: opts}, &fs.Options{
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
	server.Wait()
	return nil
}
