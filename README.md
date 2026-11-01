# rushfiles-linux

Mounts RushFiles shares on Linux through FUSE. Built from the protocol in the Windows
client (v2.0.1); the Windows code itself is not used, because its drive layer depends on
the Windows Cloud Files API.

    go build -trimpath -ldflags='-s -w' -o rushfiles .   # static binary
    ./rushfiles

Running it with no arguments opens the setup screen. From there you can sign in, see your
shares, pick a mount folder and start the drive. It mounts read-only unless you switch
Access to read-write. The drive then comes back at every login, through a systemd user
service; "Stop mounting and remove the service" undoes that.

The same steps work without the setup screen:

    ./rushfiles login
    ./rushfiles shares
    mkdir -p ~/RushFiles && ./rushfiles mount ~/RushFiles        # read-only
    ./rushfiles mount --rw ~/RushFiles                           # read-write

Stop with Ctrl-C or `fusermount3 -u ~/RushFiles`. Each share appears as a top-level folder.

## Mount on startup

The setup screen does this for you. To do it from a script, sign in with `./rushfiles login`
first, stop any mount you started by hand, then:

    ./rushfiles autostart                         # read-only at ~/RushFiles
    ./rushfiles autostart --rw                    # read-write
    ./rushfiles autostart --rw --dir ~/Drive      # read-write, somewhere else

This copies the binary to `~/.local/bin/rushfiles`, installs
`~/.config/systemd/user/rushfiles.service` and starts it straight away. If the network
isn't ready yet, the service retries every 15 seconds until the mount comes up.

To change options or pick up a rebuilt binary, run it again.

    systemctl --user status rushfiles             # is it running?
    journalctl --user -u rushfiles -f             # follow the log
    systemctl --user stop rushfiles               # unmount until next login
    ./rushfiles autostart --uninstall             # remove the service

If the log shows login errors, sign in again; the service picks up the new login on its
next retry.

To mount at boot instead of at login, also run `loginctl enable-linger $USER`.

Behaviour worth knowing:

- Files download whole on first open and are cached in `~/.cache/rushfiles-linux/files`.
- The whole file tree is kept in a SQLite database, `~/.cache/rushfiles-linux/tree/tree.db`
  (table `files`; times are Unix milliseconds), and updated every 30 seconds
  (`--sync-interval`), so browsing needs no network. The first sync takes about 2 seconds per
  1,000 files, and folders are listed live until a share's tree is ready. `--no-sync` turns this off.
- Offline, the tree stays browsable and cached files open. Other files, and edits, need the
  server. Changes arrive by polling, so there is no instant push from other users.
- The tree holds names, sizes, dates and lock state for every share. `rushfiles logout`
  removes it.
- Every 5 minutes (`--trim-interval`) the mount drops memory held for paths nothing is using,
  so a tool scanning the drive doesn't leave it large. The next access looks them up again.
- Edits upload on close. If the upload fails, the mount logs where the edit was kept
  (`~/.cache/rushfiles-linux/files/work`).
- If the server copy changed during an edit, the edit is saved as a
  `name (linux conflict DATE).ext` sibling.
- There is no offline editing, pinning, or public-link/lock UI.

Login tokens are stored in `~/.config/rushfiles-linux/tokens.json` (mode 0600).
`go test .` runs the end-to-end tests: a real FUSE mount against a mock server.

## Releases

Every push builds linux-amd64 and linux-arm64 tarballs, available as workflow artifacts in
the Actions tab. To publish them on the release page, push a version tag:

    git tag v0.1.0 && git push origin v0.1.0

Each tarball holds the binary and this README, so you can unpack it and run `./rushfiles`
without building anything. Check a download with
`sha256sum -c --ignore-missing sha256sums.txt`.
