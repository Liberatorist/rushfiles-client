package main

import (
	"context"
	"errors"
	"log"
	"sync"
	"syscall"
	"time"
)

const (
	deleteMaxWait = 30 * time.Second // a steady stream of deletes still goes out
	deleteWorkers = 4
)

// deleteQueue sends deletes in the background, so rm -r doesn't wait on the server
// for every file. A folder whose whole content is queued with it goes out as one
// delete of the folder, which takes its subtree.
type deleteQueue struct {
	c     *apiClient
	sync  *treeSync
	delay time.Duration

	mu      sync.Mutex
	queued  map[string]*queuedDelete // by normID
	sending map[string]*queuedDelete
	timer   *time.Timer
	firstAt time.Time
	running sync.WaitGroup // a scheduled or running batch

	serial sync.Mutex // a batch must see the server after the one before
}

type queuedDelete struct {
	share    *Share
	vf       VirtualFile // the tree's record, put back if the server refuses
	failures int
	err      error
	done     chan struct{}
}

func newDeleteQueue(c *apiClient, s *treeSync, delay time.Duration) *deleteQueue {
	return &deleteQueue{c: c, sync: s, delay: delay, queued: map[string]*queuedDelete{}, sending: map[string]*queuedDelete{}}
}

// add hides the entry now and deletes it on the server after the delay.
func (q *deleteQueue) add(share *Share, vf VirtualFile) {
	if t := q.sync.tree(share.ID); t != nil {
		t.hide(vf.InternalName)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.queued[normID(vf.InternalName)] = &queuedDelete{share: share, vf: vf, done: make(chan struct{})}
	q.kick()
}

// kick restarts the wait for the next batch, up to deleteMaxWait after its first delete.
func (q *deleteQueue) kick() {
	if q.timer == nil {
		q.firstAt = time.Now()
		q.running.Add(1)
	} else if !q.timer.Stop() {
		return // firing now, and it takes what is queued
	}
	wait := min(q.delay, time.Until(q.firstAt.Add(deleteMaxWait)))
	q.timer = time.AfterFunc(wait, q.fire)
}

func (q *deleteQueue) fire() {
	defer q.running.Done()
	q.serial.Lock()
	defer q.serial.Unlock()
	q.mu.Lock()
	q.timer = nil
	batch := q.queued
	q.queued = map[string]*queuedDelete{}
	for key, item := range batch {
		q.sending[key] = item
	}
	q.mu.Unlock()
	q.process(batch)
}

// flush sends what is queued now and waits for it, for unmounting.
func (q *deleteQueue) flush() {
	q.mu.Lock()
	stopped := q.timer != nil && q.timer.Stop()
	q.mu.Unlock()
	if stopped {
		q.fire()
	}
	q.running.Wait()
}

// clearName sends a queued delete of name in parent right away, so a new file of
// that name doesn't meet the old one on the server. The delete was promised when
// the entry went, so an interrupted caller doesn't cancel it.
func (q *deleteQueue) clearName(share *Share, parentID, name string) syscall.Errno {
	q.mu.Lock()
	match := func(items map[string]*queuedDelete) (string, *queuedDelete) {
		for key, item := range items {
			if item.share.ID == share.ID && item.vf.PublicName == name && normID(item.vf.ParrentID) == normID(parentID) {
				return key, item
			}
		}
		return "", nil
	}
	key, item := match(q.queued)
	if item != nil {
		delete(q.queued, key)
		q.sending[key] = item
		q.mu.Unlock()
		q.finish(item, q.send(context.Background(), item), false)
	} else if _, item = match(q.sending); item != nil {
		q.mu.Unlock()
		<-item.done
	} else {
		q.mu.Unlock()
		return 0
	}
	return toErrno(item.err)
}

func (q *deleteQueue) process(batch map[string]*queuedDelete) {
	ctx := context.Background()
	shares := map[string]map[string]*queuedDelete{}
	for key, item := range batch {
		if shares[item.share.ID] == nil {
			shares[item.share.ID] = map[string]*queuedDelete{}
		}
		shares[item.share.ID][key] = item
	}
	for _, items := range shares {
		q.processShare(ctx, items)
	}
}

func (q *deleteQueue) processShare(ctx context.Context, items map[string]*queuedDelete) {
	// whole holds, for each queued folder, whether the server has nothing in it
	// that isn't queued too.
	whole := map[string]bool{}
	var check func(share *Share, id string) (bool, error)
	check = func(share *Share, id string) (bool, error) {
		kids, err := q.c.children(ctx, share, id)
		if err != nil {
			return false, err
		}
		all := true
		for i := range kids {
			k := &kids[i]
			if k.Deleted {
				continue
			}
			if items[normID(k.InternalName)] == nil || k.lockedByOther(q.c.userID) {
				all = false
			} else if k.isDir() {
				sub, err := check(share, k.InternalName)
				if err != nil {
					return false, err
				}
				all = all && sub
			}
		}
		whole[normID(id)] = all
		return all, nil
	}
	for _, item := range items {
		if !item.vf.isDir() || items[normID(item.vf.ParrentID)] != nil {
			continue
		}
		if _, err := check(item.share, item.vf.InternalName); err != nil {
			q.retry(items, err)
			return
		}
	}
	// A folder moved away from its queued parent was not reached above.
	for key, item := range items {
		if _, seen := whole[key]; item.vf.isDir() && !seen {
			if _, err := check(item.share, item.vf.InternalName); err != nil {
				q.retry(items, err)
				return
			}
		}
	}

	covered := func(item *queuedDelete) bool {
		for p := items[normID(item.vf.ParrentID)]; p != nil; p = items[normID(p.vf.ParrentID)] {
			if whole[normID(p.vf.InternalName)] {
				return true
			}
		}
		return false
	}
	var send []*queuedDelete
	for key, item := range items {
		switch {
		case covered(item):
			q.finish(item, nil, false)
		case item.vf.isDir() && !whole[key]:
			q.finish(item, syscall.ENOTEMPTY, false)
		default:
			send = append(send, item)
		}
	}
	work := make(chan *queuedDelete)
	var wg sync.WaitGroup
	for range min(deleteWorkers, len(send)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for item := range work {
				q.finish(item, q.send(ctx, item), true)
			}
		}()
	}
	for _, item := range send {
		work <- item
	}
	close(work)
	wg.Wait()
}

// send deletes one entry, echoing the full server record as the Windows client does.
func (q *deleteQueue) send(ctx context.Context, item *queuedDelete) error {
	full, err := q.c.virtualFile(ctx, item.share, item.vf.InternalName)
	if errors.Is(err, syscall.ENOENT) || (err == nil && full == nil) {
		return nil // already gone
	}
	if err != nil {
		return err
	}
	if full.lockedByOther(q.c.userID) {
		return syscall.EACCES
	}
	return q.c.remove(ctx, item.share, *full)
}

// retry queues a batch again after the server could not be reached.
func (q *deleteQueue) retry(items map[string]*queuedDelete, err error) {
	for _, item := range items {
		q.finish(item, err, true)
	}
}

// finish settles a delete. Errors the server answered with are final, and the
// entry comes back; unreachable servers are retried a few times first.
func (q *deleteQueue) finish(item *queuedDelete, err error, retry bool) {
	key := normID(item.vf.InternalName)
	var errno syscall.Errno
	if err != nil && retry && !errors.As(err, &errno) {
		item.failures++
		if item.failures < uploadRetries {
			log.Printf("delete of %q: %v; retrying", item.vf.PublicName, err)
			q.mu.Lock()
			delete(q.sending, key)
			q.queued[key] = item
			q.kick()
			q.mu.Unlock()
			return
		}
	}
	var restore *VirtualFile
	if err != nil {
		log.Printf("delete of %q: %v; it is back", item.vf.PublicName, err)
		restore = &item.vf
	}
	if t := q.sync.tree(item.share.ID); t != nil {
		t.unhide(item.vf.InternalName, restore)
		q.sync.wake(item.share.ID)
	}
	q.mu.Lock()
	delete(q.sending, key)
	q.mu.Unlock()
	item.err = err
	close(item.done)
}
