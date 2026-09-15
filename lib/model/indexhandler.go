// Copyright (C) 2020 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/internal/itererr"
	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/svcutil"
	"github.com/syncthing/syncthing/lib/ur"
)

type indexHandler struct {
	conn                     protocol.Connection
	downloads                *deviceDownloadState
	folder                   string
	folderIsReceiveEncrypted bool
	evLogger                 events.Logger

	// We track the latest / highest sequence number in two ways for two
	// different reasons. Initially they are the same -- the highest seen
	// sequence number reported by the other side (or zero).
	//
	// One is the highest number we've seen when iterating the database,
	// which we track for database iteration purposes. When we loop, we
	// start looking at that number plus one in the next loop. Our index
	// numbering may have holes which this will skip over.
	//
	// The other is the highest sequence we previously sent to the other
	// side, used by them for correctness checks. This one must not skip
	// holes. That is, if we iterate and find a hole, this is not
	// incremented because nothing was sent to the other side.
	localPrevSequence int64 // the highest sequence number we've seen in our FileInfos
	sentPrevSequence  int64 // the highest sequence number we've sent to the peer

	cond   *sync.Cond
	paused bool
	sdb    db.DB
	runner service
}

func newIndexHandler(conn protocol.Connection, downloads *deviceDownloadState, folder config.FolderConfiguration, sdb db.DB, runner service, startInfo *clusterConfigDeviceInfo, evLogger events.Logger) (*indexHandler, error) {
	myIndexID, err := sdb.GetIndexID(folder.ID, protocol.LocalDeviceID)
	if err != nil {
		return nil, err
	}
	mySequence, err := sdb.GetDeviceSequence(folder.ID, protocol.LocalDeviceID)
	if err != nil {
		return nil, err
	}
	var startSequence int64

	// This is the other side's description of what it knows
	// about us. Lets check to see if we can start sending index
	// updates directly or need to send the index from start...

	switch startInfo.local.IndexID {
	case myIndexID:
		// They say they've seen our index ID before, so we can
		// send a delta update only.

		if startInfo.local.MaxSequence > mySequence {
			// Safety check. They claim to have more or newer
			// index data than we have - either we have lost
			// index data, or reset the index without resetting
			// the IndexID, or something else weird has
			// happened. We send a full index to reset the
			// situation.
			slog.Warn("Peer is delta index compatible, but seems out of sync with reality", conn.DeviceID().LogAttr(), folder.LogAttr())
			startSequence = 0
		} else {
			l.Debugf("Device %v folder %s is delta index compatible (mlv=%d)", conn.DeviceID().Short(), folder.Description(), startInfo.local.MaxSequence)
			startSequence = startInfo.local.MaxSequence
		}

	case 0:
		l.Debugf("Device %v folder %s has no index ID for us", conn.DeviceID().Short(), folder.Description())

	default:
		// They say they've seen an index ID from us, but it's
		// not the right one. Either they are confused or we
		// must have reset our database since last talking to
		// them. We'll start with a full index transfer.
		slog.Warn("Peer has mismatching index ID for us", conn.DeviceID().LogAttr(), folder.LogAttr(), slog.Group("indexid", slog.Any("ours", myIndexID), slog.Any("theirs", startInfo.local.IndexID)))
		startSequence = 0
	}

	// This is the other side's description of themselves. We
	// check to see that it matches the IndexID we have on file,
	// otherwise we drop our old index data and expect to get a
	// completely new set.

	theirIndexID, _ := sdb.GetIndexID(folder.ID, conn.DeviceID())
	if startInfo.remote.IndexID == 0 {
		// They're not announcing an index ID. This means they
		// do not support delta indexes and we should clear any
		// information we have from them before accepting their
		// index, which will presumably be a full index.
		l.Debugf("Device %v folder %s does not announce an index ID", conn.DeviceID().Short(), folder.Description())
		if err := sdb.DropAllFiles(folder.ID, conn.DeviceID()); err != nil {
			return nil, err
		}
	} else if startInfo.remote.IndexID != theirIndexID {
		// The index ID we have on file is not what they're
		// announcing. They must have reset their database and
		// will probably send us a full index. We drop any
		// information we have and remember this new index ID
		// instead.
		slog.Info("Peer has a new index ID", conn.DeviceID().LogAttr(), folder.LogAttr(), slog.Any("indexid", startInfo.remote.IndexID))
		if err := sdb.DropAllFiles(folder.ID, conn.DeviceID()); err != nil {
			return nil, err
		}
		if err := sdb.SetIndexID(folder.ID, conn.DeviceID(), startInfo.remote.IndexID); err != nil {
			return nil, err
		}
	}

	return &indexHandler{
		conn:                     conn,
		downloads:                downloads,
		folder:                   folder.ID,
		folderIsReceiveEncrypted: folder.Type == config.FolderTypeReceiveEncrypted,
		localPrevSequence:        startSequence,
		sentPrevSequence:         startSequence,
		evLogger:                 evLogger,

		sdb:    sdb,
		runner: runner,
		cond:   sync.NewCond(new(sync.Mutex)),
	}, nil
}

// waitWhilePaused waits for the handler to resume.
func (s *indexHandler) waitWhilePaused(ctx context.Context) error {
	s.cond.L.Lock()
	defer s.cond.L.Unlock()

	for s.paused {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			s.cond.Wait()
		}
	}

	return nil
}

func (s *indexHandler) Serve(ctx context.Context) (err error) {
	l.Debugf("Starting index handler for %s to %s at %s (localPrevSequence=%d)", s.folder, s.conn.DeviceID().Short(), s.conn, s.localPrevSequence)
	stop := make(chan struct{})

	defer func() {
		err = svcutil.NoRestartErr(err)
		l.Debugf("Exiting index handler for %s to %s at %s: %v", s.folder, s.conn.DeviceID().Short(), s.conn, err)
		close(stop)
	}()

	// Broadcast the pause cond when the context quits
	go func() {
		select {
		case <-ctx.Done():
			s.cond.Broadcast()
		case <-stop:
		}
	}()

	// We need to send one index, regardless of whether there is something to send or not
	if err := s.waitWhilePaused(ctx); err != nil {
		return err
	}
	err = s.sendIndexTo(ctx)

	// Subscribe to LocalIndexUpdated (we have new information to send) and
	// DeviceDisconnected (it might be us who disconnected, so we should
	// exit).
	sub := s.evLogger.Subscribe(events.LocalIndexUpdated | events.DeviceDisconnected)
	defer sub.Unsubscribe()

	evChan := sub.C()
	ticker := time.NewTicker(time.Minute)
	defer ticker.Stop()

	for err == nil {
		if err := s.waitWhilePaused(ctx); err != nil {
			return err
		}

		// While we have sent a sequence at least equal to the one
		// currently in the database, wait for the local index to update. The
		// local index may update for other folders than the one we are
		// sending for.
		var seq int64
		seq, err = s.sdb.GetDeviceSequence(s.folder, protocol.LocalDeviceID)
		if err != nil {
			return err
		}
		if seq <= s.localPrevSequence {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-evChan:
			case <-ticker.C:
			}
			continue
		}

		err = s.sendIndexTo(ctx)

		// Wait a short amount of time before entering the next loop. If there
		// are continuous changes happening to the local index, this gives us
		// time to batch them up a little.
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(250 * time.Millisecond):
		}
	}

	return err
}

// resume might be called because the folder was actually resumed, or just
// because the folder config changed (and thus the runner and potentially fset).
func (s *indexHandler) resume(runner service) {
	s.cond.L.Lock()
	s.paused = false
	s.runner = runner
	s.cond.Broadcast()
	s.cond.L.Unlock()
}

func (s *indexHandler) pause() {
	s.cond.L.Lock()
	if s.paused {
		s.evLogger.Log(events.Failure, "index handler got paused while already paused")
	}
	s.paused = true
	s.runner = nil
	s.cond.Broadcast()
	s.cond.L.Unlock()
}

// sendIndexTo sends file infos with a sequence number higher than prevSequence and
// returns the highest sent sequence number.
func (s *indexHandler) sendIndexTo(ctx context.Context) error {
	initial := s.localPrevSequence == 0
	batch := NewFileInfoBatch(nil)
	var batchError error
	batch.SetFlushFunc(func(fs []protocol.FileInfo) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		if len(fs) == 0 {
			// can't happen, flush is not called with an empty batch
			panic("bug: flush called with empty batch (race condition?)")
		}
		if batchError != nil {
			// can't happen, once an error is returned the index sender exits
			panic(fmt.Sprintf("bug: once failed it should stay failed (%v)", batchError))
		}
		l.Debugf("%v: Sending %d files (<%d bytes)", s, len(fs), batch.Size())

		lastSequence := fs[len(fs)-1].Sequence
		var err error
		if initial {
			initial = false
			err = s.conn.Index(ctx, &protocol.Index{
				Folder:       s.folder,
				Files:        fs,
				LastSequence: lastSequence,
			})
		} else {
			err = s.conn.IndexUpdate(ctx, &protocol.IndexUpdate{
				Folder:       s.folder,
				Files:        fs,
				PrevSequence: s.sentPrevSequence,
				LastSequence: lastSequence,
			})
		}
		if err != nil {
			batchError = err
			return err
		}
		s.sentPrevSequence = lastSequence
		return nil
	})

	var f protocol.FileInfo
	previousWasDelete := false

	for fi, err := range itererr.Zip(s.sdb.AllLocalFilesBySequence(s.folder, protocol.LocalDeviceID, s.localPrevSequence+1, MaxBatchSizeFiles+1)) {
		if err != nil {
			return err
		}
		// This is to make sure that renames (which is an add followed by a delete) land in the same batch.
		// Even if the batch is full, we allow a last delete to slip in, we do this by making sure that
		// the batch ends with a non-delete, or that the last item in the batch is already a delete
		if batch.Full() && (!fi.IsDeleted() || previousWasDelete) {
			break
		}

		if fi.SequenceNo() < s.localPrevSequence+1 {
			s.logSequenceAnomaly("database returned sequence lower than requested", map[string]any{
				"sequence": fi.SequenceNo(),
				"start":    s.localPrevSequence + 1,
			})
			return errors.New("database misbehaved")
		}

		if f.Sequence > 0 && fi.SequenceNo() <= f.Sequence {
			s.logSequenceAnomaly("database returned non-increasing sequence", map[string]any{
				"sequence": fi.SequenceNo(),
				"start":    s.localPrevSequence + 1,
				"previous": f.Sequence,
			})
			return errors.New("database misbehaved")
		}

		f = fi
		s.localPrevSequence = f.Sequence

		// If this is a folder receiving encrypted files only, we
		// mustn't ever send locally changed file infos. Those aren't
		// encrypted and thus would be a protocol error at the remote.
		if s.folderIsReceiveEncrypted && fi.IsReceiveOnlyChanged() {
			continue
		}

		f = prepareFileInfoForIndex(f)

		previousWasDelete = f.IsDeleted()

		batch.Append(f)
	}
	return batch.Flush()
}

func (s *indexHandler) receive(fs []protocol.FileInfo, update bool, op string, prevSequence, lastSequence int64) error {
	deviceID := s.conn.DeviceID()

	s.cond.L.Lock()
	paused := s.paused
	runner := s.runner
	s.cond.L.Unlock()

	if paused {
		slog.Warn("Unexpected operation on paused folder", "op", op, "folder", s.folder)
		return fmt.Errorf("%v: %w", s.folder, ErrFolderPaused)
	}

	defer runner.SchedulePull()

	s.downloads.Update(s.folder, makeForgetUpdate(fs))

	if !update {
		if err := s.sdb.DropAllFiles(s.folder, deviceID); err != nil {
			return err
		}
	}

	l.Debugf("Received %d files for %s from %s, prevSeq=%d, lastSeq=%d", len(fs), s.folder, deviceID.Short(), prevSequence, lastSequence)

	// Verify that the previous sequence number matches what we expected
	exp, err := s.sdb.GetDeviceSequence(s.folder, deviceID)
	if err != nil {
		return err
	}
	if prevSequence > 0 && prevSequence != exp {
		s.logSequenceAnomaly("index update with unexpected sequence", map[string]any{
			"prevSeq":      prevSequence,
			"lastSeq":      lastSequence,
			"batch":        len(fs),
			"expectedPrev": exp,
		})
	}

	for i := range fs {
		// Verify index in relation to the claimed sequence boundaries
		if fs[i].Sequence < prevSequence {
			s.logSequenceAnomaly("file with sequence before prevSequence", map[string]any{
				"prevSeq": prevSequence,
				"lastSeq": lastSequence,
				"batch":   len(fs),
				"seenSeq": fs[i].Sequence,
				"atIndex": i,
			})
		}
		if lastSequence > 0 && fs[i].Sequence > lastSequence {
			s.logSequenceAnomaly("file with sequence after lastSequence", map[string]any{
				"prevSeq": prevSequence,
				"lastSeq": lastSequence,
				"batch":   len(fs),
				"seenSeq": fs[i].Sequence,
				"atIndex": i,
			})
		}
		if i > 0 && fs[i].Sequence <= fs[i-1].Sequence {
			s.logSequenceAnomaly("index update with non-increasing sequence", map[string]any{
				"prevSeq":      prevSequence,
				"lastSeq":      lastSequence,
				"batch":        len(fs),
				"seenSeq":      fs[i].Sequence,
				"atIndex":      i,
				"precedingSeq": fs[i-1].Sequence,
			})
		}
	}

	// Verify the claimed last sequence number
	if lastSequence > 0 && len(fs) > 0 && lastSequence != fs[len(fs)-1].Sequence {
		s.logSequenceAnomaly("index update with unexpected last sequence", map[string]any{
			"prevSeq": prevSequence,
			"lastSeq": lastSequence,
			"batch":   len(fs),
			"seenSeq": fs[len(fs)-1].Sequence,
		})
	}

	if err := s.sdb.Update(s.folder, deviceID, fs); err != nil {
		return err
	}
	seq, err := s.sdb.GetDeviceSequence(s.folder, deviceID)
	if err != nil {
		return err
	}

	// Check that the sequence we get back is what we put in...
	if lastSequence > 0 && len(fs) > 0 && seq != lastSequence {
		s.logSequenceAnomaly("unexpected sequence after update", map[string]any{
			"prevSeq":     prevSequence,
			"lastSeq":     lastSequence,
			"batch":       len(fs),
			"seenSeq":     fs[len(fs)-1].Sequence,
			"returnedSeq": seq,
		})
	}

	s.evLogger.Log(events.RemoteIndexUpdated, map[string]interface{}{
		"device":   deviceID.String(),
		"folder":   s.folder,
		"items":    len(fs),
		"sequence": seq,
		"version":  seq, // legacy for sequence
	})

	return nil
}

func (s *indexHandler) logSequenceAnomaly(msg string, extra map[string]any) {
	extraStrs := make(map[string]string, len(extra))
	for k, v := range extra {
		extraStrs[k] = fmt.Sprint(v)
	}

	s.evLogger.Log(events.Failure, ur.FailureData{
		Description: msg,
		Extra:       extraStrs,
	})
}

func prepareFileInfoForIndex(f protocol.FileInfo) protocol.FileInfo {
	// If the file is marked LocalReceive (i.e., changed locally on a
	// receive only folder) we do not want it to ever become the
	// globally best version, invalid or not.
	if f.IsReceiveOnlyChanged() {
		f.Version = protocol.Vector{}
	}
	// The trailer with the encrypted fileinfo is device local, announce the size without it to remotes.
	f.Size -= int64(f.EncryptionTrailerSize)
	return f
}

func (s *indexHandler) String() string {
	return fmt.Sprintf("indexHandler@%p for %s to %s at %s", s, s.folder, s.conn.DeviceID().Short(), s.conn)
}

type indexHandlerRegistry struct {
	evLogger      events.Logger
	conn          protocol.Connection
	sdb           db.DB
	downloads     *deviceDownloadState
	indexHandlers *serviceMap[string, *indexHandler]
	startInfos    map[string]*clusterConfigDeviceInfo
	folderStates  map[string]*indexHandlerFolderState
	// remotePausedInfos holds the start info from the last ClusterConfig that
	// reported a folder as not running on the remote (kyos). See
	// RemoveRemotePaused and ReceiveIndex.
	remotePausedInfos map[string]*clusterConfigDeviceInfo
	mut               sync.Mutex
}

type indexHandlerFolderState struct {
	cfg    config.FolderConfiguration
	runner service
}

func newIndexHandlerRegistry(conn protocol.Connection, sdb db.DB, downloads *deviceDownloadState, evLogger events.Logger) *indexHandlerRegistry {
	r := &indexHandlerRegistry{
		evLogger:      evLogger,
		conn:          conn,
		sdb:           sdb,
		downloads:     downloads,
		indexHandlers: newServiceMap[string, *indexHandler](evLogger),
		startInfos:    make(map[string]*clusterConfigDeviceInfo),
		folderStates:  make(map[string]*indexHandlerFolderState),

		remotePausedInfos: make(map[string]*clusterConfigDeviceInfo),
		mut:               sync.Mutex{},
	}
	return r
}

func (r *indexHandlerRegistry) String() string {
	return fmt.Sprintf("indexHandlerRegistry/%v", r.conn.DeviceID().Short())
}

func (r *indexHandlerRegistry) Serve(ctx context.Context) error {
	// Running the index handler registry means running the individual index
	// handler children.
	return r.indexHandlers.Serve(ctx)
}

func (r *indexHandlerRegistry) startLocked(folder config.FolderConfiguration, runner service, startInfo *clusterConfigDeviceInfo) error {
	r.indexHandlers.RemoveAndWait(folder.ID, 0)
	delete(r.startInfos, folder.ID)

	is, err := newIndexHandler(r.conn, r.downloads, folder, r.sdb, runner, startInfo, r.evLogger)
	if err != nil {
		// kyos: this used to be discarded by every caller, leaving the folder
		// running with no handler — the peer's index was then refused as "no
		// such folder" and our own index never sent (kyos incident
		// 2026-09-15). Callers keep the start info pending and retry; see
		// startPendingLocked.
		slog.Warn("Failed to start index handler", r.conn.DeviceID().LogAttr(), folder.LogAttr(), slogutil.Error(err))
		return err
	}
	r.indexHandlers.Add(folder.ID, is)

	// This new connection might help us get in sync.
	runner.SchedulePull()
	return nil
}

// AddIndexInfo starts an index handler for given folder, unless it is paused.
// If it is paused, the given startInfo is stored to start the sender once the
// folder is resumed.
// If an index handler is already running, it will be stopped first.
func (r *indexHandlerRegistry) AddIndexInfo(folder string, startInfo *clusterConfigDeviceInfo) {
	r.mut.Lock()
	defer r.mut.Unlock()

	if r.indexHandlers.RemoveAndWait(folder, 0) == nil {
		l.Debugf("Removed index sender for device %v and folder %v due to added pending", r.conn.DeviceID().Short(), folder)
	}
	delete(r.remotePausedInfos, folder)
	folderState, ok := r.folderStates[folder]
	if !ok {
		l.Debugf("Pending index handler for device %v and folder %v", r.conn.DeviceID().Short(), folder)
		r.startInfos[folder] = startInfo
		return
	}
	_ = r.startPendingLocked(folderState, startInfo) // logged in startLocked, retried later
}

// startPendingLocked starts the index handler and, when that fails, keeps the
// start info pending (kyos). The start is retried when the folder is registered
// again and when the peer sends an index for it (ReceiveIndex), which then
// returns the real error instead of ErrFolderMissing.
func (r *indexHandlerRegistry) startPendingLocked(state *indexHandlerFolderState, startInfo *clusterConfigDeviceInfo) error {
	if err := r.startLocked(state.cfg, state.runner, startInfo); err != nil {
		r.startInfos[state.cfg.ID] = startInfo
		return err
	}
	return nil
}

// Remove stops a running index handler or removes one pending to be started.
// It is a noop if the folder isn't known.
func (r *indexHandlerRegistry) Remove(folder string) {
	r.mut.Lock()
	defer r.mut.Unlock()

	l.Debugf("Removing index handler for device %v and folder %v", r.conn.DeviceID().Short(), folder)
	r.indexHandlers.RemoveAndWait(folder, 0)
	delete(r.startInfos, folder)
	delete(r.remotePausedInfos, folder)
	l.Debugf("Removed index handler for device %v and folder %v", r.conn.DeviceID().Short(), folder)
}

// RemoveRemotePaused stops the index handler for a folder the remote reports
// as not running, like Remove, but remembers the ClusterConfig's start info
// (kyos). The remote announces the resume in a new ClusterConfig, which is
// sent asynchronously and possibly on another connection, while its index
// handler starts sending right away. Without the remembered info an index that
// wins that race hits ReceiveIndex with no handler, gets ErrFolderMissing, and
// the connection closes ("handling index for X: X: no such folder") — on a
// client that pauses and resumes folders on every start this repeated on every
// reconnect (kyos incident 2026-09-15). The info is deliberately NOT a pending
// startInfo: resuming the folder locally must still not start sending to a
// remote that has it paused.
func (r *indexHandlerRegistry) RemoveRemotePaused(folder string, startInfo *clusterConfigDeviceInfo) {
	r.mut.Lock()
	defer r.mut.Unlock()

	r.indexHandlers.RemoveAndWait(folder, 0)
	delete(r.startInfos, folder)
	if startInfo != nil {
		r.remotePausedInfos[folder] = startInfo
	}
}

// RemoveAllExcept stops all running index handlers and removes those pending to be started,
// except mentioned ones.
// It is a noop if the folder isn't known.
func (r *indexHandlerRegistry) RemoveAllExcept(except map[string]remoteFolderState) {
	r.mut.Lock()
	defer r.mut.Unlock()

	r.indexHandlers.Each(func(folder string, is *indexHandler) error {
		if _, ok := except[folder]; !ok {
			r.indexHandlers.RemoveAndWait(folder, 0)
			l.Debugf("Removed index handler for device %v and folder %v (removeAllExcept)", r.conn.DeviceID().Short(), folder)
		}
		return nil
	})
	for folder := range r.startInfos {
		if _, ok := except[folder]; !ok {
			delete(r.startInfos, folder)
			l.Debugf("Removed pending index handler for device %v and folder %v (removeAllExcept)", r.conn.DeviceID().Short(), folder)
		}
	}
	for folder := range r.remotePausedInfos {
		if _, ok := except[folder]; !ok {
			delete(r.remotePausedInfos, folder)
		}
	}
}

// RegisterFolderState must be called whenever something about the folder
// changes. The exception being if the folder is removed entirely, then call
// Remove. The fset and runner arguments may be nil, if given folder is paused.
func (r *indexHandlerRegistry) RegisterFolderState(folder config.FolderConfiguration, runner service) {
	if !folder.SharedWith(r.conn.DeviceID()) {
		r.Remove(folder.ID)
		return
	}

	r.mut.Lock()
	if folder.Paused {
		r.folderPausedLocked(folder.ID)
	} else {
		r.folderRunningLocked(folder, runner)
	}
	r.mut.Unlock()
}

// folderPausedLocked stops a running index handler.
// It is a noop if the folder isn't known or has not been started yet.
func (r *indexHandlerRegistry) folderPausedLocked(folder string) {
	l.Debugf("Pausing index handler for device %v and folder %v", r.conn.DeviceID().Short(), folder)
	delete(r.folderStates, folder)
	if is, ok := r.indexHandlers.Get(folder); ok {
		is.pause()
		l.Debugf("Paused index handler for device %v and folder %v", r.conn.DeviceID().Short(), folder)
	} else {
		l.Debugf("No index handler for device %v and folder %v to pause", r.conn.DeviceID().Short(), folder)
	}
}

// folderRunningLocked resumes an already running index handler or starts it, if it
// was added while paused.
// It is a noop if the folder isn't known.
func (r *indexHandlerRegistry) folderRunningLocked(folder config.FolderConfiguration, runner service) {
	r.folderStates[folder.ID] = &indexHandlerFolderState{
		cfg:    folder,
		runner: runner,
	}

	is, isOk := r.indexHandlers.Get(folder.ID)
	if info, ok := r.startInfos[folder.ID]; ok {
		if isOk {
			r.indexHandlers.RemoveAndWait(folder.ID, 0)
			l.Debugf("Removed index handler for device %v and folder %v in resume", r.conn.DeviceID().Short(), folder.ID)
		}
		if r.startPendingLocked(r.folderStates[folder.ID], info) == nil {
			l.Debugf("Started index handler for device %v and folder %v in resume", r.conn.DeviceID().Short(), folder.ID)
		}
	} else if isOk {
		l.Debugf("Resuming index handler for device %v and folder %v", r.conn.DeviceID().Short(), folder)
		is.resume(runner)
	} else {
		l.Debugf("Not resuming index handler for device %v and folder %v as none is paused and there is no start info", r.conn.DeviceID().Short(), folder.ID)
	}
}

func (r *indexHandlerRegistry) ReceiveIndex(folder string, fs []protocol.FileInfo, update bool, op string, prevSequence, lastSequence int64) error {
	r.mut.Lock()
	defer r.mut.Unlock()
	is, isOk := r.indexHandlers.Get(folder)
	if !isOk {
		// The remote said this folder was not running and is now sending an
		// index for it, so it resumed and its new ClusterConfig has not been
		// handled yet (kyos, see RemoveRemotePaused). If the folder runs here,
		// start the handler from the remembered start info and take the index
		// instead of closing the connection; the next ClusterConfig restarts the
		// handler with fresh info as usual.
		info, remembered := r.remotePausedInfos[folder]
		state, running := r.folderStates[folder]
		if remembered && running {
			if err := r.startLocked(state.cfg, state.runner, info); err != nil {
				return fmt.Errorf("%s: starting index handler for resumed remote folder: %w", folder, err)
			}
			delete(r.remotePausedInfos, folder)
			is, isOk = r.indexHandlers.Get(folder)
		}
	}
	if !isOk {
		// A start that failed (kyos, see startPendingLocked) is retried here.
		// If it fails again the error closes the connection naming the cause;
		// the index is not lost, the peer resends it on the next connection.
		info, pending := r.startInfos[folder]
		state, running := r.folderStates[folder]
		if pending && running {
			if err := r.startPendingLocked(state, info); err != nil {
				return fmt.Errorf("%s: starting index handler: %w", folder, err)
			}
			is, isOk = r.indexHandlers.Get(folder)
		}
	}
	if !isOk {
		if _, running := r.folderStates[folder]; !running {
			// The folder is configured here but not running for this
			// connection yet: a local resume is still being committed
			// (restartFolder registers it), or it was just paused. The peer
			// sent this because an earlier ClusterConfig said the folder runs
			// (kyos incident 2026-09-15: a client walking its folders through
			// pause/resume rejected every such index and closed the
			// connection). Drop it instead of closing: once the folder runs,
			// CommitConfiguration sends a ClusterConfig and the peer resends
			// from the sequence we recorded, so nothing is lost.
			l.Debugf("Dropping %s for %s from %v: folder not running here yet", op, folder, r.conn.DeviceID().Short())
			return nil
		}
		slog.Warn("Unexpected operation on nonexistent or paused folder", "op", op, "folder", folder)
		return fmt.Errorf("%s: %w", folder, ErrFolderMissing)
	}
	err := is.receive(fs, update, op, prevSequence, lastSequence)
	if errors.Is(err, ErrFolderPaused) {
		// Same race with the handler still paused (receive checks that
		// before touching anything). Resuming re-sends ClusterConfig (kyos).
		l.Debugf("Dropping %s for %s from %v: index handler paused", op, folder, r.conn.DeviceID().Short())
		return nil
	}
	return err
}

// makeForgetUpdate takes an index update and constructs a download progress update
// causing to forget any progress for files which we've just been sent.
func makeForgetUpdate(files []protocol.FileInfo) []protocol.FileDownloadProgressUpdate {
	updates := make([]protocol.FileDownloadProgressUpdate, 0, len(files))
	for _, file := range files {
		if file.IsSymlink() || file.IsDirectory() || file.IsDeleted() {
			continue
		}
		updates = append(updates, protocol.FileDownloadProgressUpdate{
			Name:       file.Name,
			Version:    file.Version,
			UpdateType: protocol.FileDownloadProgressUpdateTypeForget,
		})
	}
	return updates
}
