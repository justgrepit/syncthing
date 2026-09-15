// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/protocol"
)

// A remote that resumes a folder starts sending its index at once, while the
// ClusterConfig announcing the resume is sent asynchronously and may arrive
// later (or on another connection). An index that wins that race must be
// accepted, not rejected with ErrFolderMissing — rejecting it closes the
// connection, and on a client that pauses and resumes folders on every start
// that repeated on every reconnect (kyos incident 2026-09-15).
func TestIndexFromRemoteResumedBeforeClusterConfig(t *testing.T) {
	m, fc, fcfg := setupModelWithConnection(t)
	defer cleanupModelAndRemoveDir(m, fcfg.Filesystem().URI())

	// The remote reports the folder paused.
	cc := basicClusterConfig(myID, device1, fcfg.ID)
	cc.Folders[0].StopReason = protocol.FolderStopReasonPaused
	if err := m.ClusterConfig(fc, cc); err != nil {
		t.Fatal(err)
	}

	// It resumes and sends its index before the new ClusterConfig.
	files := []protocol.FileInfo{{
		Name:     "remote-file",
		Size:     10,
		Version:  protocol.Vector{}.Update(device1.Short()),
		Sequence: 1,
	}}
	err := m.Index(fc, &protocol.Index{Folder: fcfg.ID, Files: files, LastSequence: 1})
	if errors.Is(err, ErrFolderMissing) {
		t.Fatalf("index for a resumed remote folder rejected: %v", err)
	}
	if err != nil {
		t.Fatalf("index for a resumed remote folder failed: %v", err)
	}
	if _, ok, err := m.sdb.GetDeviceFile(fcfg.ID, device1, "remote-file"); err != nil || !ok {
		t.Fatalf("index was not applied (found=%v, err=%v)", ok, err)
	}

	// The resume ClusterConfig arrives late; the connection keeps working.
	cc.Folders[0].StopReason = protocol.FolderStopReasonRunning
	if err := m.ClusterConfig(fc, cc); err != nil {
		t.Fatal(err)
	}
	files[0].Name = "remote-file-2"
	files[0].Sequence = 2
	if err := m.IndexUpdate(fc, &protocol.IndexUpdate{Folder: fcfg.ID, Files: files, PrevSequence: 1, LastSequence: 2}); err != nil {
		t.Fatalf("index update after the late ClusterConfig failed: %v", err)
	}
}

// The remembered start info is only used for an index the remote sends. It is
// not a pending start: resuming the folder locally while the remote still has
// it paused must not start sending our index to that remote.
func TestRemotePausedFolderLocalResumeStartsNoSender(t *testing.T) {
	m, fc, fcfg := setupModelWithConnection(t)
	defer cleanupModelAndRemoveDir(m, fcfg.Filesystem().URI())

	sent := make(chan struct{}, 16)
	fc.setIndexFn(func(context.Context, string, []protocol.FileInfo) error {
		sent <- struct{}{}
		return nil
	})

	cc := basicClusterConfig(myID, device1, fcfg.ID)
	cc.Folders[0].StopReason = protocol.FolderStopReasonPaused
	if err := m.ClusterConfig(fc, cc); err != nil {
		t.Fatal(err)
	}
	// Drain anything sent before the remote paused.
	for drained := false; !drained; {
		select {
		case <-sent:
		case <-time.After(200 * time.Millisecond):
			drained = true
		}
	}

	pauseFolder(t, m.cfg, fcfg.ID, true)
	pauseFolder(t, m.cfg, fcfg.ID, false)
	files := []protocol.FileInfo{{Name: "local", Size: 10, Version: protocol.Vector{}.Update(myID.Short()), Sequence: 1}}
	localIndexUpdate(m, fcfg.ID, files)

	select {
	case <-sent:
		t.Fatal("sent an index to a remote that has the folder paused")
	case <-time.After(2 * time.Second):
	}
}
