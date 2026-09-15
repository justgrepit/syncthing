// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"testing"

	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/protocol"
)

// The receiver-side mirror of TestIndexFromRemoteResumedBeforeClusterConfig
// (kyos incident 2026-09-15): the LOCAL side pauses and resumes a folder
// while a peer, going by an earlier ClusterConfig, is still sending its index.
// Each index that lands while the folder is not running here must be dropped,
// not returned as an error that closes the connection, and the folder must
// take indexes normally once it runs.

func kyosRemoteFile(name string, seq int64) []protocol.FileInfo {
	return []protocol.FileInfo{{
		Name:     name,
		Size:     10,
		Version:  protocol.Vector{}.Update(device1.Short()),
		Sequence: seq,
	}}
}

func TestIndexForFolderNotYetRunningLocallyIsDropped(t *testing.T) {
	m, fc, fcfg := setupModelWithConnection(t)
	defer cleanupModelAndRemoveDir(m, fcfg.Filesystem().URI())

	// A registry whose folder has a pending start (the peer's ClusterConfig was
	// handled) but is not registered as running yet: a local resume that
	// restartFolder has not committed.
	r := newIndexHandlerRegistry(fc, m.sdb, newDeviceDownloadState(), events.NoopLogger)
	r.AddIndexInfo(fcfg.ID, &clusterConfigDeviceInfo{
		local:  protocol.Device{ID: myID},
		remote: protocol.Device{ID: device1},
	})

	if err := r.ReceiveIndex(fcfg.ID, kyosRemoteFile("early", 1), false, "Index", 0, 1); err != nil {
		t.Fatalf("index for a folder not running here yet returned %v; want it dropped", err)
	}
	if _, ok, _ := m.sdb.GetDeviceFile(fcfg.ID, device1, "early"); ok {
		t.Fatal("a dropped index was applied")
	}

	// The resume commits: the pending start runs and indexes are taken.
	runner, _ := m.folderRunners.Get(fcfg.ID)
	r.RegisterFolderState(fcfg, runner)
	if err := r.ReceiveIndex(fcfg.ID, kyosRemoteFile("late", 2), false, "Index", 0, 2); err != nil {
		t.Fatalf("index after the folder runs: %v", err)
	}
	if _, ok, err := m.sdb.GetDeviceFile(fcfg.ID, device1, "late"); err != nil || !ok {
		t.Fatalf("index after the folder runs was not applied (found=%v, err=%v)", ok, err)
	}
}

func TestIndexForLocallyPausedHandlerIsDropped(t *testing.T) {
	m, fc, fcfg := setupModelWithConnection(t)
	defer cleanupModelAndRemoveDir(m, fcfg.Filesystem().URI())

	r := newIndexHandlerRegistry(fc, m.sdb, newDeviceDownloadState(), events.NoopLogger)
	runner, _ := m.folderRunners.Get(fcfg.ID)
	r.RegisterFolderState(fcfg, runner)
	r.AddIndexInfo(fcfg.ID, &clusterConfigDeviceInfo{
		local:  protocol.Device{ID: myID},
		remote: protocol.Device{ID: device1},
	})

	// Paused locally: the handler still exists, paused.
	paused := fcfg.Copy()
	paused.Paused = true
	r.RegisterFolderState(paused, nil)

	if err := r.ReceiveIndex(fcfg.ID, kyosRemoteFile("while-paused", 1), true, "Index update", 0, 1); err != nil {
		t.Fatalf("index update for a locally paused handler returned %v; want it dropped", err)
	}
	if _, ok, _ := m.sdb.GetDeviceFile(fcfg.ID, device1, "while-paused"); ok {
		t.Fatal("a dropped index update was applied")
	}
}

func TestIndexForLocallyPausedFolderIsDropped(t *testing.T) {
	m, fc, fcfg := setupModelWithConnection(t)
	defer cleanupModelAndRemoveDir(m, fcfg.Filesystem().URI())

	pauseFolder(t, m.cfg, fcfg.ID, true)
	if err := m.Index(fc, &protocol.Index{Folder: fcfg.ID, Files: kyosRemoteFile("paused", 1), LastSequence: 1}); err != nil {
		t.Fatalf("index for a locally paused folder returned %v; want it dropped", err)
	}
}
