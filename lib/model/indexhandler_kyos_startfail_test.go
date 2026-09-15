// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model

import (
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/syncthing/syncthing/internal/db"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/protocol"
)

// kyos incident 2026-09-15, round 3: starting an index handler can fail in
// the database (newIndexHandler drops the peer's files when it announces an
// index ID we do not have on file). AddIndexInfo discarded that error, so the
// folder stayed registered as running with no handler: the peer's full index
// was then refused as "no such folder", the connection closed, and because the
// failed start never recorded the peer's index ID the next connection did the
// same. Our own index for the folder was never sent either.

var errKyosDiskFull = errors.New("database or disk is full")

type kyosFailingDropDB struct {
	db.DB
	fail atomic.Bool
}

func (d *kyosFailingDropDB) DropAllFiles(folder string, device protocol.DeviceID) error {
	if d.fail.Load() {
		return errKyosDiskFull
	}
	return d.DB.DropAllFiles(folder, device)
}

func kyosNewIndexIDInfo() *clusterConfigDeviceInfo {
	return &clusterConfigDeviceInfo{
		local:  protocol.Device{ID: myID},
		remote: protocol.Device{ID: device1, IndexID: 0x1234},
	}
}

func TestIndexHandlerStartFailureIsNotReportedAsMissingFolder(t *testing.T) {
	m, fc, fcfg := setupModelWithConnection(t)
	defer cleanupModelAndRemoveDir(m, fcfg.Filesystem().URI())

	fdb := &kyosFailingDropDB{DB: m.sdb}
	fdb.fail.Store(true)
	r := newIndexHandlerRegistry(fc, fdb, newDeviceDownloadState(), events.NoopLogger)
	runner, _ := m.folderRunners.Get(fcfg.ID)
	r.RegisterFolderState(fcfg, runner)
	r.AddIndexInfo(fcfg.ID, kyosNewIndexIDInfo())

	err := r.ReceiveIndex(fcfg.ID, kyosRemoteFile("while-failing", 1), false, "Index", 0, 1)
	if err == nil {
		t.Fatal("index accepted with no index handler; want the start failure returned")
	}
	if errors.Is(err, ErrFolderMissing) {
		t.Fatalf("start failure reported as %v; want the database error", err)
	}
	if !errors.Is(err, errKyosDiskFull) || !strings.Contains(err.Error(), "starting index handler") {
		t.Fatalf("got %v; want it to name the failed index handler start and wrap the database error", err)
	}
	if _, ok, _ := m.sdb.GetDeviceFile(fcfg.ID, device1, "while-failing"); ok {
		t.Fatal("index applied without the peer's files being dropped first")
	}

	// The database recovers: the next index starts the handler from the kept
	// start info, records the peer's index ID and is applied.
	fdb.fail.Store(false)
	if err := r.ReceiveIndex(fcfg.ID, kyosRemoteFile("after-recovery", 2), false, "Index", 0, 2); err != nil {
		t.Fatalf("index after the database recovered: %v", err)
	}
	if _, ok, err := m.sdb.GetDeviceFile(fcfg.ID, device1, "after-recovery"); err != nil || !ok {
		t.Fatalf("index after recovery not applied (found=%v, err=%v)", ok, err)
	}
	if id, _ := m.sdb.GetIndexID(fcfg.ID, device1); id != 0x1234 {
		t.Fatalf("peer index ID on file = %v; want 0x1234 recorded by the handler start", id)
	}
	if _, ok := r.indexHandlers.Get(fcfg.ID); !ok {
		t.Fatal("no index handler after recovery, so our own index is never sent")
	}
}

func TestIndexHandlerStartFailureRetriedOnFolderRestart(t *testing.T) {
	m, fc, fcfg := setupModelWithConnection(t)
	defer cleanupModelAndRemoveDir(m, fcfg.Filesystem().URI())

	fdb := &kyosFailingDropDB{DB: m.sdb}
	fdb.fail.Store(true)
	r := newIndexHandlerRegistry(fc, fdb, newDeviceDownloadState(), events.NoopLogger)
	runner, _ := m.folderRunners.Get(fcfg.ID)
	r.RegisterFolderState(fcfg, runner)
	r.AddIndexInfo(fcfg.ID, kyosNewIndexIDInfo())
	if _, ok := r.indexHandlers.Get(fcfg.ID); ok {
		t.Fatal("index handler registered although its start failed")
	}

	// A folder restart re-registers the folder: the kept start info is retried.
	fdb.fail.Store(false)
	r.RegisterFolderState(fcfg, runner)
	if _, ok := r.indexHandlers.Get(fcfg.ID); !ok {
		t.Fatal("failed start was not retried when the folder was registered again")
	}
}
