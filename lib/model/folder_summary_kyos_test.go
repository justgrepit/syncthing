// Copyright (C) 2026 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package model_test

import (
	"context"
	"testing"
	"time"

	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/events"
	"github.com/syncthing/syncthing/lib/model"
	"github.com/syncthing/syncthing/lib/model/mocks"
	"github.com/syncthing/syncthing/lib/protocol"
)

// kyos patch: the folder summary loop must not compute completion for a device
// we are not connected to. Completion is a full-folder SQL query per device on
// every index update; on a kyos node with 8 devices per folder and most of them
// offline, that loop was ~17% of a core for figures nobody reads.
func TestSummarySkipsCompletionForDisconnectedDevices(t *testing.T) {
	self := protocol.NewDeviceID([]byte("self"))
	online := protocol.NewDeviceID([]byte("online"))
	offline := protocol.NewDeviceID([]byte("offline"))

	cfg := config.New(self)
	cfg.Devices = append(cfg.Devices,
		config.DeviceConfiguration{DeviceID: online},
		config.DeviceConfiguration{DeviceID: offline})
	cfg.Folders = []config.FolderConfiguration{{
		ID:             "f",
		Path:           t.TempDir(),
		FilesystemType: config.FilesystemTypeBasic,
		Devices: []config.FolderDeviceConfiguration{
			{DeviceID: self}, {DeviceID: online}, {DeviceID: offline},
		},
	}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	wrapper := config.Wrap("", cfg, self, events.NoopLogger)
	go wrapper.Serve(ctx)

	fake := &mocks.Model{}
	fake.ConnectedToStub = func(id protocol.DeviceID) bool { return id == online }

	evLogger := events.NewLogger()
	go evLogger.Serve(ctx)

	fss := model.NewFolderSummaryService(wrapper, fake, self, evLogger)
	go fss.Serve(ctx)

	// A folder going idle from syncing takes the service's immediate path, so
	// the test does not wait on the 2 s pump.
	deadline := time.Now().Add(5 * time.Second)
	for fake.CompletionCallCount() == 0 && time.Now().Before(deadline) {
		evLogger.Log(events.StateChanged, map[string]interface{}{
			"folder": "f", "from": "syncing", "to": "idle",
		})
		time.Sleep(50 * time.Millisecond)
	}
	if fake.CompletionCallCount() == 0 {
		t.Fatal("summary never ran: no Completion call within 5s")
	}
	// Let any in-flight summary finish before inspecting the calls.
	time.Sleep(200 * time.Millisecond)

	for i := 0; i < fake.CompletionCallCount(); i++ {
		id, folder := fake.CompletionArgsForCall(i)
		if id == offline {
			t.Fatalf("completion computed for disconnected device (call %d, folder %q)", i, folder)
		}
		if id == self {
			t.Fatalf("completion computed for the local device (call %d)", i)
		}
	}
}
