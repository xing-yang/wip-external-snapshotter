/*
Copyright 2018 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"fmt"
	"time"

	"github.com/kubernetes-csi/external-snapshotter/pkg/connection"
	"k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1alpha1"
)

// Handler is responsible for handling VolumeAttachment events from informer.
type Handler interface {
	takeSnapshot(snapshot *storage.VolumeSnapshot, volume *v1.PersistentVolume, parameters map[string]string) (*storage.VolumeSnapshotData, error)
	deleteSnapshot(vsd *storage.VolumeSnapshotData) error
	listSnapshots(vsd *storage.VolumeSnapshotData) (*storage.VolumeSnapshotDataCondition, error)
}

// csiHandler is a handler that calls CSI to create/delete volume snapshot.
type csiHandler struct {
	csiConnection connection.CSIConnection
	timeout       time.Duration
}

func NewCSIHandler(csiConnection connection.CSIConnection, timeout time.Duration) Handler {
	return &csiHandler{
		csiConnection: csiConnection,
		timeout:       timeout,
	}
}

func (handler *csiHandler) takeSnapshot(snapshot *storage.VolumeSnapshot,
	volume *v1.PersistentVolume, parameters map[string]string) (*storage.VolumeSnapshotData, error) {
	ctx, cancel := context.WithTimeout(context.Background(), handler.timeout)
	defer cancel()

	snapDataObj, err := handler.csiConnection.CreateSnapshot(ctx, snapshot, volume, parameters)
	if err != nil {
		return nil, fmt.Errorf("failed to take snapshot of the volume %s: %q", volume.Name, err)
	}

	return snapDataObj, nil
}

func (handler *csiHandler) deleteSnapshot(vsd *storage.VolumeSnapshotData) error {
	if vsd.Spec.CSISnapshot == nil {
		return fmt.Errorf("CSISnapshot not defined in spec")
	}
	ctx, cancel := context.WithTimeout(context.Background(), handler.timeout)
	defer cancel()

	err := handler.csiConnection.DeleteSnapshot(ctx, vsd.Spec.CSISnapshot.SnapshotHandle)
	if err != nil {
		return fmt.Errorf("failed to delete snapshot data %s: %q", vsd.Name, err)
	}

	return nil
}

func (handler *csiHandler) listSnapshots(vsd *storage.VolumeSnapshotData) (*storage.VolumeSnapshotDataCondition, error) {
	if vsd.Spec.CSISnapshot == nil {
		return nil, fmt.Errorf("CSISnapshot not defined in spec")
	}
	ctx, cancel := context.WithTimeout(context.Background(), handler.timeout)
	defer cancel()

	snapshotDataCon, err := handler.csiConnection.ListSnapshots(ctx, vsd.Spec.CSISnapshot.SnapshotHandle)
	if err != nil {
		return nil, fmt.Errorf("failed to list snapshot data %s: %q", vsd.Name, err)
	}

	return snapshotDataCon, nil
}
