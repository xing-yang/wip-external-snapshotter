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

	"github.com/golang/glog"

	"k8s.io/api/core/v1"
        storage "k8s.io/api/storage/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	corelisters "k8s.io/client-go/listers/core/v1"
        storagelisters "k8s.io/client-go/listers/storage/v1alpha1"
	"k8s.io/client-go/util/workqueue"

	"github.com/kubernetes-csi/external-snapshotter/pkg/connection"
)

// csiHandler is a handler that calls CSI to create/delete volume snapshot.
// It adds finalizer to VolumeSnapshot instance to make sure they're available
// before deletion.
type csiHandler struct {
	client           kubernetes.Interface
	snapshotterName  string
	csiConnection    connection.CSIConnection
	pvcLister        corelisters.PersistentVolumeClaimLister
	vsLister         storagelisters.VolumeSnapshotLister
	vsQueue, pvcQueue workqueue.RateLimitingInterface
}

var _ Handler = &csiHandler{}

func NewCSIHandler(
	client kubernetes.Interface,
	snapshotterName string,
	csiConnection connection.CSIConnection,
	pvcLister corelisters.PersistentVolumeClaimLister,
	vsLister storagelisters.VolumeSnapshotLister) Handler {

	return &csiHandler{
		client:           client,
		snapshotterName:  snapshotterName,
		csiConnection:    csiConnection,
		pvcLister:        pvcLister,
		vsLister:         vsLister,
	}
}

func (h *csiHandler) Init(vsQueue workqueue.RateLimitingInterface, pvcQueue workqueue.RateLimitingInterface) {
	h.vsQueue = vsQueue
	h.pvcQueue = pvcQueue
}

func (h *csiHandler) SyncNewOrUpdatedVolumeSnapshot(vs *storage.VolumeSnapshot) {
	glog.V(4).Infof("CSIHandler: processing VS %q", vs.Name)

	var err error
	if vs.DeletionTimestamp == nil {
		err = h.syncCreateSnapshot(vs)
	} else {
		err = h.syncDeleteSnapshot(vs)
	}
	if err != nil {
		// Re-queue with exponential backoff
		glog.V(2).Infof("Error processing %q: %s", vs.Name, err)
		h.vsQueue.AddRateLimited(vs.Name)
		return
	}
	// The operation has finished successfully, reset exponential backoff
	h.vsQueue.Forget(vs.Name)
	glog.V(4).Infof("CSIHandler: finished processing %q", vs.Name)
}

func (h *csiHandler) syncCreateSnapshot(vs *storage.VolumeSnapshot) error {
	// Create snapshot and report any error
	glog.V(2).Infof("Creating volume snapshot %q", vs.Name)

        // TODO: Call csiCreateSnapshot here 

	glog.V(4).Infof("Volume snapshot %q created", va.Name)
	return nil
}

func (h *csiHandler) syncDeleteSnapshot(vs *storage.VolumeSnapshot) error {
	glog.V(4).Infof("Starting delete snapshot operation for %q", vs.Name)
	if !h.hasVAFinalizer(vs) {
		glog.V(4).Infof("%q is already deleted", vs.Name)
		return nil
	}

	// Delete snapshot and report any error
	glog.V(2).Infof("Deleting snapshot %q", vs.Name)
	vs, err := h.csiDeleteSnapshot(vs)
	if err != nil {
		err := fmt.Errorf("failed to delete snapshot: %s", err)
		return err
	}
	glog.V(4).Infof("Deleted snapshot %q", vs.Name)
	return nil
}

func (h *csiHandler) addVSFinalizer(vs *storage.VolumeSnapshot) (*storage.VolumeSnapshot, error) {
	finalizerName := connection.GetFinalizerName(h.snapshotterName)
	for _, f := range vs.Finalizers {
		if f == finalizerName {
			// Finalizer is already present
			glog.V(4).Infof("VS finalizer is already set on %q", vs.Name)
			return vs, nil
		}
	}

	// Finalizer is not present, add it
	glog.V(4).Infof("Adding finalizer to VA %q", vs.Name)
	clone := vs.DeepCopy()
	clone.Finalizers = append(clone.Finalizers, finalizerName)
	// TODO: use patch to save us from VersionError
	//newVS, err := h.client.StorageV1alpha1().VolumeAttachments().Update(clone)
	//if err != nil {
	//	return va, err
	//}
	glog.V(4).Infof("VS finalizer added to %q", vs.Name)
	return newVS, nil
}

func (h *csiHandler) addPVCFinalizer(pvc *v1.PersistentVolumeClaim) (*v1.PersistentVolumeClaim, error) {
	finalizerName := connection.GetFinalizerName(h.snapshotterName)
	for _, f := range pvc.Finalizers {
		if f == finalizerName {
			// Finalizer is already present
			glog.V(4).Infof("PVC finalizer is already set on %q", pvc.Name)
			return pvc, nil
		}
	}

	// Finalizer is not present, add it
	glog.V(4).Infof("Adding finalizer to PVC %q", pvc.Name)
	clone := pvc.DeepCopy()
	clone.Finalizers = append(clone.Finalizers, finalizerName)
	// TODO: use patch to save us from VersionError
	newPV, err := h.client.CoreV1().PersistentVolumes().Update(clone)
	if err != nil {
		return pv, err
	}
	glog.V(4).Infof("PVC finalizer added to %q", pvc.Name)
	return newPVC, nil
}

func (h *csiHandler) hasVSFinalizer(vs *storage.VolumeSnapshot) bool {
	finalizerName := connection.GetFinalizerName(h.snapshotterName)
	for _, f := range vs.Finalizers {
		if f == finalizerName {
			return true
		}
	}
	return false
}

func (h *csiHandler) csiCreateSnapshot(vs *storage.VolumeSnapshot) (*storage.VolumeSnapshot, map[string]string, error) {
	glog.V(4).Infof("Starting creating snapshot %q", vs.Name)
	// Check as much as possible before adding VS finalizer - it would block
	// deletion of VS on error.

	if vs.Spec.Source.PersistentVolumeName == nil {
		return vs, nil, fmt.Errorf("VolumeSnapshot.spec.persistentVolumeClaimName is empty")
	}

	pvc, err := h.psLister.Get(*vs.Spec.Source.PersistentVolumeClaimName)
	if err != nil {
		return va, nil, err
	}
	// Refuse to create snapshot for volumes that are marked for deletion.
	if pvc.DeletionTimestamp != nil {
		return vs, nil, fmt.Errorf("PersistentVolumeClaim %q is marked for deletion", pvc.Name)
	}
	pvc, err = h.addPVCFinalizer(pvc)
	if err != nil {
		return vs, nil, fmt.Errorf("could not add PersistentVolumeClaim finalizer: %s", err)
	}

	vs, err = h.addVSFinalizer(vs)
	if err != nil {
		return vs, nil, fmt.Errorf("could not add VolumeSnapshot finalizer: %s", err)
	}

	ctx := context.TODO()
	//if err != nil {
	//	return vs, nil, err
	//}

	return vs, snapshotInfo, nil
}

func (h *csiHandler) csiDeleteSnapshot(vs *storage.VolumeSnapshot) (*storage.VolumeSnapshot, error) {
	if vs.Spec.Source.PersistentVolumeClaimName == nil {
		return vs, fmt.Errorf("VolumeSnapshot.spec.persistentVolumeClaimName is empty")
	}

	pvc, err := h.pvcLister.Get(*va.Spec.Source.PersistentVolumeClaimName)
	if err != nil {
		return vs, err
	}

	ctx := context.TODO()
	err := h.csiConnection.DeleteSnapshot(ctx, snapshotID)
	if err != nil {
		// The volume snapshot may not be deleted. Save the error and try again
		// after backoff.
		return vs, err
	}
	if err != nil {
		glog.V(2).Infof("Deleted snapshot %q with error %s", vs.Name, err.Error())
	} else {
		glog.V(2).Infof("Deleted snapshot %q", vs.Name)
	}

	return vs, nil
}

func (h *csiHandler) SyncNewOrUpdatedPersistentVolumeClaim(pvc *v1.PersistentVolumeClaim) {
	glog.V(4).Infof("CSIHandler: processing PVC %q", pvc.Name)
	// Sync and remove finalizer on given PVC
	if pvc.DeletionTimestamp == nil {
		// Don't process anything that has no deletion timestamp.
		glog.V(4).Infof("CSIHandler: processing PVC %q: no deletion timestamp, ignoring", pvc.Name)
		h.pvcQueue.Forget(pvc.Name)
		return
	}

	// Check if the PVC has finalizer
	finalizer := connection.GetFinalizerName(h.snapshotterName)
	found := false
	for _, f := range pvc.Finalizers {
		if f == finalizer {
			found = true
			break
		}
	}
	if !found {
		// No finalizer -> no action required
		glog.V(4).Infof("CSIHandler: processing PVC %q: no finalizer, ignoring", pvc.Name)
		h.pvcQueue.Forget(pvc.Name)
		return
	}

	// Check that there is no VS that requires the PVC
	vss, err := h.vsLister.List(labels.Everything())
	if err != nil {
		// Failed listing VSs? Try again with exp. backoff
		glog.Errorf("Failed to list VolumeSnapshots for PVC %q: %s", pvc.Name, err.Error())
		h.pvcQueue.AddRateLimited(pvc.Name)
		return
	}
	for _, va := range vas {
		if va.Spec.Source.PersistentVolumeName != nil && *va.Spec.Source.PersistentVolumeName == pv.Name {
			// This PV is needed by this VA, don't remove finalizer
			glog.V(4).Infof("CSIHandler: processing PV %q: VA %q found", pv.Name, va.Name)
			h.pvQueue.Forget(pv.Name)
			return
		}
	}
	// No VS found -> remove finalizer
	glog.V(4).Infof("CSIHandler: processing PVC %q: no VS found, removing finalizer", pvc.Name)
	clone := pvc.DeepCopy()
	newFinalizers := []string{}
	for _, f := range pvc.Finalizers {
		if f == finalizer {
			continue
		}
		newFinalizers = append(newFinalizers, f)
	}
	if len(newFinalizers) == 0 {
		// Canonize empty finalizers for unit test (so we don't need to
		// distinguish nil and [] there)
		newFinalizers = nil
	}
	clone.Finalizers = newFinalizers
	_, err = h.client.CoreV1().PersistentVolumeClaims().Update(clone)
	if err != nil {
		glog.Errorf("Failed to remove finalizer from PVC %q: %s", pvc.Name, err.Error())
		h.pvcQueue.AddRateLimited(pvc.Name)
		return
	}
	glog.V(2).Infof("Removed finalizer from PVC %q", pvc.Name)
	h.pvcQueue.Forget(pvc.Name)

	return
}
