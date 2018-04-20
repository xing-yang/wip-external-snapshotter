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
	"fmt"

	"github.com/golang/glog"

	"k8s.io/api/core/v1"
        storage "k8s.io/api/storage/v1alpha1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
        storageinformers "k8s.io/client-go/informers/storage/v1alpha1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
        storagelisters "k8s.io/client-go/listers/storage/v1alpha1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
)

type CSISnapshotController struct {
	client        kubernetes.Interface
	snapshotterName  string
	handler       Handler
	eventRecorder record.EventRecorder
	vsQueue       workqueue.RateLimitingInterface
	pvcQueue       workqueue.RateLimitingInterface

	vsLister       storagelisters.VolumeSnapshotLister
	vsListerSynced cache.InformerSynced
	pvcLister       corelisters.PersistentVolumeClaimLister
	pvcListerSynced cache.InformerSynced
}

// Handler is responsible for handling VolumeSnapshot events from informer.
type Handler interface {
	Init(vsQueue workqueue.RateLimitingInterface, pvcQueue workqueue.RateLimitingInterface)

	// SyncNewOrUpdatedVolumeSnapshot processes on Add/Updated event from
	// VolumeSnapshot informers. It runs in a workqueue, guaranting that only
	// one SyncNewOrUpdatedVolumeSnapshot runs for given VS.
	// SyncNewOrUpdatedVolumeSnapshot is responsible for marking the
	// VolumeSnapshot either as forgotten (resets exponential backoff) or
	// re-queue it into the vsQueue to process it after exponential
	// backoff.
	SyncNewOrUpdatedVolumeSnapshot(vs *storage.VolumeSnapshot)

	SyncNewOrUpdatedPersistentVolumeClaim(pvc *v1.PersistentVolumeClaim)
}

// NewCSISnapshotController returns a new *CSISnapshotController
func NewCSISnapshotController(client kubernetes.Interface, snapshotterName string, handler Handler, volumeSnapshotInformer storageinformers.VolumeSnapshotInformer, pvcInformer coreinformers.PersistentVolumeClaimInformer) *CSISnapshotController {
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: client.Core().Events(v1.NamespaceAll)})
	var eventRecorder record.EventRecorder
	eventRecorder = broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: fmt.Sprintf("csi-snapshotter %s", snapshotName)})

	ctrl := &CSISnapshotController{
		client:        client,
		snapshotterName:  snapshotterName,
		handler:       handler,
		eventRecorder: eventRecorder,
		vsQueue:       workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "csi-snapshotter-vs"),
		pvcQueue:       workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "csi-snapshotter-pvc"),
	}

	volumeSnapshotInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctrl.vsAdded,
		UpdateFunc: ctrl.vsUpdated,
		DeleteFunc: ctrl.vsDeleted,
	})
	ctrl.vsLister = volumeSnapshotInformer.Lister()
	ctrl.vsListerSynced = volumeSnapshotInformer.Informer().HasSynced

	pvcInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc:    ctrl.pvcAdded,
		UpdateFunc: ctrl.pvcUpdated,
		//DeleteFunc: ctrl.pvcDeleted, TODO: do we need this?
	})
	ctrl.pvcLister = pvcInformer.Lister()
	ctrl.pvcListerSynced = pvcInformer.Informer().HasSynced
	ctrl.handler.Init(ctrl.vsQueue, ctrl.pvcQueue)

	return ctrl
}

func (ctrl *CSISnapshotController) Run(workers int, stopCh <-chan struct{}) {
	defer ctrl.vsQueue.ShutDown()
	defer ctrl.pvcQueue.ShutDown()

	glog.Infof("Starting CSI snapshotter")
	defer glog.Infof("Shutting CSI snapshotter")

	if !cache.WaitForCacheSync(stopCh, ctrl.vsListerSynced, ctrl.pvcListerSynced) {
		glog.Errorf("Cannot sync caches")
		return
	}
	for i := 0; i < workers; i++ {
		go wait.Until(ctrl.syncVS, 0, stopCh)
		go wait.Until(ctrl.syncPVC, 0, stopCh)
	}

	<-stopCh
}

// vsAdded reacts to a VolumeSnapshot creation
func (ctrl *CSISnapshotController) vsAdded(obj interface{}) {
	vs := obj.(*storage.VolumeSnapshot)
	ctrl.vsQueue.Add(vs.Name)
}

// vsUpdated reacts to a VolumeSnapshot update
func (ctrl *CSISnapshotController) vsUpdated(old, new interface{}) {
	vs := new.(*storage.VolumeSnapshot)
	ctrl.vsQueue.Add(vs.Name)
}

// vsDeleted reacts to a VolumeSnapshot deleted
func (ctrl *CSISnapshotController) vsDeleted(obj interface{}) {
	vs := obj.(*storage.VolumeSnapshot)
	if vs != nil && vs.Spec.Source.PersistentVolumeName != nil {
		// Enqueue PVC sync event - it will evaluate and remove finalizer
		ctrl.pvcQueue.Add(*vs.Spec.Source.PersistentVolumeName)
	}
}

// pvcAdded reacts to a PVC creation
func (ctrl *CSISnapshotController) pvcAdded(obj interface{}) {
	pvc := obj.(*v1.PersistentVolumeClaim)
	ctrl.pvcQueue.Add(pvc.Name)
}

// pvcUpdated reacts to a PVC update
func (ctrl *CSISnapshotController) pvcUpdated(old, new interface{}) {
	pvc := new.(*v1.PersistentVolumeClaim)
	ctrl.pvcQueue.Add(pvc.Name)
}

// syncVS deals with one key off the queue.  It returns false when it's time to quit.
func (ctrl *CSISnapshotController) syncVS() {
	key, quit := ctrl.vsQueue.Get()
	if quit {
		return
	}
	defer ctrl.vsQueue.Done(key)

	vsName := key.(string)
	glog.V(4).Infof("Started VS processing %q", vsName)

	// get VolumeSnapshot to process
	vs, err := ctrl.vsLister.Get(vsName)
	if err != nil {
		if apierrs.IsNotFound(err) {
			// VolumeSnapshot was deleted in the meantime, ignore.
			glog.V(3).Infof("VS %q deleted, ignoring", vsName)
			return
		}
		glog.Errorf("Error getting VolumeSnapshot %q: %v", vsName, err)
		ctrl.vsQueue.AddRateLimited(vsName)
		return
	}
	if vs.Spec.Snapshotter != ctrl.snapshotterName {
		glog.V(4).Infof("Skipping VolumeSnapshot %s for snapshotter %s", vs.Name, vs.Spec.Snapshotter)
		return
	}
	ctrl.handler.SyncNewOrUpdatedVolumeSnapshot(vs)
}

// syncPVC deals with one key off the queue.  It returns false when it's time to quit.
func (ctrl *CSISnapshotController) syncPVC() {
	key, quit := ctrl.pvcQueue.Get()
	if quit {
		return
	}
	defer ctrl.pvcQueue.Done(key)

	pvcName := key.(string)
	glog.V(4).Infof("Started PVC processing %q", pvcName)

	// get PVC to process
	pvc, err := ctrl.pvcLister.Get(pvcName)
	if err != nil {
		if apierrs.IsNotFound(err) {
			// PVC was deleted in the meantime, ignore.
			glog.V(3).Infof("PVC %q deleted, ignoring", pvcName)
			return
		}
		glog.Errorf("Error getting PersistentVolumeClaim %q: %v", pvcName, err)
		ctrl.pvcQueue.AddRateLimited(pvcName)
		return
	}
	ctrl.handler.SyncNewOrUpdatedPersistentVolumeClaim(pvc)
}
