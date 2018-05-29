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
	"time"

	"github.com/golang/glog"
	"github.com/kubernetes-csi/external-snapshotter/pkg/connection"
	"k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	storage "k8s.io/api/storage/v1alpha1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/util/wait"
	storageinformers "k8s.io/client-go/informers/storage/v1alpha1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	storagelisters "k8s.io/client-go/listers/storage/v1alpha1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/util/goroutinemap"
	"k8s.io/kubernetes/pkg/util/goroutinemap/exponentialbackoff"
	"k8s.io/apimachinery/pkg/api/errors"
)

var (
	// This annotation is added to a volumeSnapshot that is supposed to create snapshot
	// Its value is name of volume plugin that is supposed to create
	// a snapshot data for this snapshot.
	annStorageSnapshotter = "snapshot.beta.kubernetes.io/storage-snapshotter"

	// This annotation is added to a volumeSnapshotData that has been created by
	// Kubernetes. Its value is name of snapshotter that created the volumeSnapshotData.
	annSnapshotCreated = "snapshotdata.kubernetes.io/snapshotted-by"

	// Statuses of snapshot creation process
	statusReady   string = "ready"
	statusError   string = "error"
	statusPending string = "uploading"
	statusNew     string = "new"
)

type CSISnapshotController struct {
	client          kubernetes.Interface
	snapshotterName string
	eventRecorder   record.EventRecorder
	vsQueue         workqueue.RateLimitingInterface
	vsdQueue        workqueue.RateLimitingInterface

	vsLister        storagelisters.VolumeSnapshotLister
	vsListerSynced  cache.InformerSynced
	vsdLister       storagelisters.VolumeSnapshotDataLister
	vsdListerSynced cache.InformerSynced

	vsStore  cache.Store
	vsdStore cache.Store

	handler Handler
	// Map of scheduled/running operations.
	runningOperations goroutinemap.GoRoutineMap

	createSnapshotDataRetryCount int
	createSnapshotDataInterval   time.Duration
	resyncPeriod                 time.Duration
}

// NewCSISnapshotController returns a new *CSISnapshotController
func NewCSISnapshotController(
	client kubernetes.Interface,
	snapshotterName string,
	volumeSnapshotInformer storageinformers.VolumeSnapshotInformer,
	volumeSnapshotDataInformer storageinformers.VolumeSnapshotDataInformer,
	createSnapshotDataRetryCount int,
	createSnapshotDataInterval time.Duration,
	conn connection.CSIConnection,
	timeout time.Duration,
	resyncPeriod time.Duration,
) *CSISnapshotController {
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: client.Core().Events(v1.NamespaceAll)})
	var eventRecorder record.EventRecorder
	eventRecorder = broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: fmt.Sprintf("csi-snapshotter %s", snapshotterName)})

	ctrl := &CSISnapshotController{
		client:                       client,
		snapshotterName:              snapshotterName,
		eventRecorder:                eventRecorder,
		handler:                      NewCSIHandler(conn, timeout),
		runningOperations:            goroutinemap.NewGoRoutineMap(true),
		createSnapshotDataRetryCount: createSnapshotDataRetryCount,
		createSnapshotDataInterval:   createSnapshotDataInterval,
		resyncPeriod:                 resyncPeriod,
		vsStore:                      cache.NewStore(cache.DeletionHandlingMetaNamespaceKeyFunc),
		vsdStore:                     cache.NewStore(cache.DeletionHandlingMetaNamespaceKeyFunc),
		vsQueue:                      workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "csi-snapshotter-vs"),
		vsdQueue:                     workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "csi-snapshotter-vsd"),
	}

	volumeSnapshotInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { ctrl.enqueueVsWork(obj) },
			UpdateFunc: func(oldObj, newObj interface{}) { ctrl.enqueueVsWork(newObj) },
			DeleteFunc: func(obj interface{}) { ctrl.enqueueVsWork(obj) },
		},
		ctrl.resyncPeriod,
	)
	ctrl.vsLister = volumeSnapshotInformer.Lister()
	ctrl.vsListerSynced = volumeSnapshotInformer.Informer().HasSynced

	volumeSnapshotDataInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { ctrl.enqueueVsdWork(obj) },
			UpdateFunc: func(oldObj, newObj interface{}) { ctrl.enqueueVsdWork(newObj) },
			DeleteFunc: func(obj interface{}) { ctrl.enqueueVsdWork(obj) },
		},
		ctrl.resyncPeriod,
	)
	ctrl.vsdLister = volumeSnapshotDataInformer.Lister()
	ctrl.vsdListerSynced = volumeSnapshotDataInformer.Informer().HasSynced

	return ctrl
}

func (ctrl *CSISnapshotController) Run(workers int, stopCh <-chan struct{}) {
	defer ctrl.vsQueue.ShutDown()
	defer ctrl.vsdQueue.ShutDown()

	glog.Infof("Starting CSI snapshotter")
	defer glog.Infof("Shutting CSI snapshotter")

	if !cache.WaitForCacheSync(stopCh, ctrl.vsListerSynced, ctrl.vsdListerSynced) {
		glog.Errorf("Cannot sync caches")
		return
	}

	ctrl.initializeCaches(ctrl.vsLister, ctrl.vsdLister)

	for i := 0; i < workers; i++ {
		go wait.Until(ctrl.vsWorker, 0, stopCh)
		go wait.Until(ctrl.vsdWorker, 0, stopCh)
	}

	<-stopCh
}

// enqueueVsWork adds snapshot to given work queue.
func (ctrl *CSISnapshotController) enqueueVsWork(obj interface{}) {
	// Beware of "xxx deleted" events
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}
	if vs, ok := obj.(*storage.VolumeSnapshot); ok {
		objName, err := cache.DeletionHandlingMetaNamespaceKeyFunc(vs)
		if err != nil {
			glog.Errorf("failed to get key from object: %v, %v", err, vs)
			return
		}
		glog.V(5).Infof("enqueued %q for sync", objName)
		ctrl.vsQueue.Add(objName)
	}
}

// enqueueVsdWork adds snapshot data to given work queue.
func (ctrl *CSISnapshotController) enqueueVsdWork(obj interface{}) {
	// Beware of "xxx deleted" events
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}
	if vsd, ok := obj.(*storage.VolumeSnapshotData); ok {
		objName, err := cache.DeletionHandlingMetaNamespaceKeyFunc(vsd)
		if err != nil {
			glog.Errorf("failed to get key from object: %v, %v", err, vsd)
			return
		}
		glog.V(5).Infof("enqueued %q for sync", objName)
		ctrl.vsdQueue.Add(objName)
	}
}

// vsWorker processes items from vsQueue. It must run only once,
// syncVolume is not assured to be reentrant.
func (ctrl *CSISnapshotController) vsWorker() {
	workFunc := func() bool {
		keyObj, quit := ctrl.vsQueue.Get()
		if quit {
			return true
		}
		defer ctrl.vsQueue.Done(keyObj)
		key := keyObj.(string)
		glog.V(5).Infof("vsWorker[%s]", key)

		namespace, name, err := cache.SplitMetaNamespaceKey(key)
		if err != nil {
			glog.V(4).Infof("error getting namespace & name of snapshot %q to get snapshot from informer: %v", key, err)
			return false
		}
		snapshot, err := ctrl.vsLister.VolumeSnapshots(namespace).Get(name)
		if err == nil {
			if ctrl.shouldProcessVS(snapshot) {
				// The volume snapshot still exists in informer cache, the event must have
				// been add/update/sync
				ctrl.updateVs(snapshot)
			}
			return false
		}
		if !errors.IsNotFound(err) {
			glog.V(2).Infof("error getting snapshot %q from informer: %v", key, err)
			return false
		}
		// The snapshot is not in informer cache, the event must have been "delete"
		vsObj, found, err := ctrl.vsStore.GetByKey(key)
		if err != nil {
			glog.V(2).Infof("error getting snapshot %q from cache: %v", key, err)
			return false
		}
		if !found {
			// The controller has already processed the delete event and
			// deleted the snapshot from its cache
			glog.V(2).Infof("deletion of vs %q was already processed", key)
			return false
		}
		snapshot, ok := vsObj.(*storage.VolumeSnapshot)
		if !ok {
			glog.Errorf("expected vs, got %+v", vsObj)
			return false
		}
		if ctrl.shouldProcessVS(snapshot) {
			ctrl.deleteVS(snapshot)
		}
		return false
	}

	for {
		if quit := workFunc(); quit {
			glog.Infof("snapshot worker queue shutting down")
			return
		}
	}
}

// vsdWorker processes items from vsdQueue. It must run only once,
// syncVsd is not assured to be reentrant.
func (ctrl *CSISnapshotController) vsdWorker() {
	workFunc := func() bool {
		keyObj, quit := ctrl.vsdQueue.Get()
		if quit {
			return true
		}
		defer ctrl.vsdQueue.Done(keyObj)
		key := keyObj.(string)
		glog.V(5).Infof("vsdWorker[%s]", key)

		_, name, err := cache.SplitMetaNamespaceKey(key)
		if err != nil {
			glog.V(4).Infof("error getting name of snapshotData %q to get snapshotData from informer: %v", key, err)
			return false
		}
		vsd, err := ctrl.vsdLister.Get(name)
		if err == nil {
			// The volume still exists in informer cache, the event must have
			// been add/update/sync
			ctrl.updateVsd(vsd)
			return false
		}
		if !errors.IsNotFound(err) {
			glog.V(2).Infof("error getting vsd %q from informer: %v", key, err)
			return false
		}

		// The vsd is not in informer cache, the event must have been
		// "delete"
		vsdObj, found, err := ctrl.vsdStore.GetByKey(key)
		if err != nil {
			glog.V(2).Infof("error getting vsd %q from cache: %v", key, err)
			return false
		}
		if !found {
			// The controller has already processed the delete event and
			// deleted the volume from its cache
			glog.V(2).Infof("deletion of vsd %q was already processed", key)
			return false
		}
		vsd, ok := vsdObj.(*storage.VolumeSnapshotData)
		if !ok {
			glog.Errorf("expected vsd, got %+v", vsd)
			return false
		}
		ctrl.deleteVSD(vsd)
		return false
	}

	for {
		if quit := workFunc(); quit {
			glog.Infof("vsd worker queue shutting down")
			return
		}
	}
}

// shouldProcessVS detect if snapshotter is the same as controller snapshotter.
func (ctrl *CSISnapshotController) shouldProcessVS(snapshot *storage.VolumeSnapshot) bool {
	class, err := ctrl.getClassFromVolumeSnapshot(snapshot)
	if err != nil {
		return false
	}
	if class.Snapshotter != ctrl.snapshotterName {
		glog.V(4).Infof("Skipping VolumeSnapshot %s for snapshotter %s", vsToVsKey(snapshot), class.Snapshotter)
		return false
	}
	return true
}

// updateVs runs in worker thread and handles "vs added",
// "vs updated" and "periodic sync" events.
func (ctrl *CSISnapshotController) updateVs(vs *storage.VolumeSnapshot) {
	// Store the new vs version in the cache and do not process it if this is
	// an old version.
	new, err := ctrl.storeVSUpdate(vs)
	if err != nil {
		glog.Errorf("%v", err)
	}
	if !new {
		return
	}
	err = ctrl.syncVS(vs)
	if err != nil {
		if errors.IsConflict(err) {
			// Version conflict error happens quite often and the controller
			// recovers from it easily.
			glog.V(3).Infof("could not sync claim %q: %+v", vsToVsKey(vs), err)
		} else {
			glog.Errorf("could not sync volume %q: %+v", vsToVsKey(vs), err)
		}
	}
}

// updateVsd runs in worker thread and handles "vsd added",
// "vsd updated" and "periodic sync" events.
func (ctrl *CSISnapshotController) updateVsd(vsd *storage.VolumeSnapshotData) {
	// Store the new vs version in the cache and do not process it if this is
	// an old version.
	new, err := ctrl.storeVSDUpdate(vsd)
	if err != nil {
		glog.Errorf("%v", err)
	}
	if !new {
		return
	}
	err = ctrl.syncVSD(vsd)
	if err != nil {
		if errors.IsConflict(err) {
			// Version conflict error happens quite often and the controller
			// recovers from it easily.
			glog.V(3).Infof("could not sync vsd %q: %+v", vsd.Name, err)
		} else {
			glog.Errorf("could not sync vsd %q: %+v", vsd.Name, err)
		}
	}
}

// syncVSD deals with one key off the queue.  It returns false when it's time to quit.
func (ctrl *CSISnapshotController) syncVSD(vsd *storage.VolumeSnapshotData) error {
	glog.V(4).Infof("synchronizing VolumeSnapshotData[%s]", vsd.Name)

	// VolumeSnapshotData is not bind to any VolumeSnapshot, this case rare and we just return err
	if vsd.Spec.VolumeSnapshotRef == nil {
		// Vsd is not bind
		glog.V(4).Infof("synchronizing VolumeSnapshotData[%s]: vsd is not bind", vsd.Name)
		return fmt.Errorf("volumeSnapshotData %s is not bind to any VolumeSnapshot", vsd.Name)
	} else {
		glog.V(4).Infof("synchronizing VolumeSnapshotData[%s]: vsd is bound to vs %s", vsd.Name, vsrefToVsKey(vsd.Spec.VolumeSnapshotRef))
		// Get the VS by _name_
		var vs *storage.VolumeSnapshot
		vsName := vsrefToVsKey(vsd.Spec.VolumeSnapshotRef)
		obj, found, err := ctrl.vsStore.GetByKey(vsName)
		if err != nil {
			return err
		}
		if !found {
			glog.V(4).Infof("synchronizing VolumeSnapshotData[%s]: vs %s not found", vsd.Name, vsrefToVsKey(vsd.Spec.VolumeSnapshotRef))
			// Fall through with vs = nil
		} else {
			var ok bool
			vs, ok = obj.(*storage.VolumeSnapshot)
			if !ok {
				return fmt.Errorf("cannot convert object from vs cache to vs %q!?: %#v", vsd.Name, obj)
			}
			glog.V(4).Infof("synchronizing VolumeSnapshotData[%s]: vs %s found", vsd.Name, vsrefToVsKey(vsd.Spec.VolumeSnapshotRef))
		}
		if vs != nil && vs.UID != vsd.Spec.VolumeSnapshotRef.UID {
			// The vs that the vsd was pointing to was deleted, and another
			// with the same name created.
			glog.V(4).Infof("synchronizing VolumeSnapshotData[%s]: vsd %s has different UID, the old one must have been deleted", vsd.Name, vsrefToVsKey(vsd.Spec.VolumeSnapshotRef))
			// Treat the volume as bound to a missing claim.
			vs = nil
		}
		if vs == nil {
			ctrl.deleteSnapshotData(vsd)
		}
	}
	return nil
}

// syncVS deals with one key off the queue.  It returns false when it's time to quit.
func (ctrl *CSISnapshotController) syncVS(vs *storage.VolumeSnapshot) error {
	uniqueSnapshotName := vsToVsKey(vs)

	// vs has not been bound
	if vs.Spec.SnapshotDataName == "" {
		// if annStorageSnapshotter is not set, snapshotting is not triggered yet
		if _, ok := vs.Annotations[annStorageSnapshotter]; ok {
			var snapshotDataObj *storage.VolumeSnapshotData
			// Check whether snapshotData object is already created or not. If yes, snapshot is already
			// triggered through cloud provider, bind it and return pending state
			if snapshotDataObj = ctrl.getMatchVsd(vs); snapshotDataObj != nil {
				glog.Infof("Find snapshot data object %s from snapshot %s", snapshotDataObj.Name, uniqueSnapshotName)
				if _, err := ctrl.bindandUpdateVolumeSnapshot(snapshotDataObj, vs); err != nil {
					return err
				}
			}
			return nil
		}
		glog.Infof("syncSnapshot: Creating snapshot %s ...", uniqueSnapshotName)
		if err := ctrl.createSnapshot(vs); err != nil {
			return err
		}
		return nil
	} else {
		obj, found, err := ctrl.vsdStore.GetByKey(vs.Spec.SnapshotDataName)
		if err != nil {
			return err
		}
		if !found {
			// vs is bound to a non-existing vsd.
			return fmt.Errorf("snapshot %s is bound to a non-existing vsd %s", uniqueSnapshotName, vs.Spec.SnapshotDataName)
		}
		vsd, ok := obj.(*storage.VolumeSnapshotData)
		if !ok {
			return fmt.Errorf("cannot convert object from vsd cache to vsd %q!?: %#v", vs.Spec.SnapshotDataName, obj)
		}
		status := ctrl.getSimplifiedSnapshotStatus(vs.Status.Conditions)

		switch status {
		case statusReady:
			glog.Infof("Snapshot %s created successfully.", uniqueSnapshotName)
			return nil
		case statusError:
			glog.Infof("syncSnapshot: Error creating snapshot %s.", uniqueSnapshotName)
			return fmt.Errorf("error creating snapshot %s", uniqueSnapshotName)
		case statusPending:
			glog.V(4).Infof("syncSnapshot: Snapshot %s is Pending.", uniqueSnapshotName)
			// Query the driver for the status of the snapshot with snapshot id
			// from VolumeSnapshotData object.
			snapshotDataCon, err := ctrl.handler.listSnapshots(vsd)
			if err != nil {
				return fmt.Errorf("failed to check snapshot state %s with error %v", uniqueSnapshotName, err)
			}
			if err := ctrl.updateVolumeSnapshotDataStatus(vsd, snapshotDataCon); err != nil {
				return err
			}
			snapshotCon := GenSnapshotStatus(snapshotDataCon)
			if err := ctrl.updateVolumeSnapshotStatus(vs, snapshotCon); err != nil {
				return err
			}
			return nil
		case statusNew:
			glog.Infof("syncSnapshot: Binding snapshot %s ...", uniqueSnapshotName)
			if _, err := ctrl.bindandUpdateVolumeSnapshot(vsd, vs); err != nil {
				return err
			}
			return nil
		}
		return fmt.Errorf("error occurred when creating snapshot %s, unknown status %s", uniqueSnapshotName, status)
	}
}

// getSimplifiedSnapshotStatus get status fot snapshot.
func (ctrl *CSISnapshotController) getSimplifiedSnapshotStatus(conditions []storage.VolumeSnapshotCondition) string {
	if conditions == nil {
		glog.Errorf("No conditions for this snapshot yet.")
		return statusNew
	}
	if len(conditions) == 0 {
		glog.Errorf("Empty condition.")
		return statusNew
	}

	//index := len(conditions) - 1
	lastCondition := conditions[len(conditions)-1]
	switch lastCondition.Type {
	case storage.VolumeSnapshotConditionReady:
		if lastCondition.Status == v1.ConditionTrue {
			return statusReady
		}
	case storage.VolumeSnapshotConditionError:
		return statusError
	case storage.VolumeSnapshotConditionPending:
		if lastCondition.Status == v1.ConditionTrue ||
			lastCondition.Status == v1.ConditionUnknown {
			return statusPending
		}
	}
	return statusNew
}

// UpdateVolumeSnapshotStatus update VolumeSnapshot status if the condition is changed.
func (ctrl *CSISnapshotController) updateVolumeSnapshotStatus(snapshot *storage.VolumeSnapshot, condition *storage.VolumeSnapshotCondition) error {
	snapshotObj, err := ctrl.client.StorageV1alpha1().VolumeSnapshots(snapshot.Namespace).Get(snapshot.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("error get volume snapshot %s from  api server: %s", vsToVsKey(snapshot), err)
	}
	oldStatus := snapshotObj.Status.DeepCopy()

	status := snapshotObj.Status
	isEqual := false
	if oldStatus.Conditions == nil ||
		len(oldStatus.Conditions) == 0 ||
		condition.Type != oldStatus.Conditions[len(oldStatus.Conditions)-1].Type {
		status.Conditions = append(status.Conditions, *condition)
	} else {
		oldCondition := oldStatus.Conditions[len(oldStatus.Conditions)-1]
		if condition.Status == oldCondition.Status {
			condition.LastTransitionTime = oldCondition.LastTransitionTime
		}
		status.Conditions[len(status.Conditions)-1] = *condition
		isEqual = condition.Type == oldCondition.Type &&
			condition.Status == oldCondition.Status &&
			condition.Reason == oldCondition.Reason &&
			condition.Message == oldCondition.Message &&
			condition.LastTransitionTime.Equal(&oldCondition.LastTransitionTime)
	}

	if !isEqual {
		snapshotObj.Status = status
		newSnapshotObj, err := ctrl.client.StorageV1alpha1().VolumeSnapshots(snapshot.Namespace).UpdateStatus(snapshotObj)
		if err != nil {
			return fmt.Errorf("error update status for volume snapshot %s: %s", vsToVsKey(snapshot), err)
		}
		_, updateErr := ctrl.storeVSUpdate(newSnapshotObj)
		if updateErr != nil {
			// We will get an "snapshot update" event soon, this is not a big error
			glog.V(4).Infof("UpdateVolumeSnapshotStatus [%s]: cannot update internal cache: %v", vsToVsKey(newSnapshotObj), updateErr)
		}
		glog.Infof("UpdateVolumeSnapshotStatus finishes %+v", newSnapshotObj)
		return nil
	}

	return nil
}

// UpdateVolumeSnapshotDataStatus update VolumeSnapshotData status if the condition is changed.
func (ctrl *CSISnapshotController) updateVolumeSnapshotDataStatus(vsd *storage.VolumeSnapshotData, condition *storage.VolumeSnapshotDataCondition) error {
	snapshotDataObj, err := ctrl.client.StorageV1alpha1().VolumeSnapshotDatas().Get(vsd.Name, metav1.GetOptions{})
	if err != nil {
		return fmt.Errorf("error get volume snapshot data %s from  api server: %s", vsd.Name, err)
	}
	oldStatus := snapshotDataObj.Status.DeepCopy()

	status := snapshotDataObj.Status
	isEqual := false
	if oldStatus.Conditions == nil ||
		len(oldStatus.Conditions) == 0 ||
		condition.Type != oldStatus.Conditions[len(oldStatus.Conditions)-1].Type {
		status.Conditions = append(status.Conditions, *condition)
	} else {
		oldCondition := oldStatus.Conditions[len(oldStatus.Conditions)-1]
		if condition.Status == oldCondition.Status {
			condition.LastTransitionTime = oldCondition.LastTransitionTime
		}
		status.Conditions[len(status.Conditions)-1] = *condition
		isEqual = condition.Type == oldCondition.Type &&
			condition.Status == oldCondition.Status &&
			condition.Reason == oldCondition.Reason &&
			condition.Message == oldCondition.Message &&
			condition.LastTransitionTime.Equal(&oldCondition.LastTransitionTime)
	}

	if !isEqual {
		snapshotDataObj.Status = status
		newSnapshotDataObj, err := ctrl.client.StorageV1alpha1().VolumeSnapshotDatas().UpdateStatus(snapshotDataObj)
		if err != nil {
			return fmt.Errorf("error update status for volume snapshot data %s: %s", snapshotDataObj.Name, err)
		}
		_, updateErr := ctrl.storeVSDUpdate(newSnapshotDataObj)
		if updateErr != nil {
			// We will get an "snapshot update" event soon, this is not a big error
			glog.V(4).Infof("UpdateVolumeSnapshotStatus [%s]: cannot update internal cache: %v", newSnapshotDataObj.Name, updateErr)
		}
		glog.Infof("UpdateVolumeSnapshotStatus finishes %+v", newSnapshotDataObj)
		return nil
	}

	return nil
}

// getMatchVsd looks up VolumeSnapshotData for a VolumeSnapshot named snapshotName
func (ctrl *CSISnapshotController) getMatchVsd(vs *storage.VolumeSnapshot) *storage.VolumeSnapshotData {
	var snapshotDataObj *storage.VolumeSnapshotData
	var found bool

	objs := ctrl.vsdStore.List()
	for _, obj := range objs {
		vsd := obj.(*storage.VolumeSnapshotData)
		if vsd.Spec.VolumeSnapshotRef != nil &&
			vsd.Spec.VolumeSnapshotRef.Name == vs.Name &&
			vsd.Spec.VolumeSnapshotRef.Namespace == vs.Namespace &&
			vsd.Spec.VolumeSnapshotRef.UID == vs.UID {
			found = true
			snapshotDataObj = vsd
			break
		}
	}

	if !found {
		glog.V(4).Infof("Error: no VolumeSnapshotData for VolumeSnapshot %s found", vsToVsKey(vs))
		return nil
	}

	return snapshotDataObj
}

// deleteVS runs in worker thread and handles "snapshot deleted" event.
func (ctrl *CSISnapshotController) deleteVS(vs *storage.VolumeSnapshot) {
	_ = ctrl.vsStore.Delete(vs)
	glog.V(4).Infof("vs %q deleted", vsToVsKey(vs))

	snapshotDataName := vs.Spec.SnapshotDataName
	if snapshotDataName == "" {
		glog.V(5).Infof("deleteVS[%q]: vsd not bound", vsToVsKey(vs))
		return
	}
	// sync the vsd when its vs is deleted.  Explicitly sync'ing the
	// vsd here in response to vs deletion prevents the vsd from
	// waiting until the next sync period for its Release.
	glog.V(5).Infof("deleteVS[%q]: scheduling sync of vsd %s", vsToVsKey(vs), snapshotDataName)
	ctrl.vsdQueue.Add(snapshotDataName)
}

// deleteVSD runs in worker thread and handles "snapshot deleted" event.
func (ctrl *CSISnapshotController) deleteVSD(vsd *storage.VolumeSnapshotData) {
	_ = ctrl.vsdStore.Delete(vsd)
	glog.V(4).Infof("vsd %q deleted", vsd.Name)

	snapshotName := vsrefToVsKey(vsd.Spec.VolumeSnapshotRef)
	if snapshotName == "" {
		glog.V(5).Infof("deleteVSD[%q]: vsd not bound", vsd.Name)
		return
	}
	// sync the vs when its vs is deleted.  Explicitly sync'ing the
	// vs here in response to vsd deletion prevents the vs from
	// waiting until the next sync period for its Release.
	glog.V(5).Infof("deleteVSD[%q]: scheduling sync of vs %s", vsd.Name, snapshotName)
	ctrl.vsdQueue.Add(snapshotName)
}

// initializeCaches fills all controller caches with initial data from etcd in
// order to have the caches already filled when first addVS/addVSD to
// perform initial synchronization of the controller.
func (ctrl *CSISnapshotController) initializeCaches(vsLister storagelisters.VolumeSnapshotLister, vsdLister storagelisters.VolumeSnapshotDataLister) {
	vsList, err := vsLister.List(labels.Everything())
	if err != nil {
		glog.Errorf("CSISnapshotController can't initialize caches: %v", err)
		return
	}
	for _, vs := range vsList {
		vsClone := vs.DeepCopy()
		if _, err = ctrl.storeVSUpdate(vsClone); err != nil {
			glog.Errorf("error updating volume snapshot cache: %v", err)
		}
	}

	vsdList, err := vsdLister.List(labels.Everything())
	if err != nil {
		glog.Errorf("CSISnapshotController can't initialize caches: %v", err)
		return
	}
	for _, vsd := range vsdList {
		vsdClone := vsd.DeepCopy()
		if _, err = ctrl.storeVSDUpdate(vsdClone); err != nil {
			glog.Errorf("error updating volume snapshot cache: %v", err)
		}
	}

	glog.V(4).Infof("controller initialized")
}

// deleteSnapshotData starts delete action.
func (ctrl *CSISnapshotController) deleteSnapshotData(vsd *storage.VolumeSnapshotData) {
	operationName := fmt.Sprintf("delete-%s[%s]", vsd.Name, string(vsd.UID))
	glog.V(4).Infof("Snapshotter is about to delete volume snapshot and the operation named %s", operationName)
	ctrl.scheduleOperation(operationName, func() error {
		return ctrl.deleteSnapshotDataOperation(vsd)
	})
}

// Delete a snapshot
// 1. Find the SnapshotData corresponding to Snapshot
//   1a: Not found => finish (it's been deleted already)
// 2. Ask the backend to remove the snapshot device
// 3. Delete the SnapshotData object
// 4. Remove the Snapshot from vsStore
// 5. Finish
func (ctrl *CSISnapshotController) deleteSnapshotDataOperation(vsd *storage.VolumeSnapshotData) error {
	glog.V(4).Infof("deleteSnapshotOperation [%s] started", vsd.Name)

	err := ctrl.handler.deleteSnapshot(vsd)
	if err != nil {
		return fmt.Errorf("failed to delete snapshot %#v, err: %v", vsd.Name, err)
	}

	err = ctrl.client.StorageV1alpha1().VolumeSnapshotDatas().Delete(vsd.Name, &metav1.DeleteOptions{})
	if err != nil {
		return fmt.Errorf("failed to delete VolumeSnapshotData %s from API server: %q", vsd.Name, err)
	}

	return nil
}

// getSnapshotDataFromSnapshot looks up VolumeSnapshotData from a VolumeSnapshot.
func (ctrl *CSISnapshotController) getSnapshotDataFromSnapshot(vs *storage.VolumeSnapshot) (*storage.VolumeSnapshotData, error) {
	snapshotDataName := vs.Spec.SnapshotDataName
	if snapshotDataName == "" {
		return nil, fmt.Errorf("could not find snapshot data object for %s: SnapshotDataName in snapshot spec is empty", vsToVsKey(vs))
	}

	snapshotDataObj, err := ctrl.client.StorageV1alpha1().VolumeSnapshotDatas().Get(snapshotDataName, metav1.GetOptions{})
	if err != nil {
		glog.Errorf("Error retrieving the VolumeSnapshotData objects from API server: %v", err)
		return nil, fmt.Errorf("could not get snapshot data object %s: %v", snapshotDataName, err)
	}

	return snapshotDataObj, nil
}

// scheduleOperation starts given asynchronous operation on given volume. It
// makes sure the operation is already not running.
func (ctrl *CSISnapshotController) scheduleOperation(operationName string, operation func() error) {
	glog.V(4).Infof("scheduleOperation[%s]", operationName)

	err := ctrl.runningOperations.Run(operationName, operation)
	if err != nil {
		switch {
		case goroutinemap.IsAlreadyExists(err):
			glog.V(4).Infof("operation %q is already running, skipping", operationName)
		case exponentialbackoff.IsExponentialBackoff(err):
			glog.V(4).Infof("operation %q postponed due to exponential backoff", operationName)
		default:
			glog.Errorf("error scheduling operation %q: %v", operationName, err)
		}
	}
}

func (ctrl *CSISnapshotController) storeVSUpdate(vs interface{}) (bool, error) {
	return storeObjectUpdate(ctrl.vsStore, vs, "vs")
}

func (ctrl *CSISnapshotController) storeVSDUpdate(vsd interface{}) (bool, error) {
	return storeObjectUpdate(ctrl.vsdStore, vsd, "vsd")
}

// createSnapshot starts new asynchronous operation to create snapshot data for snapshot
func (ctrl *CSISnapshotController) createSnapshot(vs *storage.VolumeSnapshot) error {
	glog.V(4).Infof("createSnapshot[%s]: started", vsToVsKey(vs), )
	opName := fmt.Sprintf("create-%s[%s]", vsToVsKey(vs), string(vs.UID))
	ctrl.scheduleOperation(opName, func() error {
		ctrl.createSnapshotOperation(vs)
		return nil
	})
	return nil
}

// The function goes through the whole snapshot creation process.
// 1. Update VolumeSnapshot metadata to include the snapshotted PV name, timestamp and snapshot uid, also generate tag for cloud provider
// 2. Trigger the snapshot through cloud provider and attach the tag to the snapshot.
// 3. Create the VolumeSnapshotData object with the snapshot id information returned from step 2.
// 4. Bind the VolumeSnapshot and VolumeSnapshotData object
// 5. Query the snapshot status through cloud provider and update the status until snapshot is ready or fails.
func (ctrl *CSISnapshotController) createSnapshotOperation(snapshot *storage.VolumeSnapshot) error {
	glog.Infof("createSnapshot: Creating snapshot %s through the plugin ...", vsToVsKey(snapshot))

	class, err := ctrl.getClassFromVolumeSnapshot(snapshot)
	if err != nil {
		return err
	}
	newVs, err := ctrl.setVsSnapshotter(snapshot, class)
	if err != nil {
		// Save failed, the controller will retry in the next sync
		glog.V(2).Infof("error saving claim %s: %v", vsToVsKey(newVs), err)
		return err
	}

	//  A previous createSnapshot may just have finished while we were waiting for
	//  the locks. Check that snapshot data (with deterministic name) hasn't been created
	//  yet.
	snapDataName := connection.GetSnapshotDataNameForSnapshot(snapshot)
	vsd, err := ctrl.client.StorageV1alpha1().VolumeSnapshotDatas().Get(snapDataName, metav1.GetOptions{})
	if err == nil && vsd != nil {
		// Volume snapshot data has been already created, nothing to do.
		glog.V(4).Infof("createSnapshot [%s]: volume snapshot data already exists, skipping", vsToVsKey(snapshot))
		return nil
	}

	volume, err := ctrl.getVolumeFromVolumeSnapshot(snapshot)
	if err != nil {
		return err
	}
	snapshotData, err := ctrl.handler.takeSnapshot(snapshot, volume, class.Parameters)
	if err != nil {
		return fmt.Errorf("failed to take snapshot of the volume %s: %q", volume.Name, err)
	}

	metav1.SetMetaDataAnnotation(&snapshotData.ObjectMeta, annSnapshotCreated, ctrl.snapshotterName)

	// Try to create the VSD object several times
	for i := 0; i < ctrl.createSnapshotDataRetryCount; i++ {
		glog.V(4).Infof("createSnapshot [%s]: trying to save volume snapshot data %s", vsToVsKey(snapshot), snapshotData.Name)
		if _, err = ctrl.client.StorageV1alpha1().VolumeSnapshotDatas().Create(snapshotData); err == nil || apierrs.IsAlreadyExists(err) {
			// Save succeeded.
			if err != nil {
				glog.V(3).Infof("volume snapshot data %q for snapshot %q already exists, reusing", snapshotData.Name, vsToVsKey(snapshot))
				err = nil
			} else {
				glog.V(3).Infof("volume snapshot data %q for snapshot %q saved", snapshotData.Name, vsToVsKey(snapshot))
			}
			break
		}
		// Save failed, try again after a while.
		glog.V(3).Infof("failed to save volume snapshot data %q for snapshot %q: %v", snapshotData.Name, vsToVsKey(snapshot), err)
		time.Sleep(ctrl.createSnapshotDataInterval)
	}

	if err != nil {
		// Save failed. Now we have a storage asset outside of Kubernetes,
		// but we don't have appropriate volumesnapshotdata object for it.
		// Emit some event here and try to delete the storage asset several
		// times.
		strerr := fmt.Sprintf("Error creating volume snapshot data object for snapshot %s: %v. Deleting the snapshot data.", vsToVsKey(snapshot), err)
		glog.Error(strerr)
		ctrl.eventRecorder.Event(snapshot, v1.EventTypeWarning, "CreateSnapshotDataFailed", strerr)

		for i := 0; i < ctrl.createSnapshotDataRetryCount; i++ {
			if err = ctrl.handler.deleteSnapshot(snapshotData); err == nil {
				// Delete succeeded
				glog.V(4).Infof("createSnapshot [%s]: cleaning snapshot data %s succeeded", vsToVsKey(snapshot), snapshotData.Name)
				break
			}
			// Delete failed, try again after a while.
			glog.Infof("failed to delete snapshot data %q: %v", snapshotData.Name, err)
			time.Sleep(ctrl.createSnapshotDataInterval)
		}

		if err != nil {
			// Delete failed several times. There is an orphaned volume snapshot data and there
			// is nothing we can do about it.
			strerr := fmt.Sprintf("Error cleaning volume snapshot data for snapshot %s: %v. Please delete manually.", vsToVsKey(snapshot), err)
			glog.Error(strerr)
			ctrl.eventRecorder.Event(snapshot, v1.EventTypeWarning, "SnapshotDataCleanupFailed", strerr)
		}
	} else {
		// save succeeded, bind and update status fot snapshot.
		_, err := ctrl.bindandUpdateVolumeSnapshot(snapshotData, snapshot)
		if err != nil {
			return err
		}
	}

	return nil
}

func (ctrl *CSISnapshotController) bindandUpdateVolumeSnapshot(snapshotData *storage.VolumeSnapshotData, snapshot *storage.VolumeSnapshot) (*storage.VolumeSnapshot, error) {
	snapshotObj, err := ctrl.client.StorageV1alpha1().VolumeSnapshots(snapshot.Namespace).Get(snapshot.Name, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("error get snapshot %s from api server: %v", vsToVsKey(snapshot), err)
	}

	var updateSnapshot *storage.VolumeSnapshot
	if snapshotObj.Spec.SnapshotDataName == snapshotData.Name {
		glog.Infof("bindVolumeSnapshotDataToVolumeSnapshot: VolumeSnapshot %s already bind to volumeSnapshotData [%#v]", vsToVsKey(snapshot), snapshotData.Name)
	} else {
		snapshotObj.Spec.SnapshotDataName = snapshotData.Name
		updateSnapshot, err = ctrl.client.StorageV1alpha1().VolumeSnapshots(snapshot.Namespace).Update(snapshotObj)
		if err != nil {
			return nil, fmt.Errorf("error updating snapshot object %s on the API server: %v", vsToVsKey(updateSnapshot), err)
		}
	}

	var snapshotCon *storage.VolumeSnapshotCondition
	if snapshotData.Status.Conditions != nil && len(snapshotData.Status.Conditions) > 0 {
		ind := len(snapshotData.Status.Conditions) - 1
		snapshotCon = GenSnapshotStatus(&snapshotData.Status.Conditions[ind])
		if snapshotCon == nil {
			return updateSnapshot, nil
		}
	}
	updateSnapshot.Status = storage.VolumeSnapshotStatus{
		Conditions:[]storage.VolumeSnapshotCondition{
			*snapshotCon,
		},
	}

	glog.Infof("bindVolumeSnapshotDataToVolumeSnapshot: Updating VolumeSnapshot object [%#v]", updateSnapshot)
	result, err := ctrl.client.StorageV1alpha1().VolumeSnapshots(snapshot.Namespace).UpdateStatus(updateSnapshot)
	if err != nil {
		return nil, fmt.Errorf("error updating snapshot object %s on the API server: %v", vsToVsKey(updateSnapshot), err)
	}

	_, updateErr := ctrl.storeVSUpdate(result)
	if updateErr != nil {
		// We will get an "snapshot update" event soon, this is not a big error
		glog.V(4).Infof("bindVolumeSnapshotDataToVolumeSnapshot [%s]: cannot update internal cache: %v", vsToVsKey(result), updateErr)
	}

	return result, nil
}

// setVsSnapshotter saves
// snapshot.Annotations[annStorageSnapshotter] = class.Snapshotter
func (ctrl *CSISnapshotController) setVsSnapshotter(snapshot *storage.VolumeSnapshot, class *storagev1.StorageClass) (*storage.VolumeSnapshot, error) {
	if val, ok := snapshot.Annotations[annStorageSnapshotter]; ok && val == class.Snapshotter {
		// annotation is already set, nothing to do
		return snapshot, nil
	}

	// The volume from method args can be pointing to watcher cache. We must not
	// modify these, therefore create a copy.
	vsClone := snapshot.DeepCopy()
	metav1.SetMetaDataAnnotation(&vsClone.ObjectMeta, annStorageSnapshotter, class.Snapshotter)
	newVs, err := ctrl.client.StorageV1alpha1().VolumeSnapshots(snapshot.Namespace).Update(vsClone)
	if err != nil {
		return newVs, err
	}
	_, err = ctrl.storeVSUpdate(newVs)
	if err != nil {
		return newVs, err
	}
	return newVs, nil
}

// getVolumeFromVolumeSnapshot is a helper function to get PV from VolumeSnapshot.
func (ctrl *CSISnapshotController) getVolumeFromVolumeSnapshot(snapshot *storage.VolumeSnapshot) (*v1.PersistentVolume, error) {
	pvc, err := ctrl.getClaimFromVolumeSnapshot(snapshot)
	if err != nil {
		return nil, err
	}

	pvName := pvc.Spec.VolumeName
	pv, err := ctrl.client.CoreV1().PersistentVolumes().Get(pvName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve PV %s from the API server: %q", pvName, err)
	}
	return pv, nil
}

// getClassFromVolumeSnapshot is a helper function to get storage class from VolumeSnapshot.
func (ctrl *CSISnapshotController) getClassFromVolumeSnapshot(snapshot *storage.VolumeSnapshot) (*storagev1.StorageClass, error) {
	pvc, err := ctrl.getClaimFromVolumeSnapshot(snapshot)
	if err != nil {
		return nil, err
	}

	className := pvc.Spec.StorageClassName
	class, err := ctrl.client.StorageV1().StorageClasses().Get(*className, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve storage class %s from the API server: %q", className, err)
	}
	return class, nil
}

// getClaimFromVolumeSnapshot is a helper function to get PV from VolumeSnapshot.
func (ctrl *CSISnapshotController) getClaimFromVolumeSnapshot(snapshot *storage.VolumeSnapshot) (*v1.PersistentVolumeClaim, error) {
	pvcName := snapshot.Spec.PersistentVolumeClaimName
	if pvcName == "" {
		return nil, fmt.Errorf("the PVC name is not specified in snapshot %s", vsToVsKey(snapshot))
	}

	pvc, err := ctrl.client.CoreV1().PersistentVolumeClaims(snapshot.Namespace).Get(pvcName, metav1.GetOptions{})
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve PVC %s from the API server: %q", pvcName, err)
	}
	if pvc.Status.Phase != v1.ClaimBound {
		return nil, fmt.Errorf("the PVC %s not yet bound to a PV, will not attempt to take a snapshot yet", pvcName)
	}

	return pvc, nil
}
