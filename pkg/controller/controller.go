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
	"k8s.io/api/core/v1"
	storagev1 "k8s.io/api/storage/v1"
	storage "k8s.io/api/storage/v1alpha1"
	apierrs "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	coreinformers "k8s.io/client-go/informers/core/v1"
	storageinformers "k8s.io/client-go/informers/storage/v1alpha1"
	"k8s.io/client-go/kubernetes"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/kubernetes/scheme"
	corev1 "k8s.io/client-go/kubernetes/typed/core/v1"
	corelisters "k8s.io/client-go/listers/core/v1"
	storagelisters "k8s.io/client-go/listers/storage/v1alpha1"
	"k8s.io/client-go/tools/cache"
	"k8s.io/client-go/tools/record"
	ref "k8s.io/client-go/tools/reference"
	"k8s.io/client-go/util/workqueue"
	"k8s.io/kubernetes/pkg/util/goroutinemap"
	"k8s.io/kubernetes/pkg/util/goroutinemap/exponentialbackoff"
)

// This annotation is added to a VSD that has been dynamically created by
// Kubernetes. Its value is name of volume plugin that created the snapshot data.
// It serves both user (to show where a VSD comes from) and Kubernetes (to
// recognize dynamically created VSDs in its decisions).
const annDynamicallyCreatedBy = "vsd.kubernetes.io/created-by"

// annSnapshotDataShouldDelete is added to a VSD that will be deleted. It serves external
// snapshotter to delete the backend snapshot data. Value of this annotation does not matter.
const annSnapshotDataShouldDelete = "vsd.kubernetes.io/should-delete"

// ControllerParameters contains arguments for creation of a new
// SnapshotController controller.
type ControllerParameters struct {
	SnapshotterName              string
	CreateSnapshotDataRetryCount int
	CreateSnapshotDataInterval   time.Duration
	KubeClient                   clientset.Interface
	Handler                      Handler
	SyncPeriod                   time.Duration
	VolumeInformer               coreinformers.PersistentVolumeInformer
	ClaimInformer                coreinformers.PersistentVolumeClaimInformer
	VolumeSnapshotInformer       storageinformers.VolumeSnapshotInformer
	VolumeSnapshotDataInformer   storageinformers.VolumeSnapshotDataInformer
}

type CSISnapshotController struct {
	client          kubernetes.Interface
	snapshotterName string
	eventRecorder   record.EventRecorder
	vsQueue         workqueue.RateLimitingInterface
	vsdQueue        workqueue.RateLimitingInterface

	vsLister           storagelisters.VolumeSnapshotLister
	vsListerSynced     cache.InformerSynced
	vsdLister          storagelisters.VolumeSnapshotDataLister
	vsdListerSynced    cache.InformerSynced
	volumeLister       corelisters.PersistentVolumeLister
	volumeListerSynced cache.InformerSynced
	claimLister        corelisters.PersistentVolumeClaimLister
	claimListerSynced  cache.InformerSynced

	handler Handler
	// Map of scheduled/running operations.
	runningOperations goroutinemap.GoRoutineMap

	createSnapshotDataRetryCount int
	createSnapshotDataInterval   time.Duration
	resyncPeriod                 time.Duration
}

// NewCSISnapshotController returns a new *CSISnapshotController
func NewCSISnapshotController(p ControllerParameters) *CSISnapshotController {
	broadcaster := record.NewBroadcaster()
	broadcaster.StartRecordingToSink(&corev1.EventSinkImpl{Interface: p.KubeClient.CoreV1().Events(v1.NamespaceAll)})
	var eventRecorder record.EventRecorder
	eventRecorder = broadcaster.NewRecorder(scheme.Scheme, v1.EventSource{Component: fmt.Sprintf("csi-snapshotter %s", p.SnapshotterName)})

	ctrl := &CSISnapshotController{
		client:                       p.KubeClient,
		snapshotterName:              p.SnapshotterName,
		eventRecorder:                eventRecorder,
		handler:                      p.Handler,
		runningOperations:            goroutinemap.NewGoRoutineMap(true),
		createSnapshotDataRetryCount: p.CreateSnapshotDataRetryCount,
		createSnapshotDataInterval:   p.CreateSnapshotDataInterval,
		resyncPeriod:                 p.SyncPeriod,
		vsQueue:                      workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "csi-snapshotter-vs"),
		vsdQueue:                     workqueue.NewNamedRateLimitingQueue(workqueue.DefaultControllerRateLimiter(), "csi-snapshotter-vsd"),
	}

	p.VolumeSnapshotInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { ctrl.enqueueVsWork(obj) },
			UpdateFunc: func(oldObj, newObj interface{}) { ctrl.enqueueVsWork(newObj) },
			DeleteFunc: func(obj interface{}) { ctrl.enqueueVsWork(obj) },
		},
		ctrl.resyncPeriod,
	)
	ctrl.vsLister = p.VolumeSnapshotInformer.Lister()
	ctrl.vsListerSynced = p.VolumeSnapshotInformer.Informer().HasSynced

	p.VolumeSnapshotDataInformer.Informer().AddEventHandlerWithResyncPeriod(
		cache.ResourceEventHandlerFuncs{
			AddFunc:    func(obj interface{}) { ctrl.enqueueVsdWork(obj) },
			UpdateFunc: func(oldObj, newObj interface{}) { ctrl.enqueueVsdWork(newObj) },
			DeleteFunc: func(obj interface{}) { ctrl.enqueueVsdWork(obj) },
		},
		ctrl.resyncPeriod,
	)
	ctrl.vsdLister = p.VolumeSnapshotDataInformer.Lister()
	ctrl.vsdListerSynced = p.VolumeSnapshotDataInformer.Informer().HasSynced

	ctrl.volumeLister = p.VolumeInformer.Lister()
	ctrl.volumeListerSynced = p.VolumeInformer.Informer().HasSynced

	ctrl.claimLister = p.ClaimInformer.Lister()
	ctrl.claimListerSynced = p.ClaimInformer.Informer().HasSynced

	return ctrl
}

func (ctrl *CSISnapshotController) Run(workers int, stopCh <-chan struct{}) {
	defer ctrl.vsQueue.ShutDown()
	defer ctrl.vsdQueue.ShutDown()

	glog.Infof("Starting CSI snapshotter")
	defer glog.Infof("Shutting CSI snapshotter")

	if !cache.WaitForCacheSync(stopCh, ctrl.vsListerSynced, ctrl.vsdListerSynced, ctrl.claimListerSynced, ctrl.volumeListerSynced) {
		glog.Errorf("Cannot sync caches")
		return
	}

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

	vs, ok := obj.(*storage.VolumeSnapshot)
	if !ok {
		glog.V(5).Infof("can not convert object %v to VolumeSnapshot", obj)
	}

	if !ctrl.shouldEnqueueSnapshot(vs) {
		return
	}

	objName, err := cache.DeletionHandlingMetaNamespaceKeyFunc(vs)
	if err != nil {
		glog.Errorf("failed to get key from object: %v, %v", err, vs)
		return
	}

	glog.V(5).Infof("enqueued %q for sync", objName)
	ctrl.vsQueue.Add(objName)
}

// enqueueVsdWork adds snapshot data to given work queue.
func (ctrl *CSISnapshotController) enqueueVsdWork(obj interface{}) {
	// Beware of "xxx deleted" events
	if unknown, ok := obj.(cache.DeletedFinalStateUnknown); ok && unknown.Obj != nil {
		obj = unknown.Obj
	}
	vsd, ok := obj.(*storage.VolumeSnapshotData)
	if !ok {
		glog.V(5).Infof("can not convert object %v to VolumeSnapshotData", obj)
	}

	if !ctrl.shouldEnqueueSnapshotData(vsd) {
		return
	}

	objName, err := cache.DeletionHandlingMetaNamespaceKeyFunc(vsd)
	if err != nil {
		glog.Errorf("failed to get key from object: %v, %v", err, vsd)
		return
	}

	glog.V(5).Infof("enqueued %q for sync", objName)
	ctrl.vsdQueue.Add(objName)
}

// vsWorker processes items from vsQueue. It must run only once,
// syncVolume is not assured to be reentrant.
func (ctrl *CSISnapshotController) vsWorker() {

	key, quit := ctrl.vsQueue.Get()
	if quit {
		return
	}
	defer ctrl.vsQueue.Done(key)

	namespace, name, err := cache.SplitMetaNamespaceKey(key.(string))
	if err != nil {
		glog.V(4).Infof("error getting namespace & name of snapshot %q to get snapshot from informer: %v", key, err)
		return
	}
	glog.V(5).Infof("Started VS processing %q", key)

	vs, err := ctrl.vsLister.VolumeSnapshots(namespace).Get(name)
	if err != nil {
		if apierrs.IsNotFound(err) {
			// VolumeSnapshot was deleted in the meantime, ignore.
			glog.V(3).Infof("VSD %q deleted, ignoring", key)
			return
		}
		glog.Errorf("Error getting VolumeSnapshot %q: %v", key, err)
		ctrl.vsdQueue.AddRateLimited(key)
		return
	}

	err = ctrl.syncVS(vs)

	if err != nil {
		// Re-queue with exponential backoff
		glog.V(2).Infof("Error processing %q: %s", key, err)
		ctrl.vsdQueue.AddRateLimited(key)
		return
	}
	// The operation has finished successfully, reset exponential backoff
	ctrl.vsdQueue.Forget(key)
	glog.V(4).Infof("CSISnapshotController: finished processing %q", key)
}

// vsdWorker processes items from vsdQueue. It returns false when it's time to quit.
func (ctrl *CSISnapshotController) vsdWorker() {
	key, quit := ctrl.vsdQueue.Get()
	if quit {
		return
	}
	defer ctrl.vsdQueue.Done(key)

	vsdName := key.(string)
	glog.V(5).Infof("Started VSD processing %q", vsdName)

	vsd, err := ctrl.vsdLister.Get(vsdName)
	if err != nil {
		if apierrs.IsNotFound(err) {
			// VolumeSnapshotData was deleted in the meantime, ignore.
			glog.V(3).Infof("VSD %q deleted, ignoring", vsdName)
			return
		}
		glog.Errorf("Error getting VolumeSnapshotData %q: %v", vsdName, err)
		ctrl.vsdQueue.AddRateLimited(vsdName)
		return
	}

	err = ctrl.syncVSD(vsd)

	if err != nil {
		// Re-queue with exponential backoff
		glog.V(2).Infof("Error processing %q: %s", vsdName, err)
		ctrl.vsdQueue.AddRateLimited(vsdName)
		return
	}
	// The operation has finished successfully, reset exponential backoff
	ctrl.vsdQueue.Forget(vsdName)
	glog.V(4).Infof("CSISnapshotController: finished processing %q", vsdName)
}

// syncVSD deals with one key off the queue.  It returns false when it's time to quit.
func (ctrl *CSISnapshotController) syncVSD(vsd *storage.VolumeSnapshotData) error {
	// snapshotData should be deleted and we update the status if need.
	if !metav1.HasAnnotation(vsd.ObjectMeta, annSnapshotDataShouldDelete) {
		glog.V(4).Infof("syncVSD[%s]:poll status for snapshot data.", vsd.Name)

		if IsSnapshotDataReady(vsd) {
			// everything is ready.
			return nil
		}

		// Query the driver for the status of the snapshotData with snapshot id
		// from VolumeSnapshotData object.
		snapshotDataCon, err := ctrl.handler.listSnapshots(vsd)
		if err != nil {
			return fmt.Errorf("failed to check snapshotData state %s: %v", vsd.Name, err)
		}

		if _, err := ctrl.snapshotDataConditionUpdate(vsd, snapshotDataCon); err != nil {
			return err
		}

		return nil
	}

	// snapshotData should be deleted.
	ctrl.deleteSnapshotData(vsd)
	return nil
}

// syncVS deals with one key off the queue.  It returns false when it's time to quit.
func (ctrl *CSISnapshotController) syncVS(vs *storage.VolumeSnapshot) error {
	// SnapshotDataName is empty and we should create a snapshotData for it.
	if vs.Spec.SnapshotDataName == "" {
		if err := ctrl.createSnapshot(vs); err != nil {
			return err
		}
		return nil
	}

	return nil
}

func (ctrl *CSISnapshotController) snapshotDataConditionUpdate(snapshotData *storage.VolumeSnapshotData, condition *storage.VolumeSnapshotDataCondition) (*storage.VolumeSnapshotData, error) {
	if condition == nil {
		return snapshotData, nil
	}
	glog.V(2).Infof("Updating snapshot condition for %s to (%s==%s)", snapshotData.Name, condition.Type, condition.Status)
	if UpdateSnapshotDataCondition(&snapshotData.Status, condition) {
		newVsd, err := ctrl.client.StorageV1alpha1().VolumeSnapshotDatas().UpdateStatus(snapshotData)
		if err != nil {
			glog.V(4).Infof("updating VolumeSnapshotData[%s] status failed: %v", newVsd.Name, err)
			return newVsd, err
		}
		glog.V(2).Infof("VolumeSnapshotData %q status update success", snapshotData.Name)
		return newVsd, nil
	}
	// Nothing to do.
	return snapshotData, nil
}

// deleteSnapshotData starts delete action.
func (ctrl *CSISnapshotController) deleteSnapshotData(vsd *storage.VolumeSnapshotData) {
	operationName := fmt.Sprintf("delete-%s[%s]", vsd.Name, string(vsd.UID))
	glog.V(4).Infof("Snapshotter is about to delete volume snapshot and the operation named %s", operationName)
	ctrl.scheduleOperation(operationName, func() error {
		return ctrl.deleteSnapshotDataOperation(vsd)
	})
}

// deleteSnapshotDataOperation asks the backend to remove the snapshot device
func (ctrl *CSISnapshotController) deleteSnapshotDataOperation(vsd *storage.VolumeSnapshotData) error {
	glog.V(4).Infof("deleteSnapshotOperation [%s] started", vsd.Name)

	newVsd, err := ctrl.client.StorageV1alpha1().VolumeSnapshotDatas().Get(vsd.Name, metav1.GetOptions{})
	if err != nil {
		return nil
	}

	err = ctrl.handler.deleteSnapshot(newVsd)
	if err != nil {
		ctrl.eventRecorder.Event(newVsd, v1.EventTypeWarning, "SnapshotDataFailedDelete", err.Error())
		return fmt.Errorf("failed to delete snapshot %#v, err: %v", vsd.Name, err)
	}

	glog.Infof("deletion of snapshotData %q success", vsd.Name)

	err = ctrl.client.StorageV1alpha1().VolumeSnapshotDatas().Delete(vsd.Name, &metav1.DeleteOptions{})
	if err != nil {
		// Oops, could not delete the snapshotData and therefore the controller will
		// try to delete the snapshotData again on next update.
		glog.Infof("failed to delete snapshotData %q from database: %v", vsd.Name, err)
		return fmt.Errorf("failed to delete VolumeSnapshotData %s from API server: %q", vsd.Name, err)
	}

	glog.Infof("snapshotData %q deleted from database", vsd.Name)
	return nil
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

// createSnapshot starts new asynchronous operation to create snapshot data for snapshot
func (ctrl *CSISnapshotController) createSnapshot(vs *storage.VolumeSnapshot) error {
	glog.V(4).Infof("createSnapshot[%s]: started", vsToVsKey(vs))
	opName := fmt.Sprintf("create-%s[%s]", vsToVsKey(vs), string(vs.UID))
	ctrl.scheduleOperation(opName, func() error {
		ctrl.createSnapshotOperation(vs)
		return nil
	})
	return nil
}

// createSnapshotOperation attempts to create a snapshotData for the given snapshot.
// Returns an error for use by goroutinemap when expbackoff is enabled: if nil,
// the operation is deleted, else the operation may be retried with expbackoff.
func (ctrl *CSISnapshotController) createSnapshotOperation(snapshot *storage.VolumeSnapshot) error {
	glog.V(4).Infof("createSnapshotOperation [%s] started", vsToVsKey(snapshot))

	class, err := ctrl.getClassFromVolumeSnapshot(snapshot)
	if err != nil {
		return err
	}

	volume, err := ctrl.getVolumeFromVolumeSnapshot(snapshot)
	if err != nil {
		return err
	}

	//  A previous createSnapshot may just have finished while we were waiting for
	//  the locks. Check that snapshot data (with deterministic name) hasn't been created
	//  yet.
	snapDataName := GetSnapshotDataNameForSnapshot(snapshot)
	vsd, err := ctrl.client.StorageV1alpha1().VolumeSnapshotDatas().Get(snapDataName, metav1.GetOptions{})
	if err == nil && vsd != nil {
		// Volume snapshot data has been already created, nothing to do.
		glog.V(4).Infof("createSnapshot [%s]: volume snapshot data already exists, skipping", vsToVsKey(snapshot))
		return nil
	}

	// Prepare a volumeSnapshotRef and persistentVolumeRef early (to fail before a snapshotData is
	// created)
	volumeSnapshotRef, err := ref.GetReference(scheme.Scheme, snapshot)
	if err != nil {
		glog.V(3).Infof("unexpected error getting snapshot reference: %v", err)
		return err
	}
	persistentVolumeRef, err := ref.GetReference(scheme.Scheme, volume)
	if err != nil {
		glog.V(3).Infof("unexpected error getting volume reference: %v", err)
		return err
	}

	snapshotData, err := ctrl.handler.takeSnapshot(snapshot, volume, class.Parameters)
	if err != nil {
		strerr := fmt.Sprintf("Failed to create snapshot data with StorageClass %q: %v", class.Name, err)
		glog.V(2).Infof("failed to create snapshot data for snapshot %q with StorageClass %q: %v", vsToVsKey(snapshot), class.Name, err)
		ctrl.eventRecorder.Event(snapshot, v1.EventTypeWarning, "CreateSnapshotFailed", strerr)
		return fmt.Errorf("failed to take snapshot of the volume %s: %q", volume.Name, err)
	}

	glog.V(3).Infof("VolumeSnapshotData %q for VolumeSnapshot %q created", snapshotData.Name, vsToVsKey(snapshot))

	// Bind it to the VolumeSnapshot
	snapshotData.Name = snapDataName
	snapshotData.Spec.VolumeSnapshotRef = volumeSnapshotRef
	snapshotData.Spec.PersistentVolumeRef = persistentVolumeRef

	metav1.SetMetaDataAnnotation(&snapshotData.ObjectMeta, annDynamicallyCreatedBy, ctrl.snapshotterName)

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
		glog.V(2).Infof("VolumeSnapshotData %q created for VolumeSnapshot %q", snapshotData.Name, vsToVsKey(snapshot))
		msg := fmt.Sprintf("Successfully create snapshot data %s using %s", snapshotData.Name, ctrl.snapshotterName)
		ctrl.eventRecorder.Event(snapshot, v1.EventTypeNormal, "CreateSnapshotSucceeded", msg)
	}

	return nil
}

// getSnapshotDataFromSnapshot looks up VolumeSnapshotData from a VolumeSnapshot.
func (ctrl *CSISnapshotController) getSnapshotDataFromSnapshot(vs *storage.VolumeSnapshot) (*storage.VolumeSnapshotData, error) {
	snapshotDataName := vs.Spec.SnapshotDataName
	if snapshotDataName == "" {
		return nil, fmt.Errorf("could not find snapshot data object for %s: SnapshotDataName in snapshot spec is empty", vsToVsKey(vs))
	}

	snapshotDataObj, err := ctrl.vsdLister.Get(snapshotDataName)
	if err != nil {
		glog.Errorf("Error retrieving the VolumeSnapshotData objects from API server: %v", err)
		return nil, fmt.Errorf("could not get snapshot data object %s: %v", snapshotDataName, err)
	}

	return snapshotDataObj, nil
}

// getVolumeFromVolumeSnapshot is a helper function to get PV from VolumeSnapshot.
func (ctrl *CSISnapshotController) getVolumeFromVolumeSnapshot(snapshot *storage.VolumeSnapshot) (*v1.PersistentVolume, error) {
	pvc, err := ctrl.getClaimFromVolumeSnapshot(snapshot)
	if err != nil {
		return nil, err
	}

	pvName := pvc.Spec.VolumeName
	pv, err := ctrl.volumeLister.Get(pvName)
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

	pvc, err := ctrl.claimLister.PersistentVolumeClaims(snapshot.Namespace).Get(pvcName)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve PVC %s from the API server: %q", pvcName, err)
	}
	if pvc.Status.Phase != v1.ClaimBound {
		return nil, fmt.Errorf("the PVC %s not yet bound to a PV, will not attempt to take a snapshot yet", pvcName)
	}

	return pvc, nil
}

func (ctrl *CSISnapshotController) shouldEnqueueSnapshot(snapshot *storage.VolumeSnapshot) bool {
	storageClass, err := ctrl.getClassFromVolumeSnapshot(snapshot)
	if err != nil {
		glog.Errorf("Error getting snapshot %q's StorageClass: %v", vsToVsKey(snapshot), err)
		return false
	}

	if storageClass.Snapshotter != ctrl.snapshotterName {
		return false
	}

	return true
}

func (ctrl *CSISnapshotController) shouldEnqueueSnapshotData(snapshotData *storage.VolumeSnapshotData) bool {
	if !metav1.HasAnnotation(snapshotData.ObjectMeta, annDynamicallyCreatedBy) {
		return false
	}

	if ann := snapshotData.Annotations[annDynamicallyCreatedBy]; ann != ctrl.snapshotterName {
		return false
	}

	return true
}
