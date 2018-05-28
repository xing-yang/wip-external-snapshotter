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

package connection

import (
	"regexp"

	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

var (
	snapshotDataNamePrefix = "k8s-volume-snapshot"
)

func SanitizeDriverName(driver string) string {
	re := regexp.MustCompile("[^a-zA-Z0-9-]")
	name := re.ReplaceAllString(driver, "-")
	if name[len(name)-1] == '-' {
		// name must not end with '-'
		name = name + "X"
	}
	return name
}

// getFinalizerName returns Snapshotter name suitable to be used as finalizer
func GetFinalizerName(driver string) string {
	return "external-snapshotter/" + SanitizeDriverName(driver)
}

// ConvertSnapshotStatus converts snapshot status to storage.VolumeSnapshotDataCondition
func ConvertSnapshotStatus(status *csi.SnapshotStatus) storage.VolumeSnapshotDataCondition {
	var snapDataCondition storage.VolumeSnapshotDataCondition

	switch status.Type {
	case csi.SnapshotStatus_READY:
		snapDataCondition = storage.VolumeSnapshotDataCondition{
			Type:               storage.VolumeSnapshotDataConditionReady,
			Status:             v1.ConditionTrue,
			Message:            status.Details,
			LastTransitionTime: metav1.Now(),
		}
	case csi.SnapshotStatus_ERROR_UPLOADING:
		snapDataCondition = storage.VolumeSnapshotDataCondition{
			Type:               storage.VolumeSnapshotDataConditionError,
			Status:             v1.ConditionTrue,
			Message:            status.Details,
			LastTransitionTime: metav1.Now(),
		}
	case csi.SnapshotStatus_UPLOADING:
		snapDataCondition = storage.VolumeSnapshotDataCondition{
			Type:               storage.VolumeSnapshotDataConditionPending,
			Status:             v1.ConditionTrue,
			Message:            status.Details,
			LastTransitionTime: metav1.Now(),
		}
	case csi.SnapshotStatus_UNKNOWN:
		snapDataCondition = storage.VolumeSnapshotDataCondition{
			Type:               storage.VolumeSnapshotDataConditionPending,
			Status:             v1.ConditionUnknown,
			Message:            status.Details,
			LastTransitionTime: metav1.Now(),
		}
	}

	return snapDataCondition
}

// getSnapshotDataNameForSnapshot returns SnapshotData.Name for the create VolumeSnapshotData.
// The name must be unique.
func GetSnapshotDataNameForSnapshot(snapshot *storage.VolumeSnapshot) string {
	return "pvc-" + string(snapshot.UID)
}
