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
	"k8s.io/api/core/v1"
	storage "k8s.io/api/storage/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

func vsToVsKey(vs *storage.VolumeSnapshot) string {
	return fmt.Sprintf("%s/%s", vs.Namespace, vs.Name)
}

func vsrefToVsKey(vsref *v1.ObjectReference) string {
	return fmt.Sprintf("%s/%s", vsref.Namespace, vsref.Name)
}

func GetSnapshotDataNameForSnapshot(snapshot *storage.VolumeSnapshot) string {
	return "vsd-" + string(snapshot.UID)
}

// IsSnapshotDataReady returns true if a volumeSnapshotData is ready; false otherwise.
func IsSnapshotDataReady(volumeSnapshotData *storage.VolumeSnapshotData) bool {
	return IsSnapshotDataReadyConditionTrue(volumeSnapshotData.Status)
}

// IsSnapshotDataReadyConditionTrue returns true if a VolumeSnapshot is ready; false otherwise.
func IsSnapshotDataReadyConditionTrue(status storage.VolumeSnapshotDataStatus) bool {
	condition := GetSnapshotDataReadyCondition(status)
	return condition != nil && condition.Status == v1.ConditionTrue
}

// GetSnapshotReadyCondition extracts the VolumeSnapshot ready condition from the given status and returns that.
// Returns nil if the condition is not present.
func GetSnapshotDataReadyCondition(status storage.VolumeSnapshotDataStatus) *storage.VolumeSnapshotDataCondition {
	_, condition := GetSnapshotDataCondition(&status, storage.VolumeSnapshotDataConditionReady)
	return condition
}

// GetSnapshotCondition extracts the provided condition from the given status and returns that.
// Returns nil and -1 if the condition is not present, and the index of the located condition.
func GetSnapshotDataCondition(status *storage.VolumeSnapshotDataStatus, conditionType storage.VolumeSnapshotDataConditionType) (int, *storage.VolumeSnapshotDataCondition) {
	if status == nil {
		return -1, nil
	}
	for i := range status.Conditions {
		if status.Conditions[i].Type == conditionType {
			return i, &status.Conditions[i]
		}
	}
	return -1, nil
}

// UpdateSnapshotDataCondition updates existing Snapshot data condition or creates a new one. Sets LastTransitionTime to now if the
// status has changed.
// Returns true if Snapshot data condition has changed or has been added.
func UpdateSnapshotDataCondition(status *storage.VolumeSnapshotDataStatus, condition *storage.VolumeSnapshotDataCondition) bool {
	condition.LastTransitionTime = metav1.Now()
	// Try to find this pod condition.
	conditionIndex, oldCondition := GetSnapshotDataCondition(status, condition.Type)

	if oldCondition == nil {
		// We are adding new pod condition.
		status.Conditions = append(status.Conditions, *condition)
		return true
	}
	// We are updating an existing condition, so we need to check if it has changed.
	if condition.Status == oldCondition.Status {
		condition.LastTransitionTime = oldCondition.LastTransitionTime
	}

	isEqual := condition.Status == oldCondition.Status &&
		condition.Reason == oldCondition.Reason &&
		condition.Message == oldCondition.Message &&
		condition.LastTransitionTime.Equal(&oldCondition.LastTransitionTime)

	status.Conditions[conditionIndex] = *condition
	// Return true if one of the fields have changed.
	return !isEqual
}
