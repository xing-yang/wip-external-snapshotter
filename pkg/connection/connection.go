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
	"context"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/container-storage-interface/spec/lib/go/csi/v0"
	"github.com/golang/glog"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/status"
	storage "k8s.io/api/storage/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/api/core/v1"
	ref "k8s.io/client-go/tools/reference"
	"k8s.io/client-go/kubernetes/scheme"
)

// CSIConnection is gRPC connection to a remote CSI driver and abstracts all
// CSI calls.
type CSIConnection interface {
	// GetDriverName returns driver name as discovered by GetPluginInfo()
	// gRPC call.
	GetDriverName(ctx context.Context) (string, error)

	// SupportsControllerCreateSnapshot returns true if the CSI driver reports
	// CREATE_DELETE_SNAPSHOT in ControllerGetCapabilities() gRPC call.
	SupportsControllerCreateSnapshot(ctx context.Context) (bool, error)

	// SupportsControllerListSnapshots returns true if the CSI driver reports
	// LIST_SNAPSHOTS in ControllerGetCapabilities() gRPC call.
	SupportsControllerListSnapshots(ctx context.Context) (bool, error)

	// CreateSnapshot creates a snapshot for a volume
	CreateSnapshot(ctx context.Context, snapshot *storage.VolumeSnapshot, volume *v1.PersistentVolume, parameters map[string]string) (*storage.VolumeSnapshotData, error)

	// DeleteSnapshot deletes a snapshot from a volume
	DeleteSnapshot(ctx context.Context, snapshotID string) (err error)

	// ListSnapshots lists snapshot from a volume
	ListSnapshots(ctx context.Context, snapshotID string) (*storage.VolumeSnapshotDataCondition, error)

	// Probe checks that the CSI driver is ready to process requests
	Probe(ctx context.Context) error

	// Close the connection
	Close() error
}

type csiConnection struct {
	conn *grpc.ClientConn
}

var (
	_ CSIConnection = &csiConnection{}
)

func New(address string, timeout time.Duration) (CSIConnection, error) {
	conn, err := connect(address, timeout)
	if err != nil {
		return nil, err
	}
	return &csiConnection{
		conn: conn,
	}, nil
}

func connect(address string, timeout time.Duration) (*grpc.ClientConn, error) {
	glog.V(2).Infof("Connecting to %s", address)
	dialOptions := []grpc.DialOption{
		grpc.WithInsecure(),
		grpc.WithBackoffMaxDelay(time.Second),
		grpc.WithUnaryInterceptor(logGRPC),
	}
	if strings.HasPrefix(address, "/") {
		dialOptions = append(dialOptions, grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("unix", addr, timeout)
		}))
	}
	conn, err := grpc.Dial(address, dialOptions...)

	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		if !conn.WaitForStateChange(ctx, conn.GetState()) {
			glog.V(4).Infof("Connection timed out")
			return conn, nil // return nil, subsequent GetPluginInfo will show the real connection error
		}
		if conn.GetState() == connectivity.Ready {
			glog.V(3).Infof("Connected")
			return conn, nil
		}
		glog.V(4).Infof("Still trying, connection is %s", conn.GetState())
	}
}

func (c *csiConnection) GetDriverName(ctx context.Context) (string, error) {
	client := csi.NewIdentityClient(c.conn)

	req := csi.GetPluginInfoRequest{}

	rsp, err := client.GetPluginInfo(ctx, &req)
	if err != nil {
		return "", err
	}
	name := rsp.GetName()
	if name == "" {
		return "", fmt.Errorf("name is empty")
	}
	return name, nil
}

func (c *csiConnection) Probe(ctx context.Context) error {
	client := csi.NewIdentityClient(c.conn)

	req := csi.ProbeRequest{}

	_, err := client.Probe(ctx, &req)
	if err != nil {
		return err
	}
	return nil
}

func (c *csiConnection) SupportsControllerCreateSnapshot(ctx context.Context) (bool, error) {
	client := csi.NewControllerClient(c.conn)
	req := csi.ControllerGetCapabilitiesRequest{}

	rsp, err := client.ControllerGetCapabilities(ctx, &req)
	if err != nil {
		return false, err
	}
	caps := rsp.GetCapabilities()
	for _, cap := range caps {
		if cap == nil {
			continue
		}
		rpc := cap.GetRpc()
		if rpc == nil {
			continue
		}
		if rpc.GetType() == csi.ControllerServiceCapability_RPC_CREATE_DELETE_SNAPSHOT {
			return true, nil
		}
	}
	return false, nil
}

func (c *csiConnection) SupportsControllerListSnapshots(ctx context.Context) (bool, error) {
	client := csi.NewControllerClient(c.conn)
	req := csi.ControllerGetCapabilitiesRequest{}

	rsp, err := client.ControllerGetCapabilities(ctx, &req)
	if err != nil {
		return false, err
	}
	caps := rsp.GetCapabilities()
	for _, cap := range caps {
		if cap == nil {
			continue
		}
		rpc := cap.GetRpc()
		if rpc == nil {
			continue
		}
		if rpc.GetType() == csi.ControllerServiceCapability_RPC_LIST_SNAPSHOTS {
			return true, nil
		}
	}
	return false, nil
}

func (c *csiConnection) CreateSnapshot(ctx context.Context, snapshot *storage.VolumeSnapshot, volume *v1.PersistentVolume, parameters map[string]string) (*storage.VolumeSnapshotData, error) {
	if volume.Spec.CSI == nil {
		return nil, fmt.Errorf("CSIPersistentVolumeSource not defined in spec")
	}

	client := csi.NewControllerClient(c.conn)

	driverName, err := c.GetDriverName(ctx)
	if err != nil {
		return nil, err
	}

	req := csi.CreateSnapshotRequest{
		SourceVolumeId:        volume.Spec.CSI.VolumeHandle,
		Name:                  snapshot.Name,
		Parameters:            parameters,
		CreateSnapshotSecrets: nil,
	}

	rsp, err := client.CreateSnapshot(ctx, &req)
	if err != nil {
		return nil, err
	}

	snapDataName := GetSnapshotDataNameForSnapshot(snapshot)
	volumeSnapshotRef, err := ref.GetReference(scheme.Scheme, snapshot)
	if err != nil {
		return nil, fmt.Errorf("unexpected error getting snapshot reference: %v", err)
	}

	persistentVolumeRef, err := ref.GetReference(scheme.Scheme, volume)
	if err != nil {
		return nil, fmt.Errorf("unexpected error getting volume reference: %v", err)
	}

	// Create VolumeSnapshot in the database
	snapshotData := &storage.VolumeSnapshotData{
		ObjectMeta: metav1.ObjectMeta{
			Name: snapDataName,
		},
		Spec: storage.VolumeSnapshotDataSpec{
			VolumeSnapshotRef:   volumeSnapshotRef,
			PersistentVolumeRef: persistentVolumeRef,
			VolumeSnapshotDataSource: storage.VolumeSnapshotDataSource{
				CSISnapshot: &storage.CSIVolumeSnapshotSource{
					Driver:         driverName,
					SnapshotHandle: rsp.Snapshot.Id,
					CreatedAt:      rsp.Snapshot.CreatedAt,
				},
			},
		},
		Status: storage.VolumeSnapshotDataStatus{
			Conditions: []storage.VolumeSnapshotDataCondition{
				ConvertSnapshotStatus(rsp.Snapshot.Status),
			},
		},
	}

	return snapshotData, nil
}

func (c *csiConnection) DeleteSnapshot(ctx context.Context, snapshotID string) (err error) {
	client := csi.NewControllerClient(c.conn)

	req := csi.DeleteSnapshotRequest{
		SnapshotId:            snapshotID,
		DeleteSnapshotSecrets: nil,
	}

	if _, err := client.DeleteSnapshot(ctx, &req); err != nil {
		return err
	}

	return nil
}

func (c *csiConnection) ListSnapshots(ctx context.Context, snapshotID string) (*storage.VolumeSnapshotDataCondition, error) {
	client := csi.NewControllerClient(c.conn)

	req := csi.ListSnapshotsRequest{
		SnapshotId: snapshotID,
	}

	rsp, err := client.ListSnapshots(ctx, &req)
	if err != nil {
		return nil, err
	}

	if rsp.Entries == nil || len(rsp.Entries) == 0 {
		return nil, fmt.Errorf("can not find snapshot for snapshotID %s", snapshotID)
	}

	snapDataCon := ConvertSnapshotStatus(rsp.Entries[0].Snapshot.Status)
	return &snapDataCon, nil
}

func (c *csiConnection) Close() error {
	return c.conn.Close()
}

func logGRPC(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	glog.V(5).Infof("GRPC call: %s", method)
	glog.V(5).Infof("GRPC request: %+v", req)
	err := invoker(ctx, method, req, reply, cc, opts...)
	glog.V(5).Infof("GRPC response: %+v", reply)
	glog.V(5).Infof("GRPC error: %v", err)
	return err
}

// isFinished returns true if given error represents final error of an
// operation. That means the operation has failed completelly and cannot be in
// progress.  It returns false, if the error represents some transient error
// like timeout and the operation itself or previous call to the same
// operation can be actually in progress.
func isFinalError(err error) bool {
	// Sources:
	// https://github.com/grpc/grpc/blob/master/doc/statuscodes.md
	// https://github.com/container-storage-interface/spec/blob/master/spec.md
	st, ok := status.FromError(err)
	if !ok {
		// This is not gRPC error. The operation must have failed before gRPC
		// method was called, otherwise we would get gRPC error.
		return true
	}
	switch st.Code() {
	case codes.Canceled,          // gRPC: Client Application cancelled the request
		codes.DeadlineExceeded,   // gRPC: Timeout
		codes.Unavailable,        // gRPC: Server shutting down, TCP connection broken - previous Attach() or Detach() may be still in progress.
		codes.ResourceExhausted,  // gRPC: Server temporarily out of resources - previous Attach() or Detach() may be still in progress.
		codes.FailedPrecondition: // CSI: Operation pending for volume
		return false
	}
	// All other errors mean that the operation (create/delete snapshot) either did not
	// even start or failed. It is for sure not in progress.
	return true
}
