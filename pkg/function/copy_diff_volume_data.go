// Copyright 2022 The Kanister Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package function

import (
	"context"
	"fmt"

	differentialsnapshotv1alpha1 "github.com/phuongatemc/diffsnapcontroller/pkg/apis/differentialsnapshot/v1alpha1"
	"github.com/pkg/errors"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/runtime/serializer"
	"k8s.io/apimachinery/pkg/util/rand"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/kubernetes"

	kanister "github.com/kanisterio/kanister/pkg"
	"github.com/kanisterio/kanister/pkg/format"
	"github.com/kanisterio/kanister/pkg/kube"
	"github.com/kanisterio/kanister/pkg/log"
	"github.com/kanisterio/kanister/pkg/param"
	"github.com/kanisterio/kanister/pkg/poll"
	"github.com/kanisterio/kanister/pkg/restic"
)

const (
	// CopyDiffVolumeDataFuncName gives the function name
	CopyDiffVolumeDataFuncName                     = "CopyDiffVolumeData"
	CopyDiffVolumeDataMountPoint                   = "/dev/%s"
	CopyDiffVolumeDataJobPrefix                    = "copy-vol-data-"
	CopyDiffVolumeDataNamespaceArg                 = "namespace"
	CopyDiffVolumeDataVolumeArg                    = "volume"
	CopyDiffVolumeDataSnapshotBaseArg              = "snapshotBase"
	CopyDiffVolumeDataSnapshotTargetArg            = "snapshotTarget"
	CopyDiffVolumeDataArtifactPrefixArg            = "dataArtifactPrefix"
	CopyDiffVolumeDataOutputBackupID               = "backupID"
	CopyDiffVolumeDataOutputBackupRoot             = "backupRoot"
	CopyDiffVolumeDataOutputBackupArtifactLocation = "backupArtifactLocation"
	CopyDiffVolumeDataEncryptionKeyArg             = "encryptionKey"
	CopyDiffVolumeDataOutputBackupTag              = "backupTag"
	CopyDiffVolumeDataPodOverrideArg               = "podOverride"
	CopyDiffVolumeDataOutputBackupFileCount        = "fileCount"
	CopyDiffVolumeDataOutputBackupSize             = "size"
	CopyDiffVolumeDataOutputPhysicalSize           = "phySize"
)

func init() {
	_ = kanister.Register(&copyDiffVolumeDataFunc{})
}

var _ kanister.Func = (*copyDiffVolumeDataFunc)(nil)

type copyDiffVolumeDataFunc struct{}

var gvrDiffSnapCB = schema.GroupVersionResource{Group: "differentialsnapshot.example.com", Version: "v1alpha1", Resource: "getchangedblockses"}

func (*copyDiffVolumeDataFunc) Name() string {
	return CopyDiffVolumeDataFuncName
}

func copyDiffVolumeData(ctx context.Context, cli kubernetes.Interface, dynCli dynamic.Interface, tp param.TemplateParams, namespace, pvc, targetPath, encryptionKey string, podOverride map[string]interface{}, snapBase, snapTarget string) (map[string]interface{}, error) {
	// Validate PVC exists
	if _, err := cli.CoreV1().PersistentVolumeClaims(namespace).Get(ctx, pvc, metav1.GetOptions{}); err != nil {
		return nil, errors.Wrapf(err, "Failed to retrieve PVC. Namespace %s, Name %s", namespace, pvc)
	}

	// Find the changed blocks
	cbList, err := getChangedBlocks(ctx, dynCli, namespace, snapBase, snapTarget)
	if err != nil {
		return nil, err
	}

	// Create a pod with PVCs attached
	mountPoint := fmt.Sprintf(CopyDiffVolumeDataMountPoint, pvc)
	options := &kube.PodOptions{
		Namespace:    namespace,
		GenerateName: CopyDiffVolumeDataJobPrefix,
		Image:        getKanisterToolsImage(),
		Command:      []string{"sh", "-c", "tail -f /dev/null"},
		Volumes:      map[string]string{pvc: mountPoint},
		PodOverride:  podOverride,
		BlockDevice:  true,
	}
	pr := kube.NewPodRunner(cli, options)
	podFunc := copyDiffVolumeDataPodFunc(cli, tp, namespace, mountPoint, targetPath, encryptionKey, cbList)
	return pr.Run(ctx, podFunc)
}

func copyDiffVolumeDataPodFunc(cli kubernetes.Interface, tp param.TemplateParams, namespace, mountPoint, targetPath, encryptionKey string, cbList []differentialsnapshotv1alpha1.ChangedBlock) func(ctx context.Context, pod *v1.Pod) (map[string]interface{}, error) {
	return func(ctx context.Context, pod *v1.Pod) (map[string]interface{}, error) {
		// Wait for pod to reach running state
		if err := kube.WaitForPodReady(ctx, cli, pod.Namespace, pod.Name); err != nil {
			return nil, errors.Wrapf(err, "Failed while waiting for Pod %s to be ready", pod.Name)
		}
		pw, err := GetPodWriter(cli, ctx, pod.Namespace, pod.Name, pod.Spec.Containers[0].Name, tp.Profile)
		if err != nil {
			return nil, err
		}
		defer CleanUpCredsFile(ctx, pw, pod.Namespace, pod.Name, pod.Spec.Containers[0].Name)

		cbFile := "/tmp/cblocks"
		for _, cb := range cbList {
			// Create tmp block file with changed blocks
			// Find and append changed blocks to the file
			// TODO: Store metadata of CB along with snapshot for restore
			cmd := shCommand(fmt.Sprintf("dd if=%s bs=%d count=1 skip=%d oflag=append conv=notrunc of=%s", mountPoint, cb.Size, cb.Offset/cb.Size, cbFile))
			log.Print("CB CMD:", cmd)
			stdout, stderr, err := kube.Exec(cli, namespace, pod.Name, pod.Spec.Containers[0].Name, cmd, nil)
			format.LogWithCtx(ctx, pod.Name, pod.Spec.Containers[0].Name, stdout)
			format.LogWithCtx(ctx, pod.Name, pod.Spec.Containers[0].Name, stderr)
			if err != nil {
				return nil, errors.Wrapf(err, "Failed to create and upload backup")
			}
		}

		// Get restic repository
		if err := restic.GetOrCreateRepository(cli, namespace, pod.Name, pod.Spec.Containers[0].Name, targetPath, encryptionKey, tp.Profile); err != nil {
			return nil, err
		}
		// Copy data to object store
		backupTag := rand.String(10)
		cmd, err := restic.BackupCommandByTag(tp.Profile, targetPath, backupTag, cbFile, encryptionKey)
		if err != nil {
			return nil, err
		}

		stdout, stderr, err := kube.Exec(cli, namespace, pod.Name, pod.Spec.Containers[0].Name, cmd, nil)
		format.LogWithCtx(ctx, pod.Name, pod.Spec.Containers[0].Name, stdout)
		format.LogWithCtx(ctx, pod.Name, pod.Spec.Containers[0].Name, stderr)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to create and upload backup")
		}
		// Get the snapshot ID from log
		backupID := restic.SnapshotIDFromBackupLog(stdout)
		if backupID == "" {
			return nil, errors.Errorf("Failed to parse the backup ID from logs, backup logs %s", stdout)
		}
		fileCount, backupSize, phySize := restic.SnapshotStatsFromBackupLog(stdout)
		if backupSize == "" {
			log.Debug().Print("Could not parse backup stats from backup log")
		}

		return map[string]interface{}{
				CopyDiffVolumeDataOutputBackupID:               backupID,
				CopyDiffVolumeDataOutputBackupRoot:             mountPoint,
				CopyDiffVolumeDataOutputBackupArtifactLocation: targetPath,
				CopyDiffVolumeDataOutputBackupTag:              backupTag,
				CopyDiffVolumeDataOutputBackupFileCount:        fileCount,
				CopyDiffVolumeDataOutputBackupSize:             backupSize,
				CopyDiffVolumeDataOutputPhysicalSize:           phySize,
				FunctionOutputVersion:                          kanister.DefaultVersion,
			},
			nil
	}
}

func (*copyDiffVolumeDataFunc) Exec(ctx context.Context, tp param.TemplateParams, args map[string]interface{}) (map[string]interface{}, error) {
	var namespace, vol, targetPath, encryptionKey, snapBase, snapTarget string
	var err error
	if err = Arg(args, CopyDiffVolumeDataNamespaceArg, &namespace); err != nil {
		return nil, err
	}
	if err = Arg(args, CopyDiffVolumeDataVolumeArg, &vol); err != nil {
		return nil, err
	}
	if err = Arg(args, CopyDiffVolumeDataArtifactPrefixArg, &targetPath); err != nil {
		return nil, err
	}
	if err = OptArg(args, CopyDiffVolumeDataEncryptionKeyArg, &encryptionKey, restic.GeneratePassword()); err != nil {
		return nil, err
	}
	if err = Arg(args, CopyDiffVolumeDataSnapshotBaseArg, &snapBase); err != nil {
		return nil, err
	}
	if err = Arg(args, CopyDiffVolumeDataSnapshotTargetArg, &snapTarget); err != nil {
		return nil, err
	}
	podOverride, err := GetPodSpecOverride(tp, args, CopyDiffVolumeDataPodOverrideArg)
	if err != nil {
		return nil, err
	}

	if err = ValidateProfile(tp.Profile); err != nil {
		return nil, errors.Wrapf(err, "Failed to validate Profile")
	}

	targetPath = ResolveArtifactPrefix(targetPath, tp.Profile)

	cli, err := kube.NewClient()
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to create Kubernetes client")
	}
	dynCli, err := kube.NewDynamicClient()
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to create Kubernetes client")
	}
	return copyDiffVolumeData(ctx, cli, dynCli, tp, namespace, vol, targetPath, encryptionKey, podOverride, snapBase, snapTarget)
}

func (*copyDiffVolumeDataFunc) RequiredArgs() []string {
	return []string{
		CopyDiffVolumeDataNamespaceArg,
		CopyDiffVolumeDataVolumeArg,
		CopyDiffVolumeDataArtifactPrefixArg,
		CopyDiffVolumeDataSnapshotBaseArg,
		CopyDiffVolumeDataSnapshotTargetArg,
	}
}

func (*copyDiffVolumeDataFunc) Arguments() []string {
	return []string{
		CopyDiffVolumeDataNamespaceArg,
		CopyDiffVolumeDataVolumeArg,
		CopyDiffVolumeDataArtifactPrefixArg,
		CopyDiffVolumeDataEncryptionKeyArg,
		CopyDiffVolumeDataSnapshotBaseArg,
		CopyDiffVolumeDataSnapshotTargetArg,
	}
}

func getChangedBlocks(ctx context.Context, dynCli dynamic.Interface, namespace, snapBase, snapTarget string) ([]differentialsnapshotv1alpha1.ChangedBlock, error) {
	// Create resource object
	object := &unstructured.Unstructured{
		Object: map[string]interface{}{
			"apiVersion": "differentialsnapshot.example.com/v1alpha1",
			"kind":       "GetChangedBlocks",
			"metadata": map[string]interface{}{
				"generateName": "test-",
				"namespace":    namespace,
			},
			"spec": map[string]interface{}{
				"maxEntries":     10000,
				"snapshotBase":   snapBase,
				"snapshotTarget": snapTarget,
			},
		},
	}
	cb, err := dynCli.Resource(gvrDiffSnapCB).Namespace(namespace).Create(ctx, object, metav1.CreateOptions{})
	if err != nil {
		return nil, err
	}

	if err := poll.Wait(ctx, func(context.Context) (done bool, err error) {
		cbSpec, err := getCBObject(ctx, dynCli, cb.GetName(), namespace)
		if err != nil {
			return false, err
		}
		return cbSpec.Status.State == "Success", nil
	}); err != nil {
		return nil, err
	}
	cbSpec, err := getCBObject(ctx, dynCli, cb.GetName(), namespace)
	if err != nil {
		return nil, err
	}
	return cbSpec.Status.ChangeBlockList, nil
}

func getCBObject(ctx context.Context, dynCli dynamic.Interface, name, namespace string) (*differentialsnapshotv1alpha1.GetChangedBlocks, error) {
	cbObject, err := dynCli.Resource(gvrDiffSnapCB).Namespace(namespace).Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}
	getCbSpec := &differentialsnapshotv1alpha1.GetChangedBlocks{}
	cbObjectBytes, err := cbObject.MarshalJSON()
	if err != nil {
		return nil, err
	}
	d := serializer.NewCodecFactory(runtime.NewScheme()).UniversalDeserializer()
	if _, _, err := d.Decode(cbObjectBytes, nil, getCbSpec); err != nil {
		return nil, err
	}
	return getCbSpec, nil
}

func shCommand(command string) []string {
	return []string{"bash", "-o", "errexit", "-o", "pipefail", "-c", command}
}
