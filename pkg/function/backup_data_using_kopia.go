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
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/pkg/errors"
	"k8s.io/client-go/kubernetes"

	kanister "github.com/kanisterio/kanister/pkg"
	"github.com/kanisterio/kanister/pkg/consts"
	"github.com/kanisterio/kanister/pkg/field"
	"github.com/kanisterio/kanister/pkg/format"
	"github.com/kanisterio/kanister/pkg/kopia"
	"github.com/kanisterio/kanister/pkg/kube"
	"github.com/kanisterio/kanister/pkg/param"
)

const (
	// HostNameOption is the key for passing in hostname through Options map
	HostNameOption = "hostName"
	// UserNameOption is the key for passing in username through Options map
	UserNameOption = "userName"
)

func init() {
	_ = kanister.RegisterVersion(&backupDataUsingKopiaFunc{}, kanister.KopiaVersion) // nolint: errcheck
}

var _ kanister.Func = (*backupDataUsingKopiaFunc)(nil)

type backupDataUsingKopiaFunc struct{}

func (*backupDataUsingKopiaFunc) Name() string {
	return BackupDataFuncName
}

func (*backupDataUsingKopiaFunc) RequiredArgs() []string {
	return []string{
		BackupDataNamespaceArg,
		BackupDataPodArg,
		BackupDataContainerArg,
		BackupDataIncludePathArg,
		BackupDataBackupArtifactPrefixArg,
		BackupDataEncryptionKeyArg,
	}
}

func (*backupDataUsingKopiaFunc) Arguments() []string {
	return []string{
		BackupDataNamespaceArg,
		BackupDataPodArg,
		BackupDataContainerArg,
		BackupDataIncludePathArg,
		BackupDataBackupArtifactPrefixArg,
		BackupDataEncryptionKeyArg,
	}
}

func (*backupDataUsingKopiaFunc) Exec(ctx context.Context, tp param.TemplateParams, args map[string]interface{}) (map[string]interface{}, error) {
	var namespace, pod, container, includePath, backupArtifactPrefix, encryptionKey string
	var err error
	if err = Arg(args, BackupDataNamespaceArg, &namespace); err != nil {
		return nil, err
	}
	if err = Arg(args, BackupDataPodArg, &pod); err != nil {
		return nil, err
	}
	if err = Arg(args, BackupDataContainerArg, &container); err != nil {
		return nil, err
	}
	if err = Arg(args, BackupDataIncludePathArg, &includePath); err != nil {
		return nil, err
	}
	if err = Arg(args, BackupDataBackupArtifactPrefixArg, &backupArtifactPrefix); err != nil {
		return nil, err
	}
	if err = Arg(args, BackupDataEncryptionKeyArg, &encryptionKey); err != nil {
		return nil, err
	}

	if err = ValidateProfile(tp.Profile); err != nil {
		return nil, errors.Wrap(err, "Failed to validate Profile")
	}

	hostname, username, err := getHostAndUserNameFromOptions(tp.Options)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to get hostname/username from Options")
	}

	cli, err := kube.NewClient()
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create Kubernetes client")
	}

	ctx = field.Context(ctx, consts.PodNameKey, pod)
	ctx = field.Context(ctx, consts.ContainerNameKey, container)
	snapInfo, err := backupDataUsingKopia(
		ctx,
		cli,
		namespace,
		pod,
		container,
		backupArtifactPrefix,
		includePath,
		encryptionKey,
		hostname,
		username,
		tp,
	)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to backup data")
	}

	var logSize, phySize int64
	if snapInfo.Stats != nil {
		stats := snapInfo.Stats
		logSize = stats.SizeHashedB + stats.SizeCachedB
		phySize = stats.SizeUploadedB
	}

	output := map[string]interface{}{
		BackupDataOutputBackupID:           snapInfo.SnapshotID,
		BackupDataOutputBackupSize:         humanize.Bytes(uint64(logSize)),
		BackupDataOutputBackupPhysicalSize: humanize.Bytes(uint64(phySize)),
		FunctionOutputVersion:              kanister.KopiaVersion,
	}
	return output, nil
}

func backupDataUsingKopia(
	ctx context.Context,
	cli kubernetes.Interface,
	namespace,
	pod,
	container,
	backupArtifactPrefix,
	includePath,
	encryptionKey,
	hostname,
	username string,
	tp param.TemplateParams,
) (info *kopia.SnapshotCreateInfo, err error) {
	pw, err := GetPodWriter(cli, ctx, namespace, pod, container, tp.Profile)
	if err != nil {
		return nil, err
	}
	defer CleanUpCredsFile(ctx, pw, namespace, pod, container)

	contentCacheMB, metadataCacheMB := kopia.GetCacheSizeSettingsForSnapshot()

	if err = kopia.ConnectToOrCreateKopiaRepository(
		cli,
		namespace,
		pod,
		container,
		backupArtifactPrefix,
		encryptionKey,
		hostname,
		username,
		kopia.DefaultCacheDirectory,
		kopia.DefaultConfigFilePath,
		kopia.DefaultLogDirectory,
		contentCacheMB,
		metadataCacheMB,
		&kopia.KanisterProfile{Profile: tp.Profile},
	); err != nil {
		return nil, err
	}

	cmd := kopia.SnapshotCreateCommand(encryptionKey, includePath, kopia.DefaultConfigFilePath, kopia.DefaultLogDirectory)

	stdout, stderr, err := kube.Exec(cli, namespace, pod, container, cmd, nil)
	format.Log(pod, container, stdout)
	format.Log(pod, container, stderr)

	message := "Failed to create and upload backup"
	if err != nil {
		if strings.Contains(err.Error(), kopia.ErrCodeOutOfMemory) {
			message = message + ": " + kopia.OutOfMemoryStr
		}
		return nil, errors.Wrap(err, message)
	}
	// Parse logs and return snapshot IDs and stats
	return kopia.ParseSnapshotCreateOutput(stdout, stderr)
}

func getHostAndUserNameFromOptions(options map[string]string) (string, string, error) {
	var hostname, username string
	var ok bool
	if hostname, ok = options[HostNameOption]; !ok {
		return hostname, username, errors.New("Failed to find hostname option")
	}
	if username, ok = options[UserNameOption]; !ok {
		return hostname, username, errors.New("Failed to find username option")
	}
	return hostname, username, nil
}
