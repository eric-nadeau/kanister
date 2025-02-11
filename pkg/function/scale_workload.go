// Copyright 2019 The Kanister Authors.
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
	"strconv"
	"strings"

	osversioned "github.com/openshift/client-go/apps/clientset/versioned"
	"github.com/pkg/errors"
	"k8s.io/client-go/kubernetes"

	kanister "github.com/kanisterio/kanister/pkg"
	"github.com/kanisterio/kanister/pkg/kube"
	"github.com/kanisterio/kanister/pkg/param"
)

const (
	// ScaleWorkloadFuncName gives the function name
	ScaleWorkloadFuncName     = "ScaleWorkload"
	ScaleWorkloadNamespaceArg = "namespace"
	ScaleWorkloadNameArg      = "name"
	ScaleWorkloadKindArg      = "kind"
	ScaleWorkloadReplicas     = "replicas"
	ScaleWorkloadWaitArg      = "waitForReady"
)

func init() {
	_ = kanister.Register(&scaleWorkloadFunc{})
}

var (
	_ kanister.Func = (*scaleWorkloadFunc)(nil)
)

type scaleWorkloadFunc struct{}

func (*scaleWorkloadFunc) Name() string {
	return ScaleWorkloadFuncName
}

func (*scaleWorkloadFunc) Exec(ctx context.Context, tp param.TemplateParams, args map[string]interface{}) (map[string]interface{}, error) {
	var namespace, kind, name string
	var replicas int32
	namespace, kind, name, replicas, waitForReady, err := getArgs(tp, args)
	if err != nil {
		return nil, err
	}

	cfg, err := kube.LoadConfig()
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to load Kubernetes config")
	}
	cli, err := kubernetes.NewForConfig(cfg)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to create Kubernetes client")
	}
	switch strings.ToLower(kind) {
	case param.StatefulSetKind:
		return nil, kube.ScaleStatefulSet(ctx, cli, namespace, name, replicas, waitForReady)
	case param.DeploymentKind:
		return nil, kube.ScaleDeployment(ctx, cli, namespace, name, replicas, waitForReady)
	case param.DeploymentConfigKind:
		osCli, err := osversioned.NewForConfig(cfg)
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to create OpenShift client")
		}
		return nil, kube.ScaleDeploymentConfig(ctx, cli, osCli, namespace, name, replicas, waitForReady)
	}
	return nil, errors.New("Workload type not supported " + kind)
}

func (*scaleWorkloadFunc) RequiredArgs() []string {
	return []string{ScaleWorkloadReplicas}
}

func (*scaleWorkloadFunc) Arguments() []string {
	return []string{
		ScaleWorkloadReplicas,
		ScaleWorkloadNamespaceArg,
		ScaleWorkloadNameArg,
		ScaleWorkloadKindArg,
		ScaleWorkloadWaitArg,
	}
}

func getArgs(tp param.TemplateParams, args map[string]interface{}) (namespace, kind, name string, replicas int32, waitForReady bool, err error) {
	var rep interface{}
	waitForReady = true
	err = Arg(args, ScaleWorkloadReplicas, &rep)
	if err != nil {
		return
	}

	switch val := rep.(type) {
	case int:
		replicas = int32(val)
	case int32:
		replicas = val
	case int64:
		replicas = int32(val)
	case string:
		var v int
		if v, err = strconv.Atoi(val); err != nil {
			err = errors.Wrapf(err, "Cannot convert %s to int ", val)
			return
		}
		replicas = int32(v)
	default:
		err = errors.Errorf("Invalid arg type %T for Arg %s ", rep, ScaleWorkloadReplicas)
		return
	}
	// Populate default values for optional arguments from template parameters
	switch {
	case tp.StatefulSet != nil:
		kind = param.StatefulSetKind
		name = tp.StatefulSet.Name
		namespace = tp.StatefulSet.Namespace
	case tp.Deployment != nil:
		kind = param.DeploymentKind
		name = tp.Deployment.Name
		namespace = tp.Deployment.Namespace
	case tp.DeploymentConfig != nil:
		kind = param.DeploymentConfigKind
		name = tp.DeploymentConfig.Name
		namespace = tp.DeploymentConfig.Namespace
	default:
		if !ArgExists(args, ScaleWorkloadNamespaceArg) || !ArgExists(args, ScaleWorkloadNameArg) || !ArgExists(args, ScaleWorkloadKindArg) {
			return namespace, kind, name, replicas, waitForReady, errors.New("Workload information not available via defaults or namespace/name/kind parameters")
		}
	}

	err = OptArg(args, ScaleWorkloadNamespaceArg, &namespace, namespace)
	if err != nil {
		return
	}
	err = OptArg(args, ScaleWorkloadNameArg, &name, name)
	if err != nil {
		return
	}
	err = OptArg(args, ScaleWorkloadKindArg, &kind, kind)
	if err != nil {
		return
	}
	err = OptArg(args, ScaleWorkloadWaitArg, &waitForReady, waitForReady)
	if err != nil {
		return
	}
	return
}
