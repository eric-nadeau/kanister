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

package xgo

import (
	apiconfig "github.com/kanisterio/kanister/pkg/apis/config/v1alpha1"
)

// SecretP is a pointer to a Secret
type SecretP *Secret

// Secret Contains secret material such as cloud credentials
type Secret struct {

	// api
	API *SecretAPI `json:"api,omitempty"`

	// aws
	Aws *SecretAws `json:"aws,omitempty"`

	// azure
	Azure *SecretAzure `json:"azure,omitempty"`

	// ceph
	Ceph *SecretCeph `json:"ceph,omitempty"`

	// gcp
	Gcp *SecretGcp `json:"gcp,omitempty"`

	// open stack
	OpenStack *SecretOpenStack `json:"openStack,omitempty"`

	// portworx
	Portworx *SecretPortworx `json:"portworx,omitempty"`

	// type
	Type apiconfig.SecretType `json:"type,omitempty"`

	// vbr
	Vbr *SecretVbr `json:"vbr,omitempty"`

	// vsphere
	Vsphere *SecretVsphere `json:"vsphere,omitempty"`
}

// SecretAPI API key
type SecretAPI struct {
	// key
	Key string `json:"key,omitempty"`
}

// SecretAws AWS access keys
type SecretAws struct {
	// access key Id
	AccessKeyID string `json:"accessKeyId,omitempty"`

	// Optional role to create temp credentials
	Role string `json:"role,omitempty"`

	// secret access key
	SecretAccessKey string `json:"secretAccessKey,omitempty"`
}

// SecretAzure Azure storage account keys
type SecretAzure struct {
	// storage account
	StorageAccount string `json:"storageAccount,omitempty"`

	// storage key
	StorageKey string `json:"storageKey,omitempty"`

	// storage key
	StorageEnv string `json:"storageEnv,omitempty"`
}

// SecretCeph Ceph credentials
type SecretCeph struct {
	// Base64 encoding of keyring
	Keyring string `json:"keyring,omitempty"`

	// Ceph user id
	User string `json:"user,omitempty"`
}

// SecretGcp GCP service account key and project ID
type SecretGcp struct {
	// project Id
	ProjectID string `json:"projectId,omitempty"`

	// base64 encoded service account key
	ServiceKey string `json:"serviceKey,omitempty"`
}

// SecretOpenStack OpenStack credentials
type SecretOpenStack struct {
	// domain
	Domain string `json:"domain,omitempty"`

	// password
	Password string `json:"password,omitempty"`

	// project
	Project string `json:"project,omitempty"`

	// region
	Region string `json:"region,omitempty"`

	// user
	User string `json:"user,omitempty"`
}

// SecretPortworx Portworx credentials
type SecretPortworx struct {
	// Portworx issuer
	Issuer string `json:"issuer,omitempty"`

	// Portworx secret
	Secret string `json:"secret,omitempty"`
}

// SecretVbr VBR credentials
type SecretVbr struct {
	// VBR password
	Password string `json:"password,omitempty"`

	// VBR user
	User string `json:"user,omitempty"`
}

// SecretVsphere vSphere credentials
type SecretVsphere struct {
	// vSphere password
	Password string `json:"password,omitempty"`

	// vSphere user
	User string `json:"user,omitempty"`
}
