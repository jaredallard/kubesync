// Copyright (C) 2026 kubesync contributors
//
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Lesser General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Lesser General Public License for more details.
//
// You should have received a copy of the GNU Lesser General Public
// License along with this program. If not, see
// <https://www.gnu.org/licenses/>.
//
// SPDX-License-Identifier: LGPL-3.0

// Package kubesync implements a distributed mutex using Kubernetes
// Leases as the backing store.
package kubesync

import (
	"fmt"

	"k8s.io/apimachinery/pkg/util/uuid"
	"k8s.io/client-go/kubernetes"
)

// Syncer is an instance of kubesync that uses the provided Kubernetes
// client to implement distributed synchronization primitives.
//
// Attempts to match the equivalent API of the sync package in the
// standard library as closely as possible.
type Syncer struct {
	// k is the Kubernetes client to use.
	k kubernetes.Interface

	// namespace is the namespace that leases should be created in. Allows
	// for multiple distinct syncers to be created in the same cluster by
	// using different namespaces.
	namespace string

	// uniqueID should be a unique identifier for the current instance
	// of this application.
	uniqueID string
}

// New creates a new Syncer instance using the provided Kubernetes
// client (k) and namespace (namespace). instanceID should be a unique
// identifier for the current instance of this application. Generally it
// should be the hostname of the machine.
func New(k kubernetes.Interface, namespace, instanceID string) *Syncer {
	return &Syncer{
		k:         k,
		namespace: namespace,
		uniqueID:  fmt.Sprintf("%s-%s", instanceID, uuid.NewUUID()),
	}
}
