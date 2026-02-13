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

package kubesync

import (
	"context"
	"fmt"
	"math"
	"time"

	coordinationv1 "k8s.io/api/coordination/v1"
	kerrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	coordinationclientv1 "k8s.io/client-go/kubernetes/typed/coordination/v1"
	"k8s.io/utils/ptr"
)

// Contains various errors that can be returned by Mutex methods.
var (
	// ErrNotLocked is returned whenever a Mutex is attempted to be
	// unlocked or extended when it is not locked by anything. This is
	// also returned if the lease backed by this lock does not exist.
	ErrNotLocked = fmt.Errorf("not locked")

	// ErrLockedByAnother is returned whenever a Mutex is attempted to be
	// unlocked or extended when it is locked by another instance.
	ErrLockedByAnother = fmt.Errorf("locked by another instance")
)

// Mutex is a distributed mutex using Kubernetes Leases as the backing
// store.
type Mutex struct {
	// lease is the Kubernetes lease client.
	lease coordinationclientv1.LeaseInterface

	// name is the name of the mutex. Used as a primary key for the
	// lease.
	name string

	// namespace is the namespace to create leases in. Must match the
	// namespace used to create the lease client.
	namespace string

	// uniqueID should be a unique identifier for the current instance
	// of this application.
	uniqueID string

	// retryDur is the duration to wait between retries when attempting to
	// lock a resource that is already locked.
	retryDur time.Duration

	// expiration is the amount of time a lock should be held for. Once
	// this time is reached, the lock will be able to be acquired by
	// another instance.
	//
	// Defaults to 10 seconds.
	expiration time.Duration
}

// NewMutex creates a new Mutex. Uses the provided Kubernetes lease
// client (k) to create leases in the provided namespace (namespace).
// The instanceID is the unique ID for the current instance. Generally
// should be the value of the hostname.
//
// instanceID should be a unique identifier for the current instance of
// this application. Generally it should be the hostname of the machine.
func (s *Syncer) NewMutex(name string) *Mutex {
	return &Mutex{
		lease:      s.k.CoordinationV1().Leases(s.namespace),
		name:       name,
		namespace:  s.namespace,
		retryDur:   250 * time.Millisecond,
		uniqueID:   s.uniqueID,
		expiration: 10 * time.Second,
	}
}

// Lock creates a lock using a Kubernetes Lease based off of the Mutex
// that it was created from. The provided context is used to control
// the lock acquisition. If the lock is unable to be acquired, the
// function will block until the lock is acquired or the context is
// canceled.
//
// If the provided context is canceled, ctx.Err() will be returned from
// this function.
//
// [^1]: https://kubernetes.io/docs/concepts/architecture/leases
func (m *Mutex) Lock(ctx context.Context) error {
	return m.lock(ctx)
}

// lock creates a lock using a Kubernetes Lease. If the provided context
// is canceled, ctx.Err() will be returned.
func (m *Mutex) lock(ctx context.Context) error {
	// Attempt to create the lease, if not found. If the lease already
	// exists, this will no-op.
	if _, err := m.lease.Create(ctx, &coordinationv1.Lease{
		ObjectMeta: metav1.ObjectMeta{
			Name:      m.name,
			Namespace: m.namespace,
			Labels: map[string]string{
				"kubesync.jaredallard.github.com/lock": "true",
			},
		},
		Spec: coordinationv1.LeaseSpec{},
	}, metav1.CreateOptions{}); err != nil {
		if !kerrors.IsAlreadyExists(err) {
			return fmt.Errorf("failed to create lease '%s/%s': %w", m.namespace, m.name, err)
		}
	}

	// Wait until we've acquired the lease or the context is canceled.
	for ctx.Err() == nil {
		lease, err := m.lease.Get(ctx, m.name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get lease '%s/%s': %w", m.namespace, m.name, err)
		}

		// Handle the lease already being locked.
		if lease.Spec.HolderIdentity != nil {
			// Handle no duration (no expiration) or the acquire time being
			// unknown.
			if lease.Spec.LeaseDurationSeconds == nil || lease.Spec.AcquireTime == nil {
				return fmt.Errorf("lock: lease has no duration or acquisition time, refusing to acquire")
			}

			// Calculate when the lease expires.
			leaseDur := time.Duration(*lease.Spec.LeaseDurationSeconds) * time.Second

			// use the renew time if it exists, otherwise use the acquire time.
			lastUpdatedTime := lease.Spec.AcquireTime
			if lease.Spec.RenewTime != nil {
				lastUpdatedTime = lease.Spec.RenewTime
			}

			if lastUpdatedTime.Add(leaseDur).After(now().Time) {
				// Lease is still valid. Continue waiting for it to expire.
				if err := sleep(ctx, m.retryDur); err != nil {
					return err
				}
				continue
			}
		}

		var leaseTransitions int32
		if lease.Spec.LeaseTransitions != nil {
			leaseTransitions = *lease.Spec.LeaseTransitions
		}
		leaseTransitions++ // We're claiming the lease, so increment the transitions.

		// Update the lease spec with new values to claim the lease.
		lease.Spec.HolderIdentity = &m.uniqueID
		lease.Spec.LeaseTransitions = &leaseTransitions
		lease.Spec.AcquireTime = now()
		lease.Spec.LeaseDurationSeconds = ptr.To[int32](int32(math.Round(m.expiration.Seconds())))

		// Submit the update to the lease on the API server.
		_, err = m.lease.Update(ctx, lease, metav1.UpdateOptions{})
		if err == nil {
			// Acquired the lease, leave the loop.
			break
		}

		// Failed to update the lease, try again later.
		if err := sleep(ctx, m.retryDur); err != nil {
			return err
		}
	}
	if ctx.Err() != nil {
		return ctx.Err()
	}

	return nil
}

// Extend extends the lock. Lock must be called at least once before
// this function is called. An error is returned if the lock was unable
// to be extended for any reason, in this case the caller must stop
// doing work that required the lock.
func (m *Mutex) Extend(ctx context.Context) error {
	lease, err := m.lease.Get(ctx, m.name, metav1.GetOptions{})
	if err != nil {
		return ErrNotLocked
	}

	// Ensure that the lease is locked by something.
	if lease.Spec.HolderIdentity == nil {
		return ErrNotLocked
	}

	// Ensure that the lease is locked by us.
	if *lease.Spec.HolderIdentity != m.uniqueID {
		return ErrLockedByAnother
	}

	// Set the renew time to now.
	lease.Spec.RenewTime = now()

	// Submit the update to the lease on the API server.
	if _, err := m.lease.Update(ctx, lease, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("extend: error when trying to update Lease: %w", err)
	}
	return nil
}

// Unlock forcibly unlocks the lock for the provided resource name. An
// error is returned if the lock was unable to be unlocked.
func (m *Mutex) Unlock(ctx context.Context) error {
	lease, err := m.lease.Get(ctx, m.name, metav1.GetOptions{})
	if err != nil {
		return ErrNotLocked
	}

	// Ensure that the lease is locked by something.
	if lease.Spec.HolderIdentity == nil {
		return ErrNotLocked
	}

	// Ensure that the lease is locked by us.
	if *lease.Spec.HolderIdentity != m.uniqueID {
		return ErrLockedByAnother
	}

	// Unlock the lease. Leave LeaseTransitions alone so we can track how
	// many times the lease has been locked and unlocked.
	lease.Spec.HolderIdentity = nil
	lease.Spec.AcquireTime = nil
	lease.Spec.RenewTime = nil
	lease.Spec.LeaseDurationSeconds = nil

	if _, err := m.lease.Update(ctx, lease, metav1.UpdateOptions{}); err != nil {
		return fmt.Errorf("unlock: error when trying to update Lease: %w", err)
	}
	return nil
}
