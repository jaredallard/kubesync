package kubesync_test

import (
	"context"
	"fmt"
	"os"

	"github.com/jaredallard/kubesync"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// This example shows how to lock, extend, and unlock a mutex. It uses a
// fake kubernetes client to not require a real kubernetes cluster to
// run the example.
func ExampleMutex() {
	ctx := context.Background()
	k := fake.NewClientset()
	instanceID, err := os.Hostname()
	if err != nil {
		panic(fmt.Errorf("failed to get hostname: %w", err))
	}

	sync := kubesync.New(k, corev1.NamespaceDefault, instanceID)
	m := sync.NewMutex("im-a-lock")

	if err := m.Lock(ctx); err != nil {
		panic(fmt.Errorf("failed to lock: %w", err))
	}

	// do something with the lock ...

	// taking a long time? extend the lock
	if err := m.Extend(ctx); err != nil {
		// failed to extend the lock. Instead of panicing, alternatively
		// you could return early or do something else to handle this error.
		panic(fmt.Errorf("failed to extend lock: %w", err))
	}

	// don't forget to unlock it
	m.Unlock(ctx)
}
