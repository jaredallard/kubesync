package kubesync_test

import (
	"strings"
	"testing"
	"time"

	"github.com/jaredallard/kubesync"
	"gotest.tools/v3/assert"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes/fake"
)

// getMutexInputs returns test specific inputs for kubesync.NewMutex.
// Includes a fake kubernetes client, namespace, and instanceID. Call
// this whenever creating a new Mutex instance.
func getMutexInputs(t *testing.T) (k *fake.Clientset, namespace, instanceID string) {
	t.Helper()

	k = fake.NewClientset()
	namespace = "test-lock-namespace"
	instanceID = strings.ToLower(t.Name())

	_, err := k.CoreV1().Namespaces().Create(t.Context(), &v1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: namespace,
		},
	}, metav1.CreateOptions{})
	assert.NilError(t, err, "failed to create namespace")
	return
}

func TestCanLockUnlock(t *testing.T) {
	t.Parallel()

	k, namespace, instanceID := getMutexInputs(t)
	sync := kubesync.New(k, namespace, instanceID)
	m := sync.NewMutex("test-lock")

	assert.NilError(t, m.Lock(t.Context()), "failed to lock")
	assert.NilError(t, m.Unlock(t.Context()), "failed to unlock")
}

func TestShouldFailUnlockWithoutLock(t *testing.T) {
	t.Parallel()

	k, namespace, instanceID := getMutexInputs(t)
	sync := kubesync.New(k, namespace, instanceID)
	m := sync.NewMutex("test-lock")

	assert.ErrorIs(t,
		m.Unlock(t.Context()),
		kubesync.ErrNotLocked,
	)
}

func TestShouldBeAbleToExtend(t *testing.T) {
	k, namespace, instanceID := getMutexInputs(t)
	sync := kubesync.New(k, namespace, instanceID)
	lockName := "test-lock"
	m := sync.NewMutex(lockName)

	assert.NilError(t, m.Lock(t.Context()), "failed to lock")

	beforeExtendLease, err := k.CoordinationV1().Leases(namespace).Get(t.Context(), lockName, metav1.GetOptions{})
	assert.NilError(t, err, "failed to get lease")
	assert.Equal(t, beforeExtendLease.Spec.RenewTime, (*metav1.MicroTime)(nil), "lease should not have renew time set")
	// this is nil by default, but for testing we set it to now just for
	// easier testing.
	beforeExtendLease.Spec.RenewTime = &metav1.MicroTime{Time: time.Now().UTC()}

	time.Sleep(50 * time.Millisecond)

	// extend the lease
	assert.NilError(t, m.Extend(t.Context()), "failed to extend")

	// check that the lease was updated
	afterExtendLease, err := k.CoordinationV1().Leases(namespace).Get(t.Context(), lockName, metav1.GetOptions{})
	assert.NilError(t, err, "failed to get lease")

	// afterExtend should have newer renew time than original acquire
	// time.
	assert.Assert(t, afterExtendLease.Spec.RenewTime.After(beforeExtendLease.Spec.AcquireTime.Time))

	// afterExtend should have newer renew time.
	assert.Assert(t, afterExtendLease.Spec.RenewTime.After(beforeExtendLease.Spec.RenewTime.Time))

	// check that the acquire time didn't change.
	assert.Assert(t, afterExtendLease.Spec.AcquireTime.Equal(beforeExtendLease.Spec.AcquireTime))
}

func TestShouldSupportDistributedLocking(t *testing.T) {
	t.Parallel()

	k, namespace, instanceID := getMutexInputs(t)
	lockName := "test-lock"

	// create the mutex
	sync1 := kubesync.New(k, namespace, instanceID)
	m1 := sync1.NewMutex(lockName)

	// lock it once
	assert.NilError(t, m1.Lock(t.Context()), "failed to lock")

	// attempt to lock it again, which should block until we call unlock()
	// on the first lock instance.
	secondGotLock := make(chan struct{})
	go func() {
		sync2 := kubesync.New(k, namespace, instanceID)
		m2 := sync2.NewMutex(lockName)
		assert.NilError(t, m2.Lock(t.Context()), "failed to lock")
		close(secondGotLock)
		assert.NilError(t, m2.Unlock(t.Context()), "failed to unlock")
	}()

	// wait 2 seconds to ensure that the second lock didn't succeed
	// immediately, thus not working properly.
	time.Sleep(2 * time.Second)

	// check if the second lock instance is still blocked
	select {
	case <-secondGotLock:
		t.Fatal("second lock instance should not have succeeded yet")
	default:
	}

	// unlock the first lock instance, which should allow the second lock
	// instance to proceed.
	assert.NilError(t, m1.Unlock(t.Context()), "failed to unlock")

	// wait for the second lock instance to finish
	select {
	case <-secondGotLock:
		// success
	case <-time.After(5 * time.Second):
		t.Fatal("second lock instance should have succeeded")
	}
}
