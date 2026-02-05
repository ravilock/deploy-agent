package autodiscovery

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/kubernetes/fake"
	clientTesting "k8s.io/client-go/testing"

	coordinationv1 "k8s.io/api/coordination/v1"
	k8sErrors "k8s.io/apimachinery/pkg/api/errors"
)

type leaseReactor struct {
	leases []*coordinationv1.Lease
	lock   sync.Mutex
}

func newLeaseReactor() *leaseReactor {
	return &leaseReactor{
		leases: make([]*coordinationv1.Lease, 0),
	}
}

func (l *leaseReactor) Handles(action clientTesting.Action) bool {
	return action.GetResource().Resource == "leases"
}

func (l *leaseReactor) React(action clientTesting.Action) (handled bool, ret runtime.Object, err error) {
	l.lock.Lock()
	defer l.lock.Unlock()
	switch action.GetVerb() {
	case "list":
		if _, ok := action.(clientTesting.ListAction); ok {
			leaseList := &coordinationv1.LeaseList{
				Items: []coordinationv1.Lease{},
			}
			for _, lease := range l.leases {
				leaseList.Items = append(leaseList.Items, *lease.DeepCopy())
			}
			return true, leaseList, nil
		}
		return false, nil, nil
	case "create":
		if createAction, ok := action.(clientTesting.CreateAction); ok {
			lease, err := l.createLease(createAction)
			return true, lease, err
		}
		return false, nil, nil
	case "get":
		if getAction, ok := action.(clientTesting.GetAction); ok {
			lease, err := l.getLease(getAction)
			return true, lease, err
		}
		return false, nil, nil
	case "update":
		if updateAction, ok := action.(clientTesting.UpdateAction); ok {
			lease, err := l.updateLease(updateAction)
			return true, lease, err
		}
		return false, nil, nil
	case "delete":
		panic("should not be called")
	}
	return false, nil, nil
}

func (l *leaseReactor) getLease(getAction clientTesting.GetAction) (ret runtime.Object, err error) {
	for _, lease := range l.leases {
		if lease.Name == getAction.GetName() && lease.Namespace == getAction.GetNamespace() {
			return lease.DeepCopy(), nil
		}
	}
	return nil, k8sErrors.NewNotFound(coordinationv1.Resource("leases"), getAction.GetName())
}

func (l *leaseReactor) createLease(createAction clientTesting.CreateAction) (ret runtime.Object, err error) {
	leaseObject, ok := createAction.GetObject().(*coordinationv1.Lease)
	if !ok {
		return nil, errors.New("not a lease object")
	}
	for _, lease := range l.leases {
		if lease.Name == leaseObject.Name && lease.Namespace == leaseObject.Namespace {
			return nil, k8sErrors.NewAlreadyExists(coordinationv1.Resource("leases"), leaseObject.Name)
		}
	}
	lease := leaseObject.DeepCopy()
	lease.CreationTimestamp = metav1.Now()
	l.leases = append(l.leases, lease)
	return leaseObject, nil
}

func (l *leaseReactor) updateLease(updateAction clientTesting.UpdateAction) (ret runtime.Object, err error) {
	updateLeaseObject, ok := updateAction.GetObject().(*coordinationv1.Lease)
	if !ok {
		return nil, errors.New("not a lease object")
	}
	for i, currentLease := range l.leases {
		if currentLease.Name == updateLeaseObject.Name && currentLease.Namespace == updateLeaseObject.Namespace {
			lease := updateLeaseObject.DeepCopy()
			lease.CreationTimestamp = currentLease.CreationTimestamp
			l.leases[i] = lease
			return updateLeaseObject, nil
		}
	}
	return nil, k8sErrors.NewNotFound(coordinationv1.Resource("leases"), updateLeaseObject.Name)
}

func TestLeaser_NoRaceOrPrematureCancel(t *testing.T) {
	kubeClient := fake.NewSimpleClientset()
	leasablePodsCh := make(chan *corev1.Pod)
	leaserInstance, leasedPodsCh, err := newLeaser(kubeClient, leasablePodsCh)
	require.NoError(t, err)

	leaseReactor := newLeaseReactor()
	kubeClient.ReactionChain = append([]clientTesting.Reactor{leaseReactor}, kubeClient.ReactionChain...)

	// Prepare test pods
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "buildkit-0",
			Namespace: "tsuru-main",
		},
	}
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "buildkit-1",
			Namespace: "tsuru-main",
		},
	}

	// Start lease acquisition in a separate goroutine
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		leaserInstance.acquireLeaseForAllPods(ctx, KubernertesDiscoveryOptions{LeasePrefix: "lease"})
	}()

	// Send pods to the channel
	go func() {
		leasablePodsCh <- pod1
	}()
	go func() {
		leasablePodsCh <- pod2
	}()

	// Collect leased pods
	leased := make([]*corev1.Pod, 0, 2)
	timeout := time.After(2 * time.Second)
loop:
	for {
		select {
		case p, ok := <-leasedPodsCh:
			if !ok {
				break loop
			}
			leased = append(leased, p)
			if len(leased) == 2 {
				break loop
			}
		case <-timeout:
			t.Fatal("timed out waiting for leased pods")
		}
	}
	close(leasablePodsCh)

	wg.Wait()

	if len(leased) != 2 {
		t.Errorf("expected 2 leased pods, got %d", len(leased))
	}
}

func TestLeaser_LeaseAndRelease(t *testing.T) {
	leasePrefix := "lease"
	kubeClient := fake.NewSimpleClientset()
	leasablePodsCh := make(chan *corev1.Pod)
	leaserInstance, leasedPodsCh, err := newLeaser(kubeClient, leasablePodsCh)
	require.NoError(t, err)

	leaseReactor := newLeaseReactor()
	kubeClient.ReactionChain = append([]clientTesting.Reactor{leaseReactor}, kubeClient.ReactionChain...)

	// Prepare test pods
	pod1 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "buildkit-0",
			Namespace: "tsuru-main",
		},
	}
	pod2 := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "buildkit-1",
			Namespace: "tsuru-main",
		},
	}

	// Start lease acquisition in a separate goroutine
	cancellabeCtx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		leaserInstance.acquireLeaseForAllPods(cancellabeCtx, KubernertesDiscoveryOptions{LeasePrefix: leasePrefix})
	}()

	// Send pods to the channel
	go func() {
		leasablePodsCh <- pod1
	}()
	go func() {
		leasablePodsCh <- pod2
	}()

	// Collect leased pods
	var firstLeasedPod *corev1.Pod
	var secondLeasedPod *corev1.Pod
	leased := make([]*corev1.Pod, 0, 2)
	timeout := time.After(2 * time.Second)
loop:
	for {
		select {
		case leasedPod, ok := <-leasedPodsCh:
			require.True(t, ok)
			if firstLeasedPod == nil {
				firstLeasedPod = leasedPod
			} else {
				secondLeasedPod = leasedPod
			}
			leased = append(leased, leasedPod)
			if len(leased) == 2 {
				break loop
			}
		case <-timeout:
			t.Fatal("timed out waiting for leased pods")
		}
	}

	firstLease, err := kubeClient.CoordinationV1().Leases("tsuru-main").Get(context.Background(), fmt.Sprintf("%s-%s", strings.TrimRight(leasePrefix, "-"), firstLeasedPod.Name), metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, leaserInstance.holderName, *firstLease.Spec.HolderIdentity)

	secondLease, err := kubeClient.CoordinationV1().Leases("tsuru-main").Get(context.Background(), fmt.Sprintf("%s-%s", strings.TrimRight(leasePrefix, "-"), secondLeasedPod.Name), metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, leaserInstance.holderName, *secondLease.Spec.HolderIdentity)

	leaserInstance.releaseAll(releaseOptions{except: fmt.Sprintf("%s/%s", firstLeasedPod.Namespace, firstLeasedPod.Name)})
	time.Sleep(leaseDuration) // Wait some time, we expect the release loop to continue working for one lease, but not for the other

	firstLease, err = kubeClient.CoordinationV1().Leases("tsuru-main").Get(context.Background(), fmt.Sprintf("%s-%s", strings.TrimRight(leasePrefix, "-"), firstLeasedPod.Name), metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, leaserInstance.holderName, *firstLease.Spec.HolderIdentity)

	secondLease, err = kubeClient.CoordinationV1().Leases("tsuru-main").Get(context.Background(), fmt.Sprintf("%s-%s", strings.TrimRight(leasePrefix, "-"), secondLeasedPod.Name), metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "", *secondLease.Spec.HolderIdentity)

	cancel()
	close(leasablePodsCh)
	wg.Wait()
	time.Sleep(leaseDuration) // Wait some time, we expect both leases to be released now

	firstLease, err = kubeClient.CoordinationV1().Leases("tsuru-main").Get(context.Background(), fmt.Sprintf("%s-%s", strings.TrimRight(leasePrefix, "-"), firstLeasedPod.Name), metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "", *firstLease.Spec.HolderIdentity)

	secondLease, err = kubeClient.CoordinationV1().Leases("tsuru-main").Get(context.Background(), fmt.Sprintf("%s-%s", strings.TrimRight(leasePrefix, "-"), secondLeasedPod.Name), metav1.GetOptions{})
	require.NoError(t, err)
	require.Equal(t, "", *secondLease.Spec.HolderIdentity)
}

func TestLeaser_MultipleLeasers(t *testing.T) {
	leasePrefix := "lease"
	kubeClient := fake.NewSimpleClientset()
	leasablePodsCh1 := make(chan *corev1.Pod)
	leaserInstance1, leasedPodsCh1, err := newLeaser(kubeClient, leasablePodsCh1)
	require.NoError(t, err)
	leasablePodsCh2 := make(chan *corev1.Pod)
	leaserInstance2, leasedPodsCh2, err := newLeaser(kubeClient, leasablePodsCh2)
	require.NoError(t, err)

	leaseReactor := newLeaseReactor()
	kubeClient.ReactionChain = append([]clientTesting.Reactor{leaseReactor}, kubeClient.ReactionChain...)

	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "buildkit-0",
			Namespace: "tsuru-main",
		},
	}

	cancellableCtx1, cancel1 := context.WithCancel(context.Background())
	var wg1 sync.WaitGroup
	wg1.Add(1)
	go func() {
		defer wg1.Done()
		leaserInstance1.acquireLeaseForAllPods(cancellableCtx1, KubernertesDiscoveryOptions{LeasePrefix: leasePrefix})
	}()

	callableCtx2, cancel2 := context.WithCancel(context.Background())
	var wg2 sync.WaitGroup
	wg2.Add(1)
	go func() {
		defer wg2.Done()
		leaserInstance2.acquireLeaseForAllPods(callableCtx2, KubernertesDiscoveryOptions{LeasePrefix: leasePrefix})
	}()

	go func() {
		leasablePodsCh1 <- pod
	}()
	go func() {
		leasablePodsCh2 <- pod
	}()

	cancel1()
	cancel2()
}
