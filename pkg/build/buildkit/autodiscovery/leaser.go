// Copyright 2026 tsuru authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package autodiscovery

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog"
)

var (
	leaseDuration time.Duration = 5 * time.Second
	renewDeadline time.Duration = 2 * time.Second
	retryPeriod   time.Duration = 500 * time.Millisecond
)

type leaseContextCancelPair struct {
	ctx    context.Context
	cancel context.CancelFunc
}

type leaser struct {
	kubernetesInterface kubernetes.Interface
	leasablePodsCh      <-chan *corev1.Pod
	leasedPodsCh        chan<- *corev1.Pod
	leaseAcquiringWg    *sync.WaitGroup
	leaseCancelByPod    map[string]leaseContextCancelPair
	holderName          string
}

func newLeaser(kubernetesInterface kubernetes.Interface, leasablePodsCh <-chan *corev1.Pod) (*leaser, <-chan *corev1.Pod, error) {
	holderName := os.Getenv("POD_NAME")
	if holderName == "" {
		hostname, err := os.Hostname()
		if err != nil {
			return nil, nil, err
		}
		holderName = hostname
	}
	holderName = fmt.Sprintf("%s-%d", holderName, time.Now().Unix())
	leasedPodsCh := make(chan *corev1.Pod, 1)
	return &leaser{
		kubernetesInterface: kubernetesInterface,
		leasablePodsCh:      leasablePodsCh,
		leasedPodsCh:        leasedPodsCh,
		leaseAcquiringWg:    &sync.WaitGroup{},
		leaseCancelByPod:    make(map[string]leaseContextCancelPair),
		holderName:          holderName,
	}, leasedPodsCh, nil
}

type releaseOptions struct {
	except string
}

func (l *leaser) releaseAll(opts ...releaseOptions) {
	var opt releaseOptions
	if len(opts) == 0 {
		opt = releaseOptions{}
	} else {
		opt = opts[0]
	}
	l.leaseAcquiringWg.Wait()
	for name, contextPair := range l.leaseCancelByPod {
		klog.V(4).Infof("Address for leaseCancelByPod: %p", l.leaseCancelByPod)
		klog.V(4).Infof("Size for leaseCancelByPod: %d", len(l.leaseCancelByPod))
		klog.V(4).Infof("Item in leaseCancelByPod: key=%s, ctx=%p, cancel=%p", name, contextPair.ctx, contextPair.cancel)
		if opt.except == name {
			klog.V(4).Infof("Skipping release of lock for %s pod", name)
			continue
		}
		klog.V(4).Infof("Releasing lock for %s pod", name)
		contextPair.cancel()
	}
}

// acquireLeaseForAllPods tries to acquire leases for all pods received on leasablePodsCh.
// it is a blocking call and only returns after leasablePodsCh is closed and all lease acquisition.
// it should probably be used in a separate goroutine.
func (l *leaser) acquireLeaseForAllPods(ctx context.Context, opts KubernertesDiscoveryOptions) {
	// NOTE:(ravilock) the usage of WaitGroup here is to ensure that we only close the leasedPodsCh
	// after all goroutines that might write to it are done. i.e. The goroutines that acquire leases for a buildkit pod.
	for leasablePod := range l.leasablePodsCh {
		leaseCancelKey := fmt.Sprintf("%s/%s", leasablePod.Namespace, leasablePod.Name)
		klog.V(4).Infof("Received leasable pod: %s", leaseCancelKey)
		if _, found := l.leaseCancelByPod[leaseCancelKey]; found {
			klog.V(4).Infof("Lease acquisition already in progress for pod %s, skipping...", leaseCancelKey)
			continue
		}
		klog.V(4).Infof("Address for leaseCancelByPod: %p - podNameForDebug: %s", l.leaseCancelByPod, leaseCancelKey)
		klog.V(4).Infof("Size for leaseCancelByPod: %d", len(l.leaseCancelByPod))
		for key, contextPair := range l.leaseCancelByPod {
			klog.V(4).Infof("Item in leaseCancelByPod: key=%s, ctx=%p, cancel=%p", key, contextPair.ctx, contextPair.cancel)
		}

		leaseCtx, leaseCancel := context.WithCancel(ctx)
		l.leaseCancelByPod[leaseCancelKey] = leaseContextCancelPair{ctx: leaseCtx, cancel: leaseCancel}

		l.leaseAcquiringWg.Add(1)
		go func() {
			l.acquireLeaseForPod(leaseCtx, leasablePod, opts)
		}()
		go func() {
			<-leaseCtx.Done()
			klog.V(4).Infof("leaseCtx %p for %s was cancelled at %s", leaseCtx, leaseCancelKey, time.Now().String())
		}()
	}
	l.leaseAcquiringWg.Wait()
	close(l.leasedPodsCh)
}

// acquireLeaseForPod tries to acquire a lease for the given pod.
// it is a blocking call and only returns after the lease is lost or the given context is canceled.
// it should always be used in a separate goroutine.
func (l *leaser) acquireLeaseForPod(ctx context.Context, pod *corev1.Pod, opts KubernertesDiscoveryOptions) {
	klog.V(4).Infof("Attempting to acquire the lease for pod %s/%s under holder name %q...", pod.Namespace, pod.Name, l.holderName)
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock: &resourcelock.LeaseLock{
			LeaseMeta: metav1.ObjectMeta{
				Name:      fmt.Sprintf("%s-%s", strings.TrimRight(opts.LeasePrefix, "-"), pod.Name),
				Namespace: pod.Namespace,
			},
			Client: l.kubernetesInterface.CoordinationV1(),
			LockConfig: resourcelock.ResourceLockConfig{
				Identity: l.holderName,
			},
		},
		ReleaseOnCancel: true,
		LeaseDuration:   leaseDuration,
		RenewDeadline:   renewDeadline,
		RetryPeriod:     retryPeriod,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(_ context.Context) {
				select {
				case l.leasedPodsCh <- pod:
					klog.V(4).Infof("Selected BuildKit pod: %s/%s", pod.Namespace, pod.Name)

				case <-ctx.Done():
					klog.V(4).Infof("Received context cancellation: %s/%s", pod.Namespace, pod.Name)
				}
				l.leaseAcquiringWg.Done()
			},
			OnStoppedLeading: func() {
				klog.V(4).Infof("Stopped leading %s/%s pod", pod.Namespace, pod.Name)
			},
		},
	})
	klog.V(4).Infof("Shutting off the lease for %s/%s pod", pod.Namespace, pod.Name)
}
