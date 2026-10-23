/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package client_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	. "github.com/onsi/gomega"
	appsv1 "k8s.io/api/apps/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime/schema"
	appsv1applyconfigurations "k8s.io/client-go/applyconfigurations/apps/v1"
	toolscache "k8s.io/client-go/tools/cache"
	"k8s.io/utils/ptr"

	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
	"sigs.k8s.io/controller-runtime/pkg/client/interceptor"
)

// TestOverlayFakeClient checks that every read after every write observes the write without waiting for the cache.
// Reads of Deployments are served from a view that is only updated when the fake informer delivers an event, ten
// seconds after the write, so a read that returns the write before then was served by the overlay. synctest's clock
// only advances when every goroutine is blocked, so a read that waited for the event would take ten seconds.
func TestOverlayFakeClient(t *testing.T) {
	t.Parallel()

	const namespace = "default"

	deployment := func() *appsv1.Deployment {
		return &appsv1.Deployment{
			Name: "test", Namespace: namespace, UID: "test-uid",
		}
	}
	deploymentWithFinalizer := func() *appsv1.Deployment {
		d := deployment()
		d.SetFinalizers([]string{"test.io/finalizer"})
		return d
	}
	resourceVersion := func(g *WithT, rv string) int64 {
		parsed, err := strconv.ParseInt(rv, 10, 64)
		g.Expect(err).NotTo(HaveOccurred(), "failed to parse resource version %q", rv)
		return parsed
	}

	writes := map[string]func(ctx context.Context, c client.Client, g *WithT) int64{
		"Create": func(ctx context.Context, c client.Client, g *WithT) int64 {
			d := deployment()
			g.Expect(c.Create(ctx, d)).To(Succeed())
			return resourceVersion(g, d.GetResourceVersion())
		},
		"Update": func(ctx context.Context, c client.Client, g *WithT) int64 {
			d := deployment()
			g.Expect(c.Get(ctx, client.ObjectKeyFromObject(d), d)).To(Succeed())
			d.SetLabels(map[string]string{"updated": "true"})
			g.Expect(c.Update(ctx, d)).To(Succeed())
			return resourceVersion(g, d.GetResourceVersion())
		},
		"Patch": func(ctx context.Context, c client.Client, g *WithT) int64 {
			d := deployment()
			patch := client.MergeFrom(d.DeepCopyObject().(client.Object))
			d.SetLabels(map[string]string{"patched": "true"})
			g.Expect(c.Patch(ctx, d, patch)).To(Succeed())
			return resourceVersion(g, d.GetResourceVersion())
		},
		"Apply": func(ctx context.Context, c client.Client, g *WithT) int64 {
			ac := appsv1applyconfigurations.Deployment(deployment().GetName(), namespace).
				WithLabels(map[string]string{"applied": "true"})
			g.Expect(c.Apply(ctx, ac, client.FieldOwner("test"))).To(Succeed())
			return resourceVersion(g, ptr.Deref(ac.ResourceVersion, ""))
		},
		"Status Update": func(ctx context.Context, c client.Client, g *WithT) int64 {
			d := deployment()
			g.Expect(c.Get(ctx, client.ObjectKeyFromObject(d), d)).To(Succeed())
			d.Status.Replicas = 5
			g.Expect(c.Status().Update(ctx, d)).To(Succeed())
			return resourceVersion(g, d.GetResourceVersion())
		},
		"Status Patch": func(ctx context.Context, c client.Client, g *WithT) int64 {
			d := deployment()
			patch := client.MergeFrom(d.DeepCopyObject().(client.Object))
			d.Status.Replicas = 6
			g.Expect(c.Status().Patch(ctx, d, patch)).To(Succeed())
			return resourceVersion(g, d.GetResourceVersion())
		},
		"Status Apply": func(ctx context.Context, c client.Client, g *WithT) int64 {
			ac := appsv1applyconfigurations.Deployment("test", namespace).
				WithStatus(appsv1applyconfigurations.DeploymentStatus().WithReplicas(7))
			g.Expect(c.Status().Apply(ctx, ac, client.FieldOwner("test"))).To(Succeed())
			return resourceVersion(g, ptr.Deref(ac.ResourceVersion, ""))
		},
	}

	get := func(ctx context.Context, c client.Client, g *WithT, writtenRV int64) {
		result := deployment()
		g.Expect(c.Get(ctx, client.ObjectKeyFromObject(result), result)).To(Succeed())
		g.Expect(resourceVersion(g, result.GetResourceVersion())).To(BeNumerically(">=", writtenRV))
	}
	list := func(ctx context.Context, c client.Client, g *WithT, writtenRV int64) {
		result := &appsv1.DeploymentList{}
		g.Expect(c.List(ctx, result)).To(Succeed())
		if g.Expect(result.Items).To(HaveLen(1)) {
			g.Expect(resourceVersion(g, result.Items[0].GetResourceVersion())).To(BeNumerically(">=", writtenRV))
		}
	}
	reads := map[string]func(ctx context.Context, c client.Client, g *WithT, writtenRV int64){"Get": get, "List": list}

	type testCase struct {
		name           string
		initObject     func() *appsv1.Deployment
		write          func(ctx context.Context, c client.Client, g *WithT) int64
		read           func(ctx context.Context, c client.Client, g *WithT, writtenRV int64)
		readAfterCatch func(ctx context.Context, c client.Client, g *WithT, writtenRV int64)
	}
	var testCases []testCase
	for writeName, write := range writes {
		for readName, read := range reads {
			tc := testCase{name: readName + " after " + writeName, write: write, read: read, readAfterCatch: read}
			if writeName != "Create" {
				tc.initObject = deployment
			}
			testCases = append(testCases, tc)
		}
	}
	getDeleted := func(ctx context.Context, c client.Client, g *WithT, _ int64) {
		d := deployment()
		err := c.Get(ctx, client.ObjectKeyFromObject(d), d)
		g.Expect(apierrors.IsNotFound(err)).To(BeTrue(), "expected a NotFound error, got %v", err)
	}
	listDeleted := func(ctx context.Context, c client.Client, g *WithT, _ int64) {
		result := &appsv1.DeploymentList{}
		g.Expect(c.List(ctx, result)).To(Succeed())
		g.Expect(result.Items).To(BeEmpty())
	}
	deleteObject := func(ctx context.Context, c client.Client, g *WithT) int64 {
		g.Expect(c.Delete(ctx, deployment())).To(Succeed())
		return 0
	}
	deleteWithoutUID := func(ctx context.Context, c client.Client, g *WithT) int64 {
		d := deployment()
		d.SetUID("")
		g.Expect(c.Delete(ctx, d)).To(Succeed())
		return 0
	}
	getTerminating := func(ctx context.Context, c client.Client, g *WithT, _ int64) {
		result := deployment()
		g.Expect(c.Get(ctx, client.ObjectKeyFromObject(result), result)).To(Succeed())
		g.Expect(result.GetDeletionTimestamp()).ToNot(BeNil(), "expected the deletionTimestamp to be set")
	}
	listTerminating := func(ctx context.Context, c client.Client, g *WithT, _ int64) {
		result := &appsv1.DeploymentList{}
		g.Expect(c.List(ctx, result)).To(Succeed())
		if g.Expect(result.Items).To(HaveLen(1)) {
			g.Expect(result.Items[0].GetDeletionTimestamp()).ToNot(BeNil(), "expected the deletionTimestamp to be set")
		}
	}
	testCases = append(testCases,
		testCase{name: "Get after Delete", initObject: deployment, write: deleteObject, read: getDeleted, readAfterCatch: getDeleted},
		testCase{name: "List after Delete", initObject: deployment, write: deleteObject, read: listDeleted, readAfterCatch: listDeleted},
		testCase{name: "Get after Delete without UID", initObject: deployment, write: deleteWithoutUID, read: getDeleted, readAfterCatch: getDeleted},
		testCase{name: "List after Delete without UID", initObject: deployment, write: deleteWithoutUID, read: listDeleted, readAfterCatch: listDeleted},
		testCase{name: "Get after Delete with finalizer", initObject: deploymentWithFinalizer, write: deleteObject, read: getTerminating, readAfterCatch: getTerminating},
		testCase{name: "List after Delete with finalizer", initObject: deploymentWithFinalizer, write: deleteObject, read: listTerminating, readAfterCatch: listTerminating},
	)

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			synctest.Test(t, func(t *testing.T) {
				// Failures are reported without stopping the test, so the fake informer's pending events still drain
				// before the bubble exits
				g := NewWithT(nonFatalT{t})
				var initObjects []*appsv1.Deployment
				if tc.initObject != nil {
					initObjects = append(initObjects, tc.initObject())
				}
				c := newOverlayFakeClient(t, initObjects...)
				synctest.Wait()

				written := tc.write(t.Context(), c, g)
				start := time.Now()
				tc.read(t.Context(), c, g, written)
				g.Expect(time.Since(start)).To(BeZero(), "the read waited for the cache")

				// Once the cache has caught up, reads are served from it
				time.Sleep(2 * watchDelay)
				synctest.Wait()
				tc.readAfterCatch(t.Context(), c, g, written)
			})
		})
	}
}

// newOverlayFakeClient returns an overlay client whose reads of Deployments come from a view of the fake client that
// is only updated when the fake informer delivers an event, watchDelay after each write
func newOverlayFakeClient(t *testing.T, initObjects ...*appsv1.Deployment) client.Client {
	t.Helper()

	fc := newFakeCache()
	view := &deploymentView{objects: map[client.ObjectKey]*appsv1.Deployment{}}
	// The view's handler is registered first, so like an informer's store it is updated before the overlay is told
	_, _ = fc.informer.AddEventHandler(view)

	objects := make([]client.Object, 0, len(initObjects))
	for _, d := range initObjects {
		objects = append(objects, d)
	}
	upstream := fake.NewClientBuilder().
		WithGlobalResourceVersionCounter().
		WithObjects(objects...).
		WithInterceptorFuncs(fc.interceptorFuncs()).
		Build()
	for _, d := range initObjects {
		stored := &appsv1.Deployment{}
		if err := upstream.Get(t.Context(), client.ObjectKeyFromObject(d), stored); err != nil {
			t.Fatal(err)
		}
		view.OnAdd(stored, true)
	}

	reader := interceptor.NewClient(upstream, interceptor.Funcs{
		Get: func(ctx context.Context, c client.WithWatch, key client.ObjectKey, obj client.Object, opts ...client.GetOption) error {
			if d, ok := obj.(*appsv1.Deployment); ok {
				return view.get(key, d)
			}
			return c.Get(ctx, key, obj, opts...)
		},
		List: func(ctx context.Context, c client.WithWatch, list client.ObjectList, opts ...client.ListOption) error {
			if l, ok := list.(*appsv1.DeploymentList); ok {
				view.list(l)
				return nil
			}
			return c.List(ctx, list, opts...)
		},
	})
	return client.NewOverlayClient(&fakeConsistentClientUpstream{Client: reader}, fc)
}

// deploymentView is the fake informer's store
type deploymentView struct {
	mu      sync.Mutex
	objects map[client.ObjectKey]*appsv1.Deployment
}

func (v *deploymentView) OnAdd(obj any, _ bool) { v.set(obj) }

func (v *deploymentView) OnUpdate(_, obj any) { v.set(obj) }

func (v *deploymentView) OnDelete(obj any) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if d, ok := obj.(*appsv1.Deployment); ok {
		delete(v.objects, client.ObjectKeyFromObject(d))
	}
}

func (v *deploymentView) set(obj any) {
	v.mu.Lock()
	defer v.mu.Unlock()
	if d, ok := obj.(*appsv1.Deployment); ok {
		v.objects[client.ObjectKeyFromObject(d)] = d.DeepCopy()
	}
}

func (v *deploymentView) get(key client.ObjectKey, into *appsv1.Deployment) error {
	v.mu.Lock()
	defer v.mu.Unlock()
	d, ok := v.objects[key]
	if !ok {
		return apierrors.NewNotFound(schema.GroupResource{Group: "apps", Resource: "deployments"}, key.Name)
	}
	d.DeepCopyInto(into)
	return nil
}

func (v *deploymentView) list(into *appsv1.DeploymentList) {
	v.mu.Lock()
	defer v.mu.Unlock()
	into.Items = nil
	for _, d := range v.objects {
		into.Items = append(into.Items, *d.DeepCopy())
	}
}

var _ toolscache.ResourceEventHandler = &deploymentView{}

// nonFatalT reports Gomega failures with Errorf rather than Fatalf
type nonFatalT struct {
	*testing.T
}

func (t nonFatalT) Fatalf(format string, args ...any) {
	t.Helper()
	t.Errorf(format, args...)
}
