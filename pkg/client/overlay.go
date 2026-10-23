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

package client

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"reflect"
	"strings"
	"sync"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/resourceversion"
	toolscache "k8s.io/client-go/tools/cache"

	"sigs.k8s.io/controller-runtime/pkg/cache/cacheapi"
	"sigs.k8s.io/controller-runtime/pkg/client/apiutil"
)

var _ Client = (*overlayClient)(nil)

// overlayClient serves reads from the cache, overlaid with the objects returned by its own writes that the cache has
// not observed yet. A write's response is held until the informer delivers that resourceVersion or a newer one, and a
// read returns whichever of the overlay's copy and the cache's copy is newer. Unlike consistentClient, reads never wait
// for the cache, and the informer's store and event handlers never see the overlay.
//
// Versions are ordered with resourceversion.CompareResourceVersion (KEP-5504). If two versions can't be compared, the
// cache's copy is served, so the client degrades to the plain cache-backed client rather than serving an object it
// can't order.
type overlayClient struct {
	upstream  consistentClientUpstream
	informers cacheapi.Informers
	// transform is applied to write responses before they are served, so that they carry the same fields as the
	// objects in the cache
	transform toolscache.TransformFunc

	mu       sync.Mutex
	overlays map[gvkAndRepresentation]*overlay
}

// overlay holds the write responses for one kind and representation that the informer hasn't observed yet
type overlay struct {
	mu      sync.RWMutex
	entries map[types.NamespacedName]overlayEntry
}

// overlayEntry is a write response, or a tombstone for an object the client deleted from storage
type overlayEntry struct {
	obj Object
	// deleted marks a tombstone. It hides the cache's copy of the object with uid until the informer delivers the
	// delete.
	deleted bool
	uid     types.UID
}

func newOverlayClient(upstream consistentClientUpstream, informers cacheapi.Informers, transform toolscache.TransformFunc) *overlayClient {
	return &overlayClient{
		upstream:  upstream,
		informers: informers,
		transform: transform,
		overlays:  map[gvkAndRepresentation]*overlay{},
	}
}

// overlayFor returns the overlay for the object's kind and representation, registering an event handler that evicts
// its entries as the informer observes them. The handler is registered before the first entry is recorded, so no
// observation can be missed.
func (c *overlayClient) overlayFor(ctx context.Context, key gvkAndRepresentation, obj Object) (*overlay, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if o, ok := c.overlays[key]; ok {
		return o, nil
	}
	informer, err := c.informers.GetInformer(ctx, obj)
	if err != nil {
		return nil, fmt.Errorf("failed to get informer for %s: %w", key.gvk, err)
	}
	o := &overlay{entries: map[types.NamespacedName]overlayEntry{}}
	if _, err := informer.AddEventHandler(toolscache.ResourceEventHandlerFuncs{
		AddFunc:    func(obj any) { o.observe(obj, false) },
		UpdateFunc: func(_, obj any) { o.observe(obj, false) },
		DeleteFunc: func(obj any) { o.observe(obj, true) },
	}); err != nil {
		return nil, fmt.Errorf("failed to register overlay handler on informer for %s: %w", key.gvk, err)
	}
	c.overlays[key] = o
	return o, nil
}

// observe evicts the entry for an object that the informer has caught up to. The informer updates its store before it
// calls handlers, so once an entry is evicted the cache holds a version at least as new.
func (o *overlay) observe(raw any, deleted bool) {
	if tombstone, ok := raw.(toolscache.DeletedFinalStateUnknown); ok {
		raw = tombstone.Obj
	}
	seen, ok := raw.(Object)
	if !ok {
		return
	}
	key := ObjectKeyFromObject(seen)
	o.mu.Lock()
	defer o.mu.Unlock()
	entry, ok := o.entries[key]
	if !ok {
		return
	}
	switch {
	// A different object with the same name: the overlay's object is gone or has been replaced
	case entry.uid != seen.GetUID():
	// The tombstoned object's delete
	case entry.deleted && deleted:
	case !entry.deleted && (deleted || newerOrIncomparable(seen, entry.obj)):
	default:
		return
	}
	delete(o.entries, key)
}

func (o *overlay) snapshot() map[types.NamespacedName]overlayEntry {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return maps.Clone(o.entries)
}

func (o *overlay) get(key types.NamespacedName) (overlayEntry, bool) {
	o.mu.RLock()
	defer o.mu.RUnlock()
	entry, ok := o.entries[key]
	return entry, ok
}

// record holds the entry unless the overlay already holds a newer version of the same object
func (o *overlay) record(key types.NamespacedName, entry overlayEntry) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if cur, ok := o.entries[key]; ok && cur.uid == entry.uid && !cur.deleted && !entry.deleted && !newerOrIncomparable(entry.obj, cur.obj) {
		return
	}
	o.entries[key] = entry
}

// forgetIfObserved evicts the entry if the cache already holds it. A response can arrive after the informer has
// delivered it, in which case no later event would evict it.
func (o *overlay) forgetIfObserved(key types.NamespacedName, cached Object) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if entry, ok := o.entries[key]; ok && !entry.deleted && entry.uid == cached.GetUID() && newerOrIncomparable(cached, entry.obj) {
		delete(o.entries, key)
	}
}

// newerOrIncomparable reports whether a's resourceVersion is the same as or newer than b's, or the two can't be ordered
func newerOrIncomparable(a, b Object) bool {
	cmp, err := resourceversion.CompareResourceVersion(a.GetResourceVersion(), b.GetResourceVersion())
	return err != nil || cmp >= 0
}

// served picks what a read returns for an object, given the cache's copy (nil if the cache doesn't hold it) and the
// overlay's entry for it. It returns nil if the object doesn't exist as far as the client knows.
func served(cached Object, entry overlayEntry) Object {
	switch {
	case entry.deleted:
		if cached != nil && cached.GetUID() != entry.uid {
			return cached
		}
		return nil
	case cached == nil:
		return entry.obj
	case cached.GetUID() == entry.uid && !newerOrIncomparable(cached, entry.obj):
		return entry.obj
	default:
		return cached
	}
}

func (c *overlayClient) recordWrite(ctx context.Context, obj any) error {
	target, key, cacheObj, _, err := writeTargetFor(obj, c.upstream.Scheme())
	if err != nil {
		return err
	}
	switch t := obj.(type) {
	case *unstructuredApplyConfiguration:
		// writeTargetFor already returns the unstructured object the response was decoded into
	case applyConfiguration:
		// The response to a typed apply is decoded into the apply configuration, so convert it to the typed object
		raw, err := json.Marshal(t)
		if err != nil {
			return fmt.Errorf("failed to marshal apply configuration %T: %w", obj, err)
		}
		if err := json.Unmarshal(raw, cacheObj); err != nil {
			return fmt.Errorf("failed to convert apply configuration %T to %T: %w", obj, cacheObj, err)
		}
	case Object:
		target.representation = representationIDForObj(t)
	}
	o, err := c.overlayFor(ctx, target, cacheObj)
	if err != nil {
		return err
	}
	response, err := c.transformed(cacheObj)
	if err != nil {
		return err
	}
	o.record(key, overlayEntry{obj: response, uid: response.GetUID()})
	cached := response.DeepCopyObject().(Object)
	if err := c.upstream.Get(ctx, key, cached); err == nil {
		o.forgetIfObserved(key, cached)
	}
	return nil
}

// transformed returns a copy of the write response with the cache's transform applied
func (c *overlayClient) transformed(obj Object) (Object, error) {
	copied := obj.DeepCopyObject()
	if c.transform == nil {
		return copied.(Object), nil
	}
	out, err := c.transform(copied)
	if err != nil {
		return nil, fmt.Errorf("failed to transform %T: %w", obj, err)
	}
	transformed, ok := out.(Object)
	if !ok {
		return nil, fmt.Errorf("transform returned %T, which does not implement client.Object", out)
	}
	return transformed, nil
}

func (c *overlayClient) Create(ctx context.Context, obj Object, opts ...CreateOption) error {
	if err := c.upstream.Create(ctx, obj, opts...); err != nil {
		return err
	}
	if (&CreateOptions{}).ApplyOptions(opts).DisableReadYourWritesConsistency || isDryRun((&CreateOptions{}).ApplyOptions(opts).DryRun) {
		return nil
	}
	return c.recordWrite(ctx, obj)
}

func (c *overlayClient) Update(ctx context.Context, obj Object, opts ...UpdateOption) error {
	if err := c.upstream.Update(ctx, obj, opts...); err != nil {
		return err
	}
	if (&UpdateOptions{}).ApplyOptions(opts).DisableReadYourWritesConsistency || isDryRun((&UpdateOptions{}).ApplyOptions(opts).DryRun) {
		return nil
	}
	return c.recordWrite(ctx, obj)
}

func (c *overlayClient) Patch(ctx context.Context, obj Object, patch Patch, opts ...PatchOption) error {
	if err := c.upstream.Patch(ctx, obj, patch, opts...); err != nil {
		return err
	}
	if (&PatchOptions{}).ApplyOptions(opts).DisableReadYourWritesConsistency || isDryRun((&PatchOptions{}).ApplyOptions(opts).DryRun) {
		return nil
	}
	return c.recordWrite(ctx, obj)
}

func (c *overlayClient) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...ApplyOption) error {
	if err := c.upstream.Apply(ctx, obj, opts...); err != nil {
		return err
	}
	if (&ApplyOptions{}).ApplyOptions(opts).DisableReadYourWritesConsistency || isDryRun((&ApplyOptions{}).ApplyOptions(opts).DryRun) {
		return nil
	}
	return c.recordWrite(ctx, obj)
}

func (c *overlayClient) Delete(ctx context.Context, obj Object, opts ...DeleteOption) error {
	deleteOptions := (&DeleteOptions{}).ApplyOptions(opts)
	if deleteOptions.DisableReadYourWritesConsistency || isDryRun(deleteOptions.DryRun) {
		return c.upstream.Delete(ctx, obj, opts...)
	}
	gvk, err := apiutil.GVKForObject(obj, c.upstream.Scheme())
	if err != nil {
		return fmt.Errorf("failed to get GVK for object %T: %w", obj, err)
	}
	target := gvkAndRepresentation{gvk: gvk, representation: representationIDForObj(obj)}
	key := ObjectKeyFromObject(obj)
	uid := obj.GetUID()
	if p := deleteOptions.Preconditions; p != nil && p.UID != nil && *p.UID != "" {
		uid = *p.UID
	}
	if uid == "" {
		// Without the uid, a tombstone could hide an object created again with the same name
		existing := obj.DeepCopyObject().(Object)
		if err := c.Get(ctx, key, existing); err == nil {
			uid = existing.GetUID()
		}
	}
	o, err := c.overlayFor(ctx, target, obj)
	if err != nil {
		return err
	}

	response, err := c.upstream.delete(ctx, obj, opts...)
	if err != nil {
		return err
	}
	// The response holds the object if it is still in storage, for example because of a finalizer, and a Status
	// otherwise. The typed client's response carries no kind.
	if response != nil && response.GetKind() != "Status" && response.GetResourceVersion() != "" && response.GetName() == key.Name {
		remaining := obj.DeepCopyObject().(Object)
		if err := fromUnstructured(response, remaining); err != nil {
			return err
		}
		transformed, err := c.transformed(remaining)
		if err != nil {
			return err
		}
		o.record(key, overlayEntry{obj: transformed, uid: transformed.GetUID()})
		return nil
	}
	if uid != "" {
		o.record(key, overlayEntry{deleted: true, uid: uid})
	}
	return nil
}

func fromUnstructured(u *unstructured.Unstructured, into Object) error {
	if target, ok := into.(*unstructured.Unstructured); ok {
		u.DeepCopyInto(target)
		return nil
	}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, into); err != nil {
		return fmt.Errorf("failed to convert delete response to %T: %w", into, err)
	}
	return nil
}

func (c *overlayClient) DeleteAllOf(ctx context.Context, obj Object, opts ...DeleteAllOfOption) error {
	// The response doesn't say which objects were deleted, so reads only see the deletes once the cache does
	return c.upstream.DeleteAllOf(ctx, obj, opts...)
}

func (c *overlayClient) Get(ctx context.Context, key ObjectKey, obj Object, opts ...GetOption) error {
	if (&GetOptions{}).ApplyOptions(opts).DisableReadYourWritesConsistency {
		return c.upstream.Get(ctx, key, obj, opts...)
	}
	gvk, err := apiutil.GVKForObject(obj, c.upstream.Scheme())
	if err != nil {
		return fmt.Errorf("failed to get GVK for object %T: %w", obj, err)
	}
	o, err := c.overlayFor(ctx, gvkAndRepresentation{gvk: gvk, representation: representationIDForObj(obj)}, obj)
	if err != nil {
		return err
	}
	// Read the overlay before the cache. An entry evicted between the two reads is in the cache by the time it is read.
	entry, ok := o.get(key)
	err = c.upstream.Get(ctx, key, obj, opts...)
	if err != nil && !apierrors.IsNotFound(err) {
		return err
	}
	if !ok {
		return err
	}
	var cached Object
	if err == nil {
		cached = obj
	}
	switch result := served(cached, entry); {
	case result == nil:
		return apierrors.NewNotFound(c.groupResource(gvk), key.Name)
	case result != cached:
		reflect.ValueOf(obj).Elem().Set(reflect.ValueOf(result.DeepCopyObject()).Elem())
	}
	return nil
}

// groupResource returns the resource of the kind for NotFound errors, falling back to the kind if it can't be mapped
func (c *overlayClient) groupResource(gvk schema.GroupVersionKind) schema.GroupResource {
	if mapping, err := c.upstream.RESTMapper().RESTMapping(gvk.GroupKind(), gvk.Version); err == nil {
		return mapping.Resource.GroupResource()
	}
	return schema.GroupResource{Group: gvk.Group, Resource: strings.ToLower(gvk.Kind)}
}

func (c *overlayClient) List(ctx context.Context, list ObjectList, opts ...ListOption) error {
	listOpts := (&ListOptions{}).ApplyOptions(opts)
	if listOpts.DisableReadYourWritesConsistency {
		return c.upstream.List(ctx, list, opts...)
	}
	gvk, err := apiutil.GVKForObject(list, c.upstream.Scheme())
	if err != nil {
		return fmt.Errorf("failed to get GVK for list %T: %w", list, err)
	}
	gvk.Kind = strings.TrimSuffix(gvk.Kind, "List")
	item, err := newObjectFor(list, gvk, c.upstream.Scheme())
	if err != nil {
		return err
	}
	o, err := c.overlayFor(ctx, gvkAndRepresentation{gvk: gvk, representation: representationIDForObj(list)}, item)
	if err != nil {
		return err
	}
	// Snapshot the overlay before reading the cache, for the same reason as Get
	entries := o.snapshot()
	if err := c.upstream.List(ctx, list, opts...); err != nil {
		return err
	}
	if len(entries) == 0 {
		return nil
	}

	items, err := meta.ExtractList(list)
	if err != nil {
		return err
	}
	selector := listOpts.LabelSelector
	if selector == nil {
		selector = labels.Everything()
	}
	merged := make([]runtime.Object, 0, len(items))
	listed := map[types.NamespacedName]bool{}
	for _, raw := range items {
		cached := raw.(Object)
		key := ObjectKeyFromObject(cached)
		listed[key] = true
		entry, ok := entries[key]
		if !ok {
			merged = append(merged, raw)
			continue
		}
		if result := served(cached, entry); result != nil && selector.Matches(labels.Set(result.GetLabels())) {
			merged = append(merged, result.DeepCopyObject())
		}
	}
	// Objects the cache doesn't hold yet. A field selector can't be evaluated against them, so they are only added to
	// lists without one.
	if listOpts.FieldSelector == nil || listOpts.FieldSelector.Empty() {
		for key, entry := range entries {
			if listed[key] || entry.deleted || (listOpts.Namespace != "" && listOpts.Namespace != key.Namespace) || !selector.Matches(labels.Set(entry.obj.GetLabels())) {
				continue
			}
			merged = append(merged, entry.obj.DeepCopyObject())
		}
	}
	return meta.SetList(list, merged)
}

// newObjectFor returns an empty object of the list's kind and representation
func newObjectFor(list ObjectList, gvk schema.GroupVersionKind, scheme *runtime.Scheme) (Object, error) {
	switch list.(type) {
	case *unstructured.UnstructuredList:
		u := &unstructured.Unstructured{}
		u.SetGroupVersionKind(gvk)
		return u, nil
	case *metav1.PartialObjectMetadataList:
		m := &metav1.PartialObjectMetadata{}
		m.SetGroupVersionKind(gvk)
		return m, nil
	}
	raw, err := scheme.New(gvk)
	if err != nil {
		return nil, fmt.Errorf("failed to create object for GVK %s: %w", gvk, err)
	}
	obj, ok := raw.(Object)
	if !ok {
		return nil, fmt.Errorf("object of type %T for GVK %s does not implement Object", raw, gvk)
	}
	return obj, nil
}

func isDryRun(dryRun []string) bool {
	return len(dryRun) > 0
}

func (c *overlayClient) Status() SubResourceWriter {
	return c.SubResource("status")
}

func (c *overlayClient) SubResource(subResource string) SubResourceClient {
	return &overlaySubResourceClient{SubResourceClient: c.upstream.SubResource(subResource), parent: c}
}

func (c *overlayClient) Scheme() *runtime.Scheme { return c.upstream.Scheme() }

func (c *overlayClient) RESTMapper() meta.RESTMapper { return c.upstream.RESTMapper() }

func (c *overlayClient) GroupVersionKindFor(obj runtime.Object) (schema.GroupVersionKind, error) {
	return c.upstream.GroupVersionKindFor(obj)
}

func (c *overlayClient) IsObjectNamespaced(obj runtime.Object) (bool, error) {
	return c.upstream.IsObjectNamespaced(obj)
}

// overlaySubResourceClient records the parent object returned by subresource updates and patches, such as to status.
// The response to a subresource create, such as an eviction, is a different kind, so it isn't recorded.
type overlaySubResourceClient struct {
	SubResourceClient
	parent *overlayClient
}

func (s *overlaySubResourceClient) Update(ctx context.Context, obj Object, opts ...SubResourceUpdateOption) error {
	if err := s.SubResourceClient.Update(ctx, obj, opts...); err != nil {
		return err
	}
	// With a SubResourceBody, the response is decoded into the body rather than the object
	updateOpts := (&SubResourceUpdateOptions{}).ApplyOptions(opts)
	if updateOpts.DisableReadYourWritesConsistency || isDryRun(updateOpts.DryRun) || updateOpts.SubResourceBody != nil {
		return nil
	}
	return s.parent.recordWrite(ctx, obj)
}

func (s *overlaySubResourceClient) Patch(ctx context.Context, obj Object, patch Patch, opts ...SubResourcePatchOption) error {
	if err := s.SubResourceClient.Patch(ctx, obj, patch, opts...); err != nil {
		return err
	}
	patchOpts := (&SubResourcePatchOptions{}).ApplyOptions(opts)
	if patchOpts.DisableReadYourWritesConsistency || isDryRun(patchOpts.DryRun) || patchOpts.SubResourceBody != nil {
		return nil
	}
	return s.parent.recordWrite(ctx, obj)
}

func (s *overlaySubResourceClient) Apply(ctx context.Context, obj runtime.ApplyConfiguration, opts ...SubResourceApplyOption) error {
	if err := s.SubResourceClient.Apply(ctx, obj, opts...); err != nil {
		return err
	}
	applyOpts := (&SubResourceApplyOptions{}).ApplyOpts(opts)
	if applyOpts.DisableReadYourWritesConsistency || isDryRun(applyOpts.DryRun) || applyOpts.SubResourceBody != nil {
		return nil
	}
	return s.parent.recordWrite(ctx, obj)
}
