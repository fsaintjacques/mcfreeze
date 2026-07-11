// SPDX-License-Identifier: Apache-2.0

package controller

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/fsaintjacques/mcfreeze/go/api"
	v1alpha1 "github.com/fsaintjacques/mcfreeze/go/api/v1alpha1"
	"github.com/robfig/cron/v3"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/ptr"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// CronRunnable schedules periodic DatasetVersion creations from
// Dataset.Spec.Trigger.Cron entries. It implements both manager.Runnable and
// manager.LeaderElectionRunnable so only the elected leader fires builds.
//
// DatasetReconciler.Reconcile calls Sync(ctx, &ds) on every reconcile of a
// Dataset to keep the in-memory cron entry table in lockstep with the CRD.
// On Start, the runnable performs a one-shot staggered catch-up across all
// known datasets so a control-plane restart does not lose a missed tick.
type CronRunnable struct {
	Client    client.Client
	Namespace string

	// VersionIDFn returns the deterministic VersionID to use for a tick at
	// time t. Defaults to t.UTC().Format("20060102-150405").
	VersionIDFn func(time.Time) string

	mu      sync.Mutex
	cron    *cron.Cron
	entries map[types.NamespacedName]cronEntry
	started bool
}

type cronEntry struct {
	schedule string
	id       cron.EntryID
}

// NeedLeaderElection returns true: only the elected leader fires triggers.
func (c *CronRunnable) NeedLeaderElection() bool { return true }

// Start initializes the cron scheduler, performs a staggered catch-up across
// existing Datasets, and blocks until ctx is cancelled.
func (c *CronRunnable) Start(ctx context.Context) error {
	c.mu.Lock()
	if c.cron == nil {
		c.cron = cron.New()
	}
	if c.entries == nil {
		c.entries = make(map[types.NamespacedName]cronEntry)
	}
	if c.VersionIDFn == nil {
		c.VersionIDFn = func(t time.Time) string { return t.UTC().Format("20060102-150405") }
	}
	c.started = true
	c.cron.Start()
	c.mu.Unlock()

	// Staggered catch-up: list datasets and re-arm any cron triggers.
	// Spread the initial bursts ~1s apart to avoid a thundering herd.
	var datasets v1alpha1.DatasetList
	if err := c.Client.List(ctx, &datasets, client.InNamespace(c.Namespace)); err == nil {
		for i := range datasets.Items {
			ds := &datasets.Items[i]
			_ = c.Sync(ctx, ds)
			// Fire one catch-up build immediately if cron is configured and
			// no version is currently building.
			if ds.Spec.Trigger != nil && ds.Spec.Trigger.Cron != nil {
				go func(idx int, ds v1alpha1.Dataset) {
					select {
					case <-time.After(time.Duration(idx) * time.Second):
					case <-ctx.Done():
						return
					}
					c.fire(context.Background(), &ds)
				}(i, *ds)
			}
		}
	}

	<-ctx.Done()

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cron != nil {
		<-c.cron.Stop().Done()
	}
	return nil
}

// Sync reconciles the cron entry for ds with its current spec. Adds, updates,
// or removes the entry to match. Idempotent.
func (c *CronRunnable) Sync(_ context.Context, ds *v1alpha1.Dataset) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.cron == nil {
		c.cron = cron.New()
	}
	if c.entries == nil {
		c.entries = make(map[types.NamespacedName]cronEntry)
	}

	key := types.NamespacedName{Namespace: ds.Namespace, Name: ds.Name}
	desired := ""
	if ds.Spec.Trigger != nil && ds.Spec.Trigger.Cron != nil {
		desired = ds.Spec.Trigger.Cron.Schedule
	}

	existing, hasExisting := c.entries[key]

	// Removed: drop the entry.
	if desired == "" {
		if hasExisting {
			c.cron.Remove(existing.id)
			delete(c.entries, key)
		}
		return nil
	}

	// Unchanged: no-op.
	if hasExisting && existing.schedule == desired {
		return nil
	}

	// Add or replace.
	if hasExisting {
		c.cron.Remove(existing.id)
		delete(c.entries, key)
	}
	dsCopy := ds.DeepCopy()
	id, err := c.cron.AddFunc(desired, func() {
		c.fire(context.Background(), dsCopy)
	})
	if err != nil {
		return fmt.Errorf("invalid cron schedule %q: %w", desired, err)
	}
	c.entries[key] = cronEntry{schedule: desired, id: id}
	return nil
}

// Forget removes the cron entry for a deleted Dataset. Safe to call when no
// entry exists.
func (c *CronRunnable) Forget(ns, name string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	key := types.NamespacedName{Namespace: ns, Name: name}
	if e, ok := c.entries[key]; ok && c.cron != nil {
		c.cron.Remove(e.id)
		delete(c.entries, key)
	}
}

// fire creates a new DatasetVersion CR for ds, skipping if a building version
// already exists. Errors are logged.
func (c *CronRunnable) fire(ctx context.Context, ds *v1alpha1.Dataset) {
	// Skip if any sibling is currently building.
	var siblings v1alpha1.DatasetVersionList
	if err := c.Client.List(ctx, &siblings,
		client.InNamespace(ds.Namespace),
		client.MatchingLabels{v1alpha1.DatasetLabel: ds.Name},
	); err != nil {
		slog.Error("cron: list siblings", "dataset", ds.Name, "err", err)
		return
	}
	for i := range siblings.Items {
		if siblings.Items[i].Status.State == string(api.StateBuilding) {
			slog.Info("cron: skip; build already in flight", "dataset", ds.Name)
			return
		}
	}

	versionID := c.VersionIDFn(time.Now())
	v := &v1alpha1.DatasetVersion{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: ds.Namespace,
			Name:      v1alpha1.VersionCRName(ds.Name, versionID),
			Labels:    map[string]string{v1alpha1.DatasetLabel: ds.Name},
			OwnerReferences: []metav1.OwnerReference{{
				APIVersion:         v1alpha1.GroupVersion.String(),
				Kind:               "Dataset",
				Name:               ds.Name,
				UID:                ds.UID,
				Controller:         ptr.To(true),
				BlockOwnerDeletion: ptr.To(true),
			}},
		},
		Spec: v1alpha1.DatasetVersionSpec{
			Dataset:    ds.Name,
			VersionID:  versionID,
			ShardCount: ds.Spec.ShardCount,
		},
	}
	if err := c.Client.Create(ctx, v); err != nil && !apierrors.IsAlreadyExists(err) {
		slog.Error("cron: create version", "dataset", ds.Name, "version", versionID, "err", err)
		return
	}
	slog.Info("cron: created version", "dataset", ds.Name, "version", versionID)
}
