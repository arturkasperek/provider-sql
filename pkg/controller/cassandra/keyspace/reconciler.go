/*
Copyright 2020 The Crossplane Authors.

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

package keyspace

import (
	"context"
	"strings"
	"strconv"

	"github.com/crossplane-contrib/provider-sql/apis/cassandra/v1alpha1"
	"github.com/crossplane-contrib/provider-sql/pkg/clients/cassandra"
	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	xpcontroller "github.com/crossplane/crossplane-runtime/pkg/controller"
	"github.com/crossplane/crossplane-runtime/pkg/event"
	"github.com/crossplane/crossplane-runtime/pkg/meta"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/pkg/resource"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
)

const (
	errTrackPCUsage = "cannot track ProviderConfig usage"
	errGetPC        = "cannot get ProviderConfig"
	errNoSecretRef  = "ProviderConfig does not reference a credentials Secret"
	errGetSecret    = "cannot get credentials Secret"
	errNotKeyspace  = "managed resource is not a Keyspace custom resource"
	errSelectKeyspace = "cannot select keyspace"
	errCreateKeyspace = "cannot create keyspace"
	errDropKeyspace   = "cannot drop keyspace"
	maxConcurrency  = 5
	defaultStrategy = "SimpleStrategy"
	defaultReplicas = 1
)

// Setup adds a controller that reconciles Keyspace managed resources.
func Setup(mgr ctrl.Manager, o xpcontroller.Options) error {
	name := managed.ControllerName(v1alpha1.KeyspaceGroupKind)

	t := resource.NewProviderConfigUsageTracker(mgr.GetClient(), &v1alpha1.ProviderConfigUsage{})
	r := managed.NewReconciler(mgr,
		resource.ManagedKind(v1alpha1.KeyspaceGroupVersionKind),
		managed.WithExternalConnecter(&connector{kube: mgr.GetClient(), usage: t, newClient: cassandra.New}),
		managed.WithLogger(o.Logger.WithValues("controller", name)),
		managed.WithPollInterval(o.PollInterval),
		managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))))

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		For(&v1alpha1.Keyspace{}).
		WithOptions(controller.Options{
			MaxConcurrentReconciles: maxConcurrency,
		}).
		Complete(r)
}

type connector struct {
	kube      client.Client
	usage     resource.Tracker
	newClient func(creds map[string][]byte, keyspace string) *cassandra.CassandraDB
}

func (c *connector) Connect(ctx context.Context, mg resource.Managed) (managed.ExternalClient, error) {
	cr, ok := mg.(*v1alpha1.Keyspace)
	if !ok {
		return nil, errors.New(errNotKeyspace)
	}

	if err := c.usage.Track(ctx, mg); err != nil {
		return nil, errors.Wrap(err, errTrackPCUsage)
	}

	pc := &v1alpha1.ProviderConfig{}
	if err := c.kube.Get(ctx, types.NamespacedName{Name: cr.GetProviderConfigReference().Name}, pc); err != nil {
		return nil, errors.Wrap(err, errGetPC)
	}

	ref := pc.Spec.Credentials.ConnectionSecretRef
	if ref == nil {
		return nil, errors.New(errNoSecretRef)
	}

	s := &corev1.Secret{}
	if err := c.kube.Get(ctx, types.NamespacedName{Namespace: ref.Namespace, Name: ref.Name}, s); err != nil {
		return nil, errors.Wrap(err, errGetSecret)
	}

	db := c.newClient(s.Data, "")
	return &external{db: db}, nil
}

type external struct {
	db *cassandra.CassandraDB
}

func (c *external) Observe(ctx context.Context, mg resource.Managed) (managed.ExternalObservation, error) {
	cr, ok := mg.(*v1alpha1.Keyspace)
	if !ok {
		return managed.ExternalObservation{}, errors.New(errNotKeyspace)
	}

	observed := &v1alpha1.KeyspaceParameters{
		ReplicationClass:   new(string),
		ReplicationFactor:  new(int),
		DurableWrites:      new(bool),
	}

	query := "SELECT replication, durable_writes FROM system_schema.keyspaces WHERE keyspace_name = ?"
	iter, err := c.db.Query(ctx, query, meta.GetExternalName(cr))
	if err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, errSelectKeyspace)
	}
	defer iter.Close()

	replicationMap := map[string]string{}
	if !iter.Scan(&replicationMap, &observed.DurableWrites) {
		return managed.ExternalObservation{}, errors.New("failed to scan keyspace attributes")
	}

	if rc, ok := replicationMap["class"]; ok {
		// Remove Cassandra prefix if present.
		if strings.HasPrefix(rc, "org.apache.cassandra.locator.") {
			rc = strings.TrimPrefix(rc, "org.apache.cassandra.locator.")
		}
		*observed.ReplicationClass = rc
	}
	if rf, ok := replicationMap["replication_factor"]; ok {
		rfInt, _ := strconv.Atoi(rf)
		*observed.ReplicationFactor = rfInt
	}

	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceLateInitialized: lateInit(observed, &cr.Spec.ForProvider),
		ResourceUpToDate:        upToDate(observed, &cr.Spec.ForProvider),
	}, nil
}

func (c *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) {
	cr, ok := mg.(*v1alpha1.Keyspace)
	if !ok {
		return managed.ExternalCreation{}, errors.New(errNotKeyspace)
	}

	params := cr.Spec.ForProvider
	strategy := defaultStrategy
	if params.ReplicationClass != nil {
		strategy = *params.ReplicationClass
	}

	replicationFactor := defaultReplicas
	if params.ReplicationFactor != nil {
		replicationFactor = *params.ReplicationFactor
	}

	durableWrites := true
	if params.DurableWrites != nil {
		durableWrites = *params.DurableWrites
	}

	query := "CREATE KEYSPACE IF NOT EXISTS " + cassandra.QuoteIdentifier(meta.GetExternalName(cr)) +
		" WITH replication = {'class': '" + strategy + "', 'replication_factor': " + strconv.Itoa(replicationFactor) + "} AND durable_writes = " + strconv.FormatBool(durableWrites)

	if err := c.db.Exec(ctx, query); err != nil {
		return managed.ExternalCreation{}, errors.New(errCreateKeyspace + ": " + err.Error())
	}

	return managed.ExternalCreation{}, nil
}

func (c *external) Update(ctx context.Context, mg resource.Managed) (managed.ExternalUpdate, error) {
	return managed.ExternalUpdate{}, nil
}

func (c *external) Delete(ctx context.Context, mg resource.Managed) error {
	cr, ok := mg.(*v1alpha1.Keyspace)
	if !ok {
		return errors.New(errNotKeyspace)
	}

	query := "DROP KEYSPACE IF EXISTS " + cassandra.QuoteIdentifier(meta.GetExternalName(cr))
	if err := c.db.Exec(ctx, query); err != nil {
		return errors.New(errDropKeyspace + ": " + err.Error())
	}

	return nil
}

func upToDate(observed *v1alpha1.KeyspaceParameters, desired *v1alpha1.KeyspaceParameters) bool {
	if observed.ReplicationClass == nil || desired.ReplicationClass == nil || *observed.ReplicationClass != *desired.ReplicationClass {
		return false
	}
	if observed.ReplicationFactor == nil || desired.ReplicationFactor == nil || *observed.ReplicationFactor != *desired.ReplicationFactor {
		return false
	}
	if observed.DurableWrites == nil || desired.DurableWrites == nil || *observed.DurableWrites != *desired.DurableWrites {
		return false
	}
	return true
}

func lateInit(observed *v1alpha1.KeyspaceParameters, desired *v1alpha1.KeyspaceParameters) bool {
	li := false

	if desired.ReplicationClass == nil {
		desired.ReplicationClass = observed.ReplicationClass
		li = true
	}
	if desired.ReplicationFactor == nil {
		desired.ReplicationFactor = observed.ReplicationFactor
		li = true
	}
	if desired.DurableWrites == nil {
		desired.DurableWrites = observed.DurableWrites
		li = true
	}

	return li
}
