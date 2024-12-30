/*
Copyright 2021 The Crossplane Authors.

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

package database

import (
	"context"
	"github.com/pkg/errors"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	xpcontroller "github.com/crossplane/crossplane-runtime/pkg/controller"
	"github.com/crossplane/crossplane-runtime/pkg/event"
	"github.com/crossplane/crossplane-runtime/pkg/meta"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/pkg/resource"
	"github.com/crossplane-contrib/provider-sql/apis/cassandra/v1alpha1"
	"github.com/crossplane-contrib/provider-sql/pkg/clients/cassandra"
)

const (
	errTrackPCUsage = "cannot track ProviderConfig usage"
	errGetPC        = "cannot get ProviderConfig"
	errNoSecretRef  = "ProviderConfig does not reference a credentials Secret"
	errGetSecret    = "cannot get credentials Secret"
	errNotDatabase  = "managed resource is not a Database custom resource"
	errSelectDB     = "cannot select keyspace"
	errCreateDB     = "cannot create keyspace"
	errDropDB       = "cannot drop keyspace"
	maxConcurrency  = 5
)

// Setup adds a controller that reconciles Database managed resources.
func Setup(mgr ctrl.Manager, o xpcontroller.Options) error {
	name := managed.ControllerName(v1alpha1.DatabaseGroupKind)

	t := resource.NewProviderConfigUsageTracker(mgr.GetClient(), &v1alpha1.ProviderConfigUsage{})
	r := managed.NewReconciler(mgr,
		resource.ManagedKind(v1alpha1.DatabaseGroupVersionKind),
		managed.WithExternalConnecter(&connector{kube: mgr.GetClient(), usage: t, newClient: cassandra.New}),
		managed.WithLogger(o.Logger.WithValues("controller", name)),
		managed.WithPollInterval(o.PollInterval),
		managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))))

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		For(&v1alpha1.Database{}).
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
	cr, ok := mg.(*v1alpha1.Database)
	if !ok {
		return nil, errors.New(errNotDatabase)
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
	cr, ok := mg.(*v1alpha1.Database)
	if !ok {
		return managed.ExternalObservation{}, errors.New(errNotDatabase)
	}

	iter, err := c.db.Query(ctx, "SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = ?", meta.GetExternalName(cr))
	if err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, "failed to query keyspaces")
	}
	defer iter.Close()

	exists := iter.NumRows() > 0
	if exists {
		cr.SetConditions(xpv1.Available())
		return managed.ExternalObservation{
			ResourceExists:          true,
			ResourceLateInitialized: false,
			ResourceUpToDate:        true,
		}, nil
	}

	return managed.ExternalObservation{ResourceExists: false}, nil
}

func (c *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) {
	cr, ok := mg.(*v1alpha1.Database)
	if !ok {
		return managed.ExternalCreation{}, errors.New(errNotDatabase)
	}

	query := "CREATE KEYSPACE IF NOT EXISTS " + cassandra.QuoteIdentifier(meta.GetExternalName(cr)) + " WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}"
	if err := c.db.Exec(ctx, query); err != nil {
		return managed.ExternalCreation{}, errors.New(errCreateDB + ": " + err.Error())
	}

	return managed.ExternalCreation{}, nil
}

func (c *external) Update(_ context.Context, _ resource.Managed) (managed.ExternalUpdate, error) {
	return managed.ExternalUpdate{}, nil
}

func (c *external) Delete(ctx context.Context, mg resource.Managed) error {
	cr, ok := mg.(*v1alpha1.Database)
	if !ok {
		return errors.New(errNotDatabase)
	}

	query := "DROP KEYSPACE IF EXISTS " + cassandra.QuoteIdentifier(meta.GetExternalName(cr))
	if err := c.db.Exec(ctx, query); err != nil {
		return errors.New(errDropDB + ": " + err.Error())
	}

	return nil
}
