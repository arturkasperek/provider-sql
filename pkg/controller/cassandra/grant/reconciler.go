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

package grant

import (
	"context"
	"fmt"
	"strings"

	"github.com/crossplane-contrib/provider-sql/apis/cassandra/v1alpha1"
	"github.com/crossplane-contrib/provider-sql/pkg/clients/cassandra"
	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	xpcontroller "github.com/crossplane/crossplane-runtime/pkg/controller"
	"github.com/crossplane/crossplane-runtime/pkg/event"
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
	errNotGrant     = "managed resource is not a Grant custom resource"
	errGrantCreate  = "cannot create grant"
	errGrantDelete  = "cannot delete grant"
	errGrantObserve = "cannot observe grant"
	maxConcurrency  = 5
)

// Setup adds a controller that reconciles Grant managed resources.
func Setup(mgr ctrl.Manager, o xpcontroller.Options) error {
	name := managed.ControllerName(v1alpha1.GrantGroupKind)

	t := resource.NewProviderConfigUsageTracker(mgr.GetClient(), &v1alpha1.ProviderConfigUsage{})
	r := managed.NewReconciler(mgr,
		resource.ManagedKind(v1alpha1.GrantGroupVersionKind),
		managed.WithExternalConnecter(&connector{kube: mgr.GetClient(), usage: t, newClient: cassandra.New}),
		managed.WithLogger(o.Logger.WithValues("controller", name)),
		managed.WithPollInterval(o.PollInterval),
		managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))))

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		For(&v1alpha1.Grant{}).
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
	cr, ok := mg.(*v1alpha1.Grant)
	if !ok {
		return nil, errors.New(errNotGrant)
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
	cr, ok := mg.(*v1alpha1.Grant)
	if !ok {
		return managed.ExternalObservation{}, errors.New(errNotGrant)
	}

	role := *cr.Spec.ForProvider.Role
	keyspace := *cr.Spec.ForProvider.Keyspace

	query := fmt.Sprintf("SELECT permissions FROM system_auth.role_permissions WHERE role = ? AND resource = 'data/%s'", keyspace)
	var permissions []string
	iter, err := c.db.Query(ctx, query, role)
	if err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, errGrantObserve)
	}
	defer iter.Close()

	observedPermissions := make(map[string]bool)
	resourceExists := false
	for iter.Scan(&permissions) {
		for _, p := range permissions {
			observedPermissions[p] = true
		}
	}

	desiredPermissions := make(map[string]bool)
	for _, p := range replaceUnderscoreWithSpace(cr.Spec.ForProvider.Privileges) {
		desiredPermissions[p] = true
	}

	upToDate := true
	for p := range desiredPermissions {
		if !observedPermissions[p] {
			upToDate = false
			break
		} else {
			resourceExists = true
		}
	}

	if resourceExists {
		cr.SetConditions(xpv1.Available())
	}

	return managed.ExternalObservation{
		ResourceExists:          resourceExists,
		ResourceLateInitialized: false,
		ResourceUpToDate:        upToDate,
	}, nil
}

func (c *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) {
	cr, ok := mg.(*v1alpha1.Grant)
	if !ok {
		return managed.ExternalCreation{}, errors.New(errNotGrant)
	}

	role := *cr.Spec.ForProvider.Role
	keyspace := *cr.Spec.ForProvider.Keyspace
	privileges := replaceUnderscoreWithSpace(cr.Spec.ForProvider.Privileges)

	for _, privilege := range privileges {
		query := fmt.Sprintf("GRANT %s ON KEYSPACE %s TO %s", privilege, cassandra.QuoteIdentifier(keyspace), cassandra.QuoteIdentifier(role))
		if err := c.db.Exec(ctx, query); err != nil {
			return managed.ExternalCreation{}, errors.Wrap(err, errGrantCreate)
		}
	}

	return managed.ExternalCreation{}, nil
}

func (c *external) Update(ctx context.Context, mg resource.Managed) (managed.ExternalUpdate, error) {
	cr, ok := mg.(*v1alpha1.Grant)
	if !ok {
		return managed.ExternalUpdate{}, errors.New(errNotGrant)
	}

	role := *cr.Spec.ForProvider.Role
	keyspace := *cr.Spec.ForProvider.Keyspace
	privileges := replaceUnderscoreWithSpace(cr.Spec.ForProvider.Privileges)

	for _, privilege := range privileges {
		query := fmt.Sprintf("GRANT %s ON KEYSPACE %s TO %s", privilege, cassandra.QuoteIdentifier(keyspace), cassandra.QuoteIdentifier(role))
		if err := c.db.Exec(ctx, query); err != nil {
			return managed.ExternalUpdate{}, errors.Wrap(err, errGrantCreate)
		}
	}

	return managed.ExternalUpdate{}, nil
}

func (c *external) Delete(ctx context.Context, mg resource.Managed) error {
	cr, ok := mg.(*v1alpha1.Grant)
	if !ok {
		return errors.New(errNotGrant)
	}

	role := *cr.Spec.ForProvider.Role
	keyspace := *cr.Spec.ForProvider.Keyspace
	privileges := replaceUnderscoreWithSpace(cr.Spec.ForProvider.Privileges)

	for _, privilege := range privileges {
		query := fmt.Sprintf("REVOKE %s ON KEYSPACE %s FROM %s", privilege, cassandra.QuoteIdentifier(keyspace), cassandra.QuoteIdentifier(role))
		if err := c.db.Exec(ctx, query); err != nil {
			return errors.Wrap(err, errGrantDelete)
		}
	}

	return nil
}

func replaceUnderscoreWithSpace(privileges []v1alpha1.GrantPrivilege) []string {
	replaced := make([]string, len(privileges))
	for i, privilege := range privileges {
		replaced[i] = strings.ReplaceAll(string(privilege), "_", " ")
	}
	return replaced
}
