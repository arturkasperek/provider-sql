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

package role

import (
	"context"
	"fmt"

	"github.com/crossplane-contrib/provider-sql/apis/cassandra/v1alpha1"
	"github.com/crossplane-contrib/provider-sql/pkg/clients/cassandra"
	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	xpcontroller "github.com/crossplane/crossplane-runtime/pkg/controller"
	"github.com/crossplane/crossplane-runtime/pkg/event"
	"github.com/crossplane/crossplane-runtime/pkg/meta"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/pkg/password"
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
	errNotRole      = "managed resource is not a Role custom resource"
	errSelectRole   = "cannot select role"
	errCreateRole   = "cannot create role"
	errUpdateRole   = "cannot update role"
	errDropRole     = "cannot drop role"
	maxConcurrency  = 5
)

// Setup adds a controller that reconciles Role managed resources.
func Setup(mgr ctrl.Manager, o xpcontroller.Options) error {
	name := managed.ControllerName(v1alpha1.RoleGroupKind)

	t := resource.NewProviderConfigUsageTracker(mgr.GetClient(), &v1alpha1.ProviderConfigUsage{})
	r := managed.NewReconciler(mgr,
		resource.ManagedKind(v1alpha1.RoleGroupVersionKind),
		managed.WithExternalConnecter(&connector{kube: mgr.GetClient(), usage: t, newClient: cassandra.New}),
		managed.WithLogger(o.Logger.WithValues("controller", name)),
		managed.WithPollInterval(o.PollInterval),
		managed.WithRecorder(event.NewAPIRecorder(mgr.GetEventRecorderFor(name))))

	return ctrl.NewControllerManagedBy(mgr).
		Named(name).
		For(&v1alpha1.Role{}).
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
	cr, ok := mg.(*v1alpha1.Role)
	if !ok {
		return nil, errors.New(errNotRole)
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
	cr, ok := mg.(*v1alpha1.Role)
	if !ok {
		return managed.ExternalObservation{}, errors.New(errNotRole)
	}

	query := "SELECT is_superuser, can_login FROM system_auth.roles WHERE role = ?"
	var isSuperuser, canLogin bool
	iter, err := c.db.Query(ctx, query, meta.GetExternalName(cr))
	if err != nil {
		return managed.ExternalObservation{}, errors.Wrap(err, errSelectRole)
	}
	defer iter.Close()

	if !iter.Scan(&isSuperuser, &canLogin) {
		return managed.ExternalObservation{
			ResourceExists: false,
			ResourceUpToDate: false,
		}, nil
	}

	observed := &v1alpha1.RoleParameters{
		Privileges: v1alpha1.RolePrivilege{
			SuperUser: &isSuperuser,
			Login:     &canLogin,
		},
	}

	cr.SetConditions(xpv1.Available())

	return managed.ExternalObservation{
		ResourceExists:          true,
		ResourceLateInitialized: lateInit(observed, &cr.Spec.ForProvider),
		ResourceUpToDate:        upToDate(observed, &cr.Spec.ForProvider),
	}, nil
}

func (c *external) Create(ctx context.Context, mg resource.Managed) (managed.ExternalCreation, error) {
	cr, ok := mg.(*v1alpha1.Role)
	if !ok {
		return managed.ExternalCreation{}, errors.New(errNotRole)
	}

	pw, err := password.Generate()
	if err != nil {
		return managed.ExternalCreation{}, err
	}

	params := cr.Spec.ForProvider
	query := fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s WITH SUPERUSER = %t AND LOGIN = %t AND PASSWORD = '%s'", 
		cassandra.QuoteIdentifier(meta.GetExternalName(cr)), 
		params.Privileges.SuperUser != nil && *params.Privileges.SuperUser, 
		params.Privileges.Login != nil && *params.Privileges.Login, 
		pw)

	if err := c.db.Exec(ctx, query); err != nil {
		return managed.ExternalCreation{}, errors.New(errCreateRole + ": " + err.Error())
	}

	connectionDetails := c.db.GetConnectionDetails(meta.GetExternalName(cr), pw)

	return managed.ExternalCreation{
		ConnectionDetails: connectionDetails,
	}, nil
}

func (c *external) Update(ctx context.Context, mg resource.Managed) (managed.ExternalUpdate, error) {
	cr, ok := mg.(*v1alpha1.Role)
	if !ok {
		return managed.ExternalUpdate{}, errors.New(errNotRole)
	}

	params := cr.Spec.ForProvider
	query := fmt.Sprintf("ALTER ROLE %s WITH SUPERUSER = %t AND LOGIN = %t", 
		cassandra.QuoteIdentifier(meta.GetExternalName(cr)), 
		params.Privileges.SuperUser != nil && *params.Privileges.SuperUser, 
		params.Privileges.Login != nil && *params.Privileges.Login)

	if err := c.db.Exec(ctx, query); err != nil {
		return managed.ExternalUpdate{}, errors.New(errUpdateRole + ": " + err.Error())
	}

	return managed.ExternalUpdate{}, nil
}

func (c *external) Delete(ctx context.Context, mg resource.Managed) error {
	cr, ok := mg.(*v1alpha1.Role)
	if !ok {
		return errors.New(errNotRole)
	}

	query := fmt.Sprintf("DROP ROLE IF EXISTS %s", cassandra.QuoteIdentifier(meta.GetExternalName(cr)))
	if err := c.db.Exec(ctx, query); err != nil {
		return errors.New(errDropRole + ": " + err.Error())
	}

	return nil
}

func upToDate(observed *v1alpha1.RoleParameters, desired *v1alpha1.RoleParameters) bool {
	if observed.Privileges.SuperUser == nil || desired.Privileges.SuperUser == nil || *observed.Privileges.SuperUser != *desired.Privileges.SuperUser {
		return false
	}
	if observed.Privileges.Login == nil || desired.Privileges.Login == nil || *observed.Privileges.Login != *desired.Privileges.Login {
		return false
	}
	return true
}

func lateInit(observed *v1alpha1.RoleParameters, desired *v1alpha1.RoleParameters) bool {
	li := false

	if desired.Privileges.SuperUser == nil {
		desired.Privileges.SuperUser = observed.Privileges.SuperUser
		li = true
	}
	if desired.Privileges.Login == nil {
		desired.Privileges.Login = observed.Privileges.Login
		li = true
	}

	return li
}
