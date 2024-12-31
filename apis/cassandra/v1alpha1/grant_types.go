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

package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
)

// A GrantSpec defines the desired state of a Grant.
type GrantSpec struct {
	xpv1.ResourceSpec `json:",inline"`
	ForProvider       GrantParameters `json:"forProvider"`
}

// GrantPrivilege represents a privilege to be granted
// +kubebuilder:validation:Enum=ALL_PERMISSIONS;ALTER;AUTHORIZE;CREATE;DESCRIBE;DROP;EXECUTE;MODIFY;SELECT
type GrantPrivilege string

// If Privileges are specified, we should have at least one

// GrantPrivileges is a list of the privileges to be granted
// +kubebuilder:validation:MinItems:=1
type GrantPrivileges []GrantPrivilege

// GrantParameters define the desired state of a PostgreSQL grant instance.
type GrantParameters struct {
	// Privileges to be granted.
	Privileges GrantPrivileges `json:"privileges"`

	// Role this grant is for.
	// +optional
	// +crossplane:generate:reference:type=Role
	Role *string `json:"role,omitempty"`

	// RoleRef references the role object this grant is for.
	// +immutable
	// +optional
	RoleRef *xpv1.Reference `json:"roleRef,omitempty"`

	// RoleSelector selects a reference to a Role this grant is for.
	// +immutable
	// +optional
	RoleSelector *xpv1.Selector `json:"roleSelector,omitempty"`

	// Keyspace this grant is for.
	// +optional
	// +crossplane:generate:reference:type=Keyspace
	Keyspace *string `json:"keyspace,omitempty"`

	// KeyspaceRef references the keyspace object this grant it for.
	// +immutable
	// +optional
	KeyspaceRef *xpv1.Reference `json:"keyspaceRef,omitempty"`

	// KeyspaceSelector selects a reference to a Keyspace this grant is for.
	// +immutable
	// +optional
	KeyspaceSelector *xpv1.Selector `json:"keyspaceSelector,omitempty"`
}

// A GrantStatus represents the observed state of a Grant.
type GrantStatus struct {
	xpv1.ResourceStatus `json:",inline"`
}

// +kubebuilder:object:root=true

// A Grant represents the declarative state of a PostgreSQL grant.
// +kubebuilder:subresource:status
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="SYNCED",type="string",JSONPath=".status.conditions[?(@.type=='Synced')].status"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:printcolumn:name="ROLE",type="string",JSONPath=".spec.forProvider.role"
// +kubebuilder:printcolumn:name="KEYSAPCE",type="string",JSONPath=".spec.forProvider.keyspace"
// +kubebuilder:printcolumn:name="PRIVILEGES",type="string",JSONPath=".spec.forProvider.privileges"
// +kubebuilder:resource:scope=Cluster,categories={crossplane,managed,sql}
type Grant struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   GrantSpec   `json:"spec"`
	Status GrantStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// GrantList contains a list of Grant
type GrantList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Grant `json:"items"`
}
