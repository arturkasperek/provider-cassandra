/*
Copyright 2022 The Crossplane Authors.

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
	"reflect"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"

	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
)

// KeyspaceParameters are the configurable fields of a Keyspace.
type KeyspaceParameters struct {
	// ReplicationClass used for keyspace
	// +kubebuilder:validation:Enum=SimpleStrategy;NetworkTopologyStrategy
	// +optional
	ReplicationClass *string `json:"replicationClass,omitempty"`

	// ReplicationFactor used for keyspace
	// +optional
	ReplicationFactor *int `json:"replicationFactor,omitempty"`

	// Decided if turn on durable writes
	// +optional
	DurableWrites *bool `json:"durableWrites,omitempty"`
}

// KeyspaceObservation are the observable fields of a Keyspace.
type KeyspaceObservation struct {
	ObservableField string `json:"observableField,omitempty"`
}

// A KeyspaceSpec defines the desired state of a Keyspace.
type KeyspaceSpec struct {
	xpv1.ResourceSpec `json:",inline"`
	ForProvider       KeyspaceParameters `json:"forProvider"`
}

// A KeyspaceStatus represents the observed state of a Keyspace.
type KeyspaceStatus struct {
	xpv1.ResourceStatus `json:",inline"`
	AtProvider          KeyspaceObservation `json:"atProvider,omitempty"`
}

// +kubebuilder:object:root=true

// A Keyspace is an example API type.
// +kubebuilder:printcolumn:name="READY",type="string",JSONPath=".status.conditions[?(@.type=='Ready')].status"
// +kubebuilder:printcolumn:name="SYNCED",type="string",JSONPath=".status.conditions[?(@.type=='Synced')].status"
// +kubebuilder:printcolumn:name="EXTERNAL-NAME",type="string",JSONPath=".metadata.annotations.crossplane\\.io/external-name"
// +kubebuilder:printcolumn:name="AGE",type="date",JSONPath=".metadata.creationTimestamp"
// +kubebuilder:subresource:status
// +kubebuilder:resource:scope=Cluster,categories={crossplane,managed,cassandra}
type Keyspace struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   KeyspaceSpec   `json:"spec"`
	Status KeyspaceStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// KeyspaceList contains a list of Keyspace
type KeyspaceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Keyspace `json:"items"`
}

// Keyspace type metadata.
var (
	KeyspaceKind             = reflect.TypeOf(Keyspace{}).Name()
	KeyspaceGroupKind        = schema.GroupKind{Group: Group, Kind: KeyspaceKind}.String()
	KeyspaceKindAPIVersion   = KeyspaceKind + "." + SchemeGroupVersion.String()
	KeyspaceGroupVersionKind = SchemeGroupVersion.WithKind(KeyspaceKind)
)

func init() {
	SchemeBuilder.Register(&Keyspace{}, &KeyspaceList{})
}
