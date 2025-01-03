package grant

import (
	"context"
	"fmt"
	"testing"

	"github.com/gocql/gocql"
	"github.com/pkg/errors"

	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/pkg/resource"
	"github.com/crossplane/crossplane-runtime/pkg/test"
	"github.com/google/go-cmp/cmp"

	"github.com/crossplane/provider-cassandra/apis/cql/v1alpha1"
	"github.com/crossplane/provider-cassandra/internal/clients/cassandra"
)

func pointerToString(s string) *string {
	return &s
}

func TestObserve(t *testing.T) {
	type fields struct {
		db cassandra.DB
	}

	type args struct {
		ctx context.Context
		mg  resource.Managed
	}

	type want struct {
		o   managed.ExternalObservation
		err error
	}

	called := false
	cases := map[string]struct {
		reason string
		fields fields
		args   args
		want   want
	}{
		"ErrNotGrant": {
			reason: "Should return an error if the managed resource is not a *Grant",
			args: args{
				mg: nil,
			},
			want: want{
				err: errors.New(errNotGrant),
			},
		},
		"GrantNotFound": {
			reason: "Should return ResourceExists: false when the grant does not exist",
			fields: fields{
				db: &cassandra.MockDB{
					QueryFunc: func(ctx context.Context, query string, args ...interface{}) (*gocql.Iter, error) {
						return &gocql.Iter{}, nil
					},
					ScanFunc: func(iter *gocql.Iter, dest ...interface{}) bool { return false },
				},
			},
			args: args{
				mg: &v1alpha1.Grant{
					Spec: v1alpha1.GrantSpec{
						ForProvider: v1alpha1.GrantParameters{
							Role:     pointerToString("example_role"),
							Keyspace: pointerToString("example_keyspace"),
						},
					},
				},
			},
			want: want{
				o: managed.ExternalObservation{
					ResourceExists:   false,
					ResourceUpToDate: true,
				},
			},
		},
		"GrantExists": {
			reason: "Should return ResourceExists: true when the grant exists",
			fields: fields{
				db: &cassandra.MockDB{
					QueryFunc: func(ctx context.Context, query string, args ...interface{}) (*gocql.Iter, error) {
						return &gocql.Iter{}, nil
					},
					ScanFunc: func(iter *gocql.Iter, dest ...interface{}) bool {
						if called {
							return false // Stop after the first iteration
						}
						called = true
						if len(dest) > 0 {
							if permissions, ok := dest[0].(*[]string); ok {
								*permissions = []string{"SELECT", "MODIFY"}
							}
						}
						return true
					},
				},
			},
			args: args{
				mg: &v1alpha1.Grant{
					Spec: v1alpha1.GrantSpec{
						ForProvider: v1alpha1.GrantParameters{
							Role:       pointerToString("example_role"),
							Keyspace:   pointerToString("example_keyspace"),
							Privileges: []v1alpha1.GrantPrivilege{"SELECT", "MODIFY"},
						},
					},
				},
			},
			want: want{
				o: managed.ExternalObservation{
					ResourceExists:   true,
					ResourceUpToDate: true,
				},
				err: nil,
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := external{db: tc.fields.db}
			got, err := e.Observe(tc.args.ctx, tc.args.mg)
			if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
				t.Errorf("\n%s\nObserve(...): -want error, +got error:\n%s\n", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.o, got); diff != "" {
				t.Errorf("\n%s\nObserve(...): -want, +got:\n%s\n", tc.reason, diff)
			}
		})
	}
}

func TestCreate(t *testing.T) {
	errBoom := errors.New("boom")

	type fields struct {
		db cassandra.DB
	}

	type args struct {
		ctx context.Context
		mg  resource.Managed
	}

	type want struct {
		c   managed.ExternalCreation
		err error
	}

	cases := map[string]struct {
		reason string
		fields fields
		args   args
		want   want
	}{
		"ErrNotGrant": {
			reason: "Should return an error if the managed resource is not a *Grant",
			args: args{
				mg: nil,
			},
			want: want{
				err: errors.New(errNotGrant),
			},
		},
		"CreateGrantSuccess": {
			reason: "Should successfully create the grant if the query succeeds",
			fields: fields{
				db: &cassandra.MockDB{
					ExecFunc: func(ctx context.Context, query string, args ...interface{}) error {
						expectedQuery := "GRANT SELECT ON KEYSPACE \"example_keyspace\" TO \"example_role\""
						if query != expectedQuery {
							return fmt.Errorf("unexpected query: got %s, want %s", query, expectedQuery)
						}
						return nil
					},
				},
			},
			args: args{
				mg: &v1alpha1.Grant{
					Spec: v1alpha1.GrantSpec{
						ForProvider: v1alpha1.GrantParameters{
							Role:       pointerToString("example_role"),
							Keyspace:   pointerToString("example_keyspace"),
							Privileges: []v1alpha1.GrantPrivilege{"SELECT"},
						},
					},
				},
			},
			want: want{
				c:   managed.ExternalCreation{},
				err: nil,
			},
		},
		"CreateGrantFailure": {
			reason: "Should return an error if the query fails",
			fields: fields{
				db: &cassandra.MockDB{
					ExecFunc: func(ctx context.Context, query string, args ...interface{}) error {
						return errBoom
					},
				},
			},
			args: args{
				mg: &v1alpha1.Grant{
					Spec: v1alpha1.GrantSpec{
						ForProvider: v1alpha1.GrantParameters{
							Role:       pointerToString("example_role"),
							Keyspace:   pointerToString("example_keyspace"),
							Privileges: []v1alpha1.GrantPrivilege{"SELECT"},
						},
					},
				},
			},
			want: want{
				err: errors.Wrap(errBoom, errGrantCreate),
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := external{db: tc.fields.db}
			got, err := e.Create(tc.args.ctx, tc.args.mg)
			if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
				t.Errorf("\n%s\nCreate(...): -want error, +got error:\n%s\n", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.c, got); diff != "" {
				t.Errorf("\n%s\nCreate(...): -want, +got:\n%s\n", tc.reason, diff)
			}
		})
	}
}

func TestUpdate(t *testing.T) {
	errBoom := errors.New("boom")

	type fields struct {
		db cassandra.DB
	}

	type args struct {
		ctx context.Context
		mg  resource.Managed
	}

	type want struct {
		u   managed.ExternalUpdate
		err error
	}

	cases := map[string]struct {
		reason string
		fields fields
		args   args
		want   want
	}{
		"ErrNotGrant": {
			reason: "Should return an error if the managed resource is not a *Grant",
			args: args{
				mg: nil,
			},
			want: want{
				err: errors.New(errNotGrant),
			},
		},
		"UpdateGrantSuccess": {
			reason: "Should successfully update the grant if the queries succeed",
			fields: fields{
				db: &cassandra.MockDB{
					ExecFunc: func(ctx context.Context, query string, args ...interface{}) error {
						expectedGrantQuery := "GRANT SELECT ON KEYSPACE \"example_keyspace\" TO \"example_role\""
						expectedRevokeQuery := "REVOKE MODIFY ON KEYSPACE \"example_keyspace\" FROM \"example_role\""

						if query == expectedGrantQuery || query == expectedRevokeQuery {
							return nil
						}
						return fmt.Errorf("unexpected query: got %s", query)
					},
				},
			},
			args: args{
				mg: &v1alpha1.Grant{
					Spec: v1alpha1.GrantSpec{
						ForProvider: v1alpha1.GrantParameters{
							Role:       pointerToString("example_role"),
							Keyspace:   pointerToString("example_keyspace"),
							Privileges: []v1alpha1.GrantPrivilege{"SELECT"},
						},
					},
					Status: v1alpha1.GrantStatus{
						AtProvider: v1alpha1.GrantObservation{
							Privileges: []string{"MODIFY"},
						},
					},
				},
			},
			want: want{
				u:   managed.ExternalUpdate{},
				err: nil,
			},
		},
		"UpdateGrantFailure": {
			reason: "Should return an error if any query fails",
			fields: fields{
				db: &cassandra.MockDB{
					ExecFunc: func(ctx context.Context, query string, args ...interface{}) error {
						return errBoom
					},
				},
			},
			args: args{
				mg: &v1alpha1.Grant{
					Spec: v1alpha1.GrantSpec{
						ForProvider: v1alpha1.GrantParameters{
							Role:       pointerToString("example_role"),
							Keyspace:   pointerToString("example_keyspace"),
							Privileges: []v1alpha1.GrantPrivilege{"SELECT"},
						},
					},
				},
			},
			want: want{
				err: errors.Wrap(errBoom, errGrantCreate),
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := external{db: tc.fields.db}
			got, err := e.Update(tc.args.ctx, tc.args.mg)
			if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
				t.Errorf("\n%s\nUpdate(...): -want error, +got error:\n%s\n", tc.reason, diff)
			}
			if diff := cmp.Diff(tc.want.u, got); diff != "" {
				t.Errorf("\n%s\nUpdate(...): -want, +got:\n%s\n", tc.reason, diff)
			}
		})
	}
}
