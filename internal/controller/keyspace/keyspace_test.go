package keyspace

import (
	"context"
	"github.com/pkg/errors"
	"testing"
	"github.com/gocql/gocql"

	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
	"github.com/crossplane/crossplane-runtime/pkg/resource"
	"github.com/crossplane/crossplane-runtime/pkg/test"
	"github.com/google/go-cmp/cmp"

	"github.com/crossplane/provider-cassandra/apis/cql/v1alpha1"
	"github.com/crossplane/provider-cassandra/internal/clients/cassandra"
)

func TestConnect(t *testing.T) {
	errBoom := errors.New("boom")

	type fields struct {
			kube      resource.ClientApplicator
			usage     resource.Tracker
			newClient func(creds map[string][]byte, keyspace string) cassandra.DB
		}

	type args struct {
			ctx context.Context
			mg  resource.Managed
		}

	type want struct {
			client managed.ExternalClient
			err    error
		}

	cases := map[string]struct {
		reason string
		fields fields
		args   args
		want   want
	}{
		"ErrNotKeyspace": {
			reason: "Should return an error when the managed resource is not a *Keyspace",
			args: args{
				mg: nil,
			},
			want: want{
				err: errors.New(errNotKeyspace),
			},
		},
		"ErrTrackPCUsage": {
			reason: "Should return an error when tracking provider config usage fails",
			fields: fields{
				usage: resource.TrackerFn(func(ctx context.Context, mg resource.Managed) error { return errBoom }),
			},
			args: args{
				mg: &v1alpha1.Keyspace{},
			},
			want: want{
				err: errors.Wrap(errBoom, errTrackPCUsage),
			},
		},
	}

	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			c := &connector{
				kube:      tc.fields.kube,
				usage:     tc.fields.usage,
				newClient: tc.fields.newClient,
			}
			_, err := c.Connect(tc.args.ctx, tc.args.mg)
			if diff := cmp.Diff(tc.want.err, err, test.EquateErrors()); diff != "" {
				t.Errorf("\n%s\nConnect(...): -want error, +got error:\n%s\n", tc.reason, diff)
			}
		})
	}
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

	cases := map[string]struct {
		reason string
		fields fields
		args   args
		want   want
	}{
		"ErrNotKeyspace": {
			reason: "Should return an error if the managed resource is not a *Keyspace",
			args: args{
				mg: nil,
			},
			want: want{
				err: errors.New(errNotKeyspace),
			},
		},
		"KeyspaceNotFound": {
			reason: "Should return ResourceExists: false when the keyspace does not exist",
			fields: fields{
				db: &cassandra.MockDB{
					QueryFunc: func(ctx context.Context, query string, args ...interface{}) (*gocql.Iter, error) {
						return nil, nil
					},
					ScanFunc: func(iter *gocql.Iter, dest ...interface{}) bool { return false },
				},
			},
			args: args{
				mg: &v1alpha1.Keyspace{},
			},
			want: want{
				o: managed.ExternalObservation{
					ResourceExists: false,
				},
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

// Additional test functions (Create, Update, Delete) can be written following a similar structure.