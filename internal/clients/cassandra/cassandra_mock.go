package cassandra

import (
	"context"

	"github.com/gocql/gocql"

	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
)

type MockDB struct {
	ExecFunc                 func(ctx context.Context, query string, args ...interface{}) error
	QueryFunc                func(ctx context.Context, query string, args ...interface{}) (*gocql.Iter, error)
	ScanFunc                 func(iter *gocql.Iter, dest ...interface{}) bool
	CloseFunc                func()
	GetConnectionDetailsFunc func(username, password string) managed.ConnectionDetails
}

// Exec executes a CQL statement.
func (m *MockDB) Exec(ctx context.Context, query string, args ...interface{}) error {
	if m.ExecFunc != nil {
		return m.ExecFunc(ctx, query, args...)
	}
	return nil
}

// Query performs a query and returns an iterator for the results.
func (m *MockDB) Query(ctx context.Context, query string, args ...interface{}) (*gocql.Iter, error) {
	if m.QueryFunc != nil {
		return m.QueryFunc(ctx, query, args...)
	}
	return nil, nil
}

// Scan performs scanning of an iterator.
func (m *MockDB) Scan(iter *gocql.Iter, dest ...interface{}) bool {
	if m.ScanFunc != nil {
		return m.ScanFunc(iter, dest...)
	}
	return false
}

// Close closes the Cassandra session.
func (m *MockDB) Close() {
	if m.CloseFunc != nil {
		m.CloseFunc()
	}
}

// GetConnectionDetails returns the connection details for a user of this DB.
func (m *MockDB) GetConnectionDetails(username, password string) managed.ConnectionDetails {
	if m.GetConnectionDetailsFunc != nil {
		return m.GetConnectionDetailsFunc(username, password)
	}
	return managed.ConnectionDetails{
		xpv1.ResourceCredentialsSecretUserKey:     []byte(username),
		xpv1.ResourceCredentialsSecretPasswordKey: []byte(password),
	}
}
