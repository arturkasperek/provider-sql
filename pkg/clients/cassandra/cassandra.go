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

package cassandra

import (
	"context"
	"strconv"
	"strings"

	"github.com/gocql/gocql"

	xpv1 "github.com/crossplane/crossplane-runtime/apis/common/v1"
	"github.com/crossplane/crossplane-runtime/pkg/reconciler/managed"
)

const (
	defaultCassandraPort = 9042
)

type CassandraDB struct {
	session  *gocql.Session
	endpoint string
	port     string
}

// New initializes a new Cassandra client.
func New(creds map[string][]byte, keyspace string) *CassandraDB {
	cluster := gocql.NewCluster("localhost:9042")

	port := string(creds[xpv1.ResourceCredentialsSecretPortKey])
	if port != "" {
		cluster.Port = parsePort(port)
	}

	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: string(creds[xpv1.ResourceCredentialsSecretUserKey]),
		Password: string(creds[xpv1.ResourceCredentialsSecretPasswordKey]),
	}

	if keyspace != "" {
		cluster.Keyspace = keyspace
	}

	cluster.Consistency = gocql.Quorum
	session, _ := cluster.CreateSession()

	return &CassandraDB{
		session:  session,
		endpoint: string(creds[xpv1.ResourceCredentialsSecretEndpointKey]),
		port:     port,
	}
}

// Exec executes a CQL statement.
func (c *CassandraDB) Exec(ctx context.Context, query string, args ...interface{}) error {
	return c.session.Query(query, args...).WithContext(ctx).Exec()
}

// Query performs a query and returns an iterator for the results.
func (c *CassandraDB) Query(ctx context.Context, query string, args ...interface{}) *gocql.Iter {
	return c.session.Query(query, args...).WithContext(ctx).Iter()
}

// Close closes the Cassandra session.
func (c *CassandraDB) Close() {
	if c.session != nil {
		c.session.Close()
	}
}

// GetConnectionDetails returns the connection details for a user of this DB.
func (c *CassandraDB) GetConnectionDetails(username, password string) managed.ConnectionDetails {
	return managed.ConnectionDetails{
		xpv1.ResourceCredentialsSecretUserKey:     []byte(username),
		xpv1.ResourceCredentialsSecretPasswordKey: []byte(password),
		xpv1.ResourceCredentialsSecretEndpointKey: []byte(c.endpoint),
		xpv1.ResourceCredentialsSecretPortKey:     []byte(c.port),
	}
}

// Helper function to parse port.
func parsePort(port string) int {
	p, err := strconv.Atoi(port)
	if err != nil {
		return defaultCassandraPort
	}
	return p
}

// QuoteIdentifier safely quotes an identifier to prevent SQL injection.
// Cassandra uses double quotes to delimit identifiers.
func QuoteIdentifier(id string) string {
    return `"` + strings.ReplaceAll(id, `"`, `""`) + `"`
}
