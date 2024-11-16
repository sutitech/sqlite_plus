package database

import (
	"context"
	"fmt"
	"sync/atomic"

	pool "github.com/jolestar/go-commons-pool/v2"
)

type GroupConfiguration struct {
	balancerPolicy  GroupLoadBalancerPolicy
	migrateDatabase SchemaMigrator
}

type Group struct {
	identifier    string
	path          string
	size          int
	configuration *GroupConfiguration
	pool          *pool.ObjectPool
	context       *context.Context
}

func (group *Group) Exec(query string, args ...any) error {
	obj, err := group.pool.BorrowObject(*group.context)
	defer group.pool.ReturnObject(*group.context, obj)

	if err != nil {
		panic(err)
	}

	db := obj.(*Database)

	return db.Exec(query, args...)
}

func newGroup(identifier string, path string, size int, configuration *GroupConfiguration) *Group {
	v := uint64(0)

	factory := pool.NewPooledObjectFactorySimple(
		func(context.Context) (interface{}, error) {
			dbConfig := &DatabaseConfiguration{
				synchronous:           false,
				maxOpenConnections:    1,
				maxIdleConnections:    1,
				connectionMaxLifeTime: 0,
			}

			databaseName := fmt.Sprintf("%s/%s_%d.db", path, identifier, atomic.AddUint64(&v, 1))
			database, _ := createDatabase(databaseName, "sqlite3", dbConfig)
			configuration.migrateDatabase(database)

			return database, nil
		})

	poolConfig := pool.NewDefaultPoolConfig()
	poolConfig.LIFO = true
	poolConfig.MaxTotal = size
	poolConfig.BlockWhenExhausted = true

	ctx := context.Background()
	p := pool.NewObjectPool(ctx, factory, poolConfig)

	group := &Group{
		identifier:    identifier,
		path:          path,
		size:          size,
		configuration: configuration,
		pool:          p,
		context:       &ctx,
	}

	return group
}
