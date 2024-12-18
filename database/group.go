package database

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/projectdiscovery/roundrobin"
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
	databases     map[string]*Database
	balancer      *roundrobin.RoundRobin
}

func (group *Group) Exec(query string, args ...any) error {
	index := group.nextDatabaseIndex()

	return group.databases[index].Exec(query, args...)
}

func newGroup(identifier string, path string, size int, configuration *GroupConfiguration) *Group {
	indexes := generateStringArray(size)
	rb, _ := roundrobin.New(indexes...)

	group := &Group{
		identifier:    identifier,
		path:          path,
		size:          size,
		configuration: configuration,
		balancer:      rb,
	}

	group.initDatabases()

	return group
}

func (group *Group) initDatabases() {
	var databases map[string]*Database = make(map[string]*Database)

	// Needs to move up
	config := &DatabaseConfiguration{
		synchronous:           false,
		maxOpenConnections:    1,
		maxIdleConnections:    1,
		connectionMaxLifeTime: 5 * time.Minute,
	}

	for i := 0; i < group.size; i++ {
		databaseName := fmt.Sprintf("%s/%s_%d.db", group.path, group.identifier, i)
		database, _ := createDatabase(databaseName, "sqlite3", config)
		group.configuration.migrateDatabase(database)
		databases[fmt.Sprintf("%d", i)] = database
	}

	group.databases = databases
}

func (group *Group) nextDatabaseIndex() string {
	if group.configuration.balancerPolicy == GroupLoadBalancerPolicy_Random {
		return fmt.Sprintf("%d", rand.Intn(group.size))
	}

	if group.configuration.balancerPolicy == GroupLoadBalancerPolicy_RoundRobin {
		return group.balancer.Next().String()
	}

	return "0"
}

func generateStringArray(max int) []string {
	var result []string
	for i := 0; i < max; i++ {
		result = append(result, strconv.Itoa(i)) // Convert int to string and add to the array
	}
	return result
}
