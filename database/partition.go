package database

import (
	"fmt"
	"sync"
)

type PartitionConfiguration struct {
	MaxConcurrency     int
	RotationPolicy     GroupRotationPolicy
	LoadBalancerPolicy GroupLoadBalancerPolicy
	SchemaMigrator     SchemaMigrator
}

type Partition struct {
	Name          string
	path          string
	mutex         sync.Mutex
	configuration *PartitionConfiguration
	groups        map[string]*Group
}

func NewPartition(name string, path string, configuration *PartitionConfiguration) *Partition {
	return &Partition{
		Name:          name,
		path:          path,
		configuration: configuration,
		groups:        make(map[string]*Group),
	}
}

func (partition *Partition) getCurrentGroupIdentifier() string {
	// now := time.Now()
	// return fmt.Sprintf("%02d", now.Second())
	return fmt.Sprintf("%02d", 00)
}

func (partition *Partition) getCurrentGroup() *Group {
	groupIdentifier := partition.getCurrentGroupIdentifier()

	group := partition.groups[groupIdentifier]
	if group != nil {
		return group
	}

	partition.mutex.Lock()
	defer partition.mutex.Unlock()

	group = partition.groups[groupIdentifier]
	if group != nil {
		return group
	}

	group = newGroup(
		groupIdentifier,
		partition.path,
		partition.configuration.MaxConcurrency,
		&GroupConfiguration{
			balancerPolicy:  partition.configuration.LoadBalancerPolicy,
			migrateDatabase: partition.configuration.SchemaMigrator,
		},
	)

	partition.groups[groupIdentifier] = group

	return group
}

func (partition *Partition) Exec(query string, args ...any) error {
	group := partition.getCurrentGroup()

	return group.Exec(query, args...)
}

func (partition *Partition) Close() {
	group := partition.getCurrentGroup()

	group.close()
}
