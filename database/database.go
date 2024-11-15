package database

import (
	"database/sql"
	"fmt"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type SchemaMigrator func(database *Database) error

type DatabaseConfiguration struct {
	synchronous           bool
	maxOpenConnections    int
	maxIdleConnections    int
	connectionMaxLifeTime time.Duration
}

type Database struct {
	name       string
	driverName string
	connection *sql.DB
	config     *DatabaseConfiguration
}

func createDatabase(name string, driverName string, config *DatabaseConfiguration) (*Database, error) {
	connection, err := sql.Open(driverName, name)
	if err != nil {
		return nil, err
	}

	db := &Database{
		name:       name,
		driverName: driverName,
		connection: connection,
		config:     config,
	}

	err = db.setup()
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (db *Database) Exec(statement string, args ...any) error {
	_, err := db.connection.Exec(statement, args...)
	if err != nil {
		return fmt.Errorf("failed to exec statement: %v", err)
	}

	return nil
}

func (db *Database) Migrate(schemaMigrator SchemaMigrator) error {
	err := schemaMigrator(db)
	if err != nil {
		return fmt.Errorf("failed to migrate a database: %v", err)
	}

	return nil
}

func (db *Database) setup() error {
	db.connection.SetMaxOpenConns(db.config.maxOpenConnections)
	db.connection.SetMaxIdleConns(db.config.maxIdleConnections)
	db.connection.SetConnMaxLifetime(db.config.connectionMaxLifeTime)

	if !db.config.synchronous {
		statement := "PRAGMA synchronous = OFF;"

		err := db.Exec(statement)
		if err != nil {
			return err
		}
	}

	return nil
}
