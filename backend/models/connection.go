package models

import "time"

type DatabaseType string

const (
	PostgreSQL   DatabaseType = "PostgreSQL"
	MongoDB      DatabaseType = "MongoDB"
	Elasticsearch DatabaseType = "Elasticsearch"
	Meilisearch  DatabaseType = "Meilisearch"
	ClickHouse   DatabaseType = "ClickHouse"
	Cassandra    DatabaseType = "Cassandra"
	Aerospike    DatabaseType = "Aerospike"
	Redis        DatabaseType = "Redis"
	InfluxDB     DatabaseType = "InfluxDB"
	Neo4j        DatabaseType = "Neo4j"
	Couchbase    DatabaseType = "Couchbase"
	Supabase     DatabaseType = "Supabase"
	Druid        DatabaseType = "Druid"
	CockroachDB  DatabaseType = "CockroachDB"
	Kafka        DatabaseType = "Kafka"
	RabbitMQ     DatabaseType = "RabbitMQ"
	Zookeeper    DatabaseType = "Zookeeper"
)

type Connection struct {
	ID        string       `json:"id"`
	Name      string       `json:"name"`
	Type      DatabaseType `json:"type"`
	Host      string       `json:"host"`
	Port      string       `json:"port"`
	Database  string       `json:"database"`
	Username  string       `json:"username"`
	Password  string       `json:"password"`
	SSL       bool         `json:"ssl"`
	Connected bool         `json:"connected"`
	CreatedAt time.Time    `json:"createdAt"`
	UpdatedAt time.Time    `json:"updatedAt"`
}

