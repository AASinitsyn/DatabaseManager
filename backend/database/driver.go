package database

import (
	"context"
	"database-manager/models"
)

type DatabaseDriver interface {
	Connect(ctx context.Context, conn models.Connection) error
	Disconnect(ctx context.Context) error
	IsConnected(ctx context.Context) bool
	ExecuteQuery(ctx context.Context, query string) (*models.QueryResponse, error)
	CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error
	ListDatabases(ctx context.Context) ([]models.DatabaseInfo, error)
	UpdateDatabase(ctx context.Context, oldName, newName string, options map[string]interface{}) error
	DeleteDatabase(ctx context.Context, name string) error
	CreateTable(ctx context.Context, name string, columns []models.TableColumn) error
	ListTables(ctx context.Context) ([]models.TableInfo, error)
	DeleteTable(ctx context.Context, name string) error
	UpdateTable(ctx context.Context, oldName, newName string, columns []models.TableColumn) error
	CreateUser(ctx context.Context, username, password, database string, permissions []string) error
	ListUsers(ctx context.Context) ([]models.UserInfo, error)
	UpdateUser(ctx context.Context, username, password string, permissions []string) error
	DeleteUser(ctx context.Context, username string) error
	Ping(ctx context.Context) error
}

type DriverFactory struct{}

func NewDriverFactory() *DriverFactory {
	return &DriverFactory{}
}

func (f *DriverFactory) CreateDriver(dbType models.DatabaseType) DatabaseDriver {
	switch dbType {
	case models.PostgreSQL:
		return NewPostgreSQLDriver()
	case models.MongoDB:
		return NewMongoDBDriver()
	case models.Elasticsearch:
		return NewElasticsearchDriver()
	case models.Meilisearch:
		return NewMeilisearchDriver()
	case models.ClickHouse:
		return NewClickHouseDriver()
	case models.Cassandra:
		return NewCassandraDriver()
	case models.Aerospike:
		return NewAerospikeDriver()
	case models.Redis:
		return NewRedisDriver()
	case models.InfluxDB:
		return NewInfluxDBDriver()
	case models.Neo4j:
		return NewNeo4jDriver()
	case models.Couchbase:
		return NewCouchbaseDriver()
	case models.Supabase:
		return NewSupabaseDriver()
	case models.Druid:
		return NewDruidDriver()
	case models.CockroachDB:
		return NewCockroachDBDriver()
	case models.Kafka:
		return NewKafkaDriver()
	case models.RabbitMQ:
		return NewRabbitMQDriver()
	case models.Zookeeper:
		return NewZookeeperDriver()
	default:
		return nil
	}
}

