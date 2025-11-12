package database

import (
	"context"
	"database-manager/models"
	"fmt"
	"time"

	"github.com/gocql/gocql"
)

type CassandraDriver struct {
	session *gocql.Session
	cluster *gocql.ClusterConfig
	conn    models.Connection
}

func NewCassandraDriver() *CassandraDriver {
	return &CassandraDriver{}
}

func (d *CassandraDriver) Connect(ctx context.Context, conn models.Connection) error {
	cluster := gocql.NewCluster(conn.Host)
	cluster.Port = 9042
	if conn.Port != "" {
		port := 9042
		fmt.Sscanf(conn.Port, "%d", &port)
		cluster.Port = port
	}
	cluster.Keyspace = conn.Database
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: conn.Username,
		Password: conn.Password,
	}
	cluster.Timeout = 10 * time.Second

	session, err := cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("ошибка подключения к Cassandra: %w", err)
	}

	d.session = session
	d.cluster = cluster
	d.conn = conn
	return nil
}

func (d *CassandraDriver) Disconnect(ctx context.Context) error {
	if d.session != nil {
		d.session.Close()
		d.session = nil
	}
	return nil
}

func (d *CassandraDriver) IsConnected(ctx context.Context) bool {
	if d.session == nil {
		return false
	}
	return d.session.Closed() == false
}

func (d *CassandraDriver) Ping(ctx context.Context) error {
	if d.session == nil {
		return fmt.Errorf("подключение не установлено")
	}
	return d.session.Query("SELECT now() FROM system.local").Exec()
}

func (d *CassandraDriver) ExecuteQuery(ctx context.Context, query string) (*models.QueryResponse, error) {
	if d.session == nil {
		return nil, fmt.Errorf("подключение не установлено")
	}

	startTime := time.Now()
	iter := d.session.Query(query).Iter()

	columns := iter.Columns()
	rowsData := make([]map[string]interface{}, 0)

	var row map[string]interface{}
	for iter.MapScan(row) {
		rowsData = append(rowsData, row)
		row = make(map[string]interface{})
	}

	if err := iter.Close(); err != nil {
		return &models.QueryResponse{
			Error: err.Error(),
		}, nil
	}

	columnNames := make([]string, len(columns))
	for i, col := range columns {
		columnNames[i] = col.Name
	}

	executionTime := time.Since(startTime).Milliseconds()

	return &models.QueryResponse{
		Columns:       columnNames,
		Rows:          rowsData,
		RowCount:      len(rowsData),
		ExecutionTime: executionTime,
	}, nil
}

func (d *CassandraDriver) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	if d.session == nil {
		return fmt.Errorf("подключение не установлено")
	}

	replicationFactor := 3
	if rf, ok := options["replication_factor"].(float64); ok {
		replicationFactor = int(rf)
	}

	query := fmt.Sprintf(`CREATE KEYSPACE IF NOT EXISTS %s 
		WITH replication = {
			'class': 'SimpleStrategy',
			'replication_factor': %d
		}`, name, replicationFactor)

	return d.session.Query(query).Exec()
}

func (d *CassandraDriver) ListDatabases(ctx context.Context) ([]models.DatabaseInfo, error) {
	if d.session == nil {
		return nil, fmt.Errorf("подключение не установлено")
	}

	query := "SELECT keyspace_name, durable_writes FROM system_schema.keyspaces WHERE keyspace_name NOT IN ('system', 'system_schema', 'system_auth', 'system_distributed', 'system_traces')"
	iter := d.session.Query(query).Iter()

	databases := make([]models.DatabaseInfo, 0)
	var keyspaceName string
	var durableWrites bool

	for iter.Scan(&keyspaceName, &durableWrites) {
		databases = append(databases, models.DatabaseInfo{
			Name: keyspaceName,
		})
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("ошибка получения списка баз данных: %w", err)
	}

	return databases, nil
}

func (d *CassandraDriver) UpdateDatabase(ctx context.Context, oldName, newName string, options map[string]interface{}) error {
	if d.session == nil {
		return fmt.Errorf("подключение не установлено")
	}

	if newName != "" && newName != oldName {
		return fmt.Errorf("переименование keyspace в Cassandra не поддерживается напрямую. Используйте CREATE KEYSPACE и миграцию данных")
	}

	if replicationFactor, ok := options["replication_factor"].(float64); ok {
		query := fmt.Sprintf(`ALTER KEYSPACE %s WITH replication = {
			'class': 'SimpleStrategy',
			'replication_factor': %d
		}`, oldName, int(replicationFactor))
		if err := d.session.Query(query).Exec(); err != nil {
			return fmt.Errorf("ошибка обновления keyspace: %w", err)
		}
	}

	return nil
}

func (d *CassandraDriver) DeleteDatabase(ctx context.Context, name string) error {
	if d.session == nil {
		return fmt.Errorf("подключение не установлено")
	}

	query := fmt.Sprintf("DROP KEYSPACE IF EXISTS %s", name)
	if err := d.session.Query(query).Exec(); err != nil {
		return fmt.Errorf("ошибка удаления keyspace: %w", err)
	}

	return nil
}

func (d *CassandraDriver) CreateTable(ctx context.Context, name string, columns []models.TableColumn) error {
	if d.session == nil {
		return fmt.Errorf("подключение не установлено")
	}

	if len(columns) == 0 {
		return fmt.Errorf("необходимо указать хотя бы одну колонку")
	}

	primaryKeys := make([]string, 0)
	cols := make([]string, 0, len(columns))

	for _, col := range columns {
		colDef := fmt.Sprintf("%s %s", col.Name, col.Type)
		cols = append(cols, colDef)
		if col.PrimaryKey {
			primaryKeys = append(primaryKeys, col.Name)
		}
	}

	if len(primaryKeys) == 0 {
		if len(columns) > 0 {
			primaryKeys = append(primaryKeys, columns[0].Name)
		}
	}

	colsStr := cols[0]
	if len(cols) > 1 {
		for i := 1; i < len(cols); i++ {
			colsStr += ", " + cols[i]
		}
	}
	
	primaryKeysStr := primaryKeys[0]
	if len(primaryKeys) > 1 {
		for i := 1; i < len(primaryKeys); i++ {
			primaryKeysStr += ", " + primaryKeys[i]
		}
	}
	
	query := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %s (%s, PRIMARY KEY (%s))",
		name, colsStr, primaryKeysStr)

	return d.session.Query(query).Exec()
}

func (d *CassandraDriver) ListTables(ctx context.Context) ([]models.TableInfo, error) {
	if d.session == nil {
		return nil, fmt.Errorf("подключение не установлено")
	}

	keyspace := d.conn.Database
	query := fmt.Sprintf("SELECT table_name FROM system_schema.tables WHERE keyspace_name = '%s'", keyspace)
	iter := d.session.Query(query).Iter()

	tables := make([]models.TableInfo, 0)
	var tableName string
	for iter.Scan(&tableName) {
		tables = append(tables, models.TableInfo{
			Name:     tableName,
			Database: keyspace,
		})
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("ошибка получения списка таблиц: %w", err)
	}

	return tables, nil
}

func (d *CassandraDriver) DeleteTable(ctx context.Context, name string) error {
	if d.session == nil {
		return fmt.Errorf("подключение не установлено")
	}

	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", name)
	return d.session.Query(query).Exec()
}

func (d *CassandraDriver) UpdateTable(ctx context.Context, oldName, newName string, columns []models.TableColumn) error {
	if d.session == nil {
		return fmt.Errorf("подключение не установлено")
	}

	tableName := oldName
	if newName != "" && newName != oldName {
		query := fmt.Sprintf("ALTER TABLE %s RENAME TO %s", oldName, newName)
		if err := d.session.Query(query).Exec(); err != nil {
			return fmt.Errorf("ошибка переименования таблицы: %w", err)
		}
		tableName = newName
	}

	if len(columns) > 0 {
		for _, col := range columns {
			query := fmt.Sprintf("ALTER TABLE %s ADD %s %s", tableName, col.Name, col.Type)
			if err := d.session.Query(query).Exec(); err != nil {
				return fmt.Errorf("ошибка добавления колонки %s: %w", col.Name, err)
			}
		}
	}

	return nil
}

func (d *CassandraDriver) CreateUser(ctx context.Context, username, password, database string, permissions []string) error {
	if d.session == nil {
		return fmt.Errorf("подключение не установлено")
	}

	createQuery := fmt.Sprintf("CREATE ROLE IF NOT EXISTS %s WITH PASSWORD = '%s' AND LOGIN = true", username, password)
	if err := d.session.Query(createQuery).Exec(); err != nil {
		return fmt.Errorf("ошибка создания пользователя: %w", err)
	}

	if len(permissions) > 0 {
		for _, perm := range permissions {
			grantQuery := fmt.Sprintf("GRANT %s ON KEYSPACE %s TO %s", perm, database, username)
			if database == "" {
				grantQuery = fmt.Sprintf("GRANT %s ON ALL KEYSPACES TO %s", perm, username)
			}
			if err := d.session.Query(grantQuery).Exec(); err != nil {
				return fmt.Errorf("ошибка выдачи прав: %w", err)
			}
		}
	}

	return nil
}

func (d *CassandraDriver) ListUsers(ctx context.Context) ([]models.UserInfo, error) {
	if d.session == nil {
		return nil, fmt.Errorf("подключение не установлено")
	}

	query := "SELECT role, is_superuser, can_login FROM system_auth.roles"
	iter := d.session.Query(query).Iter()

	users := make([]models.UserInfo, 0)
	var username string
	var isSuperuser bool
	var canLogin bool

	for iter.Scan(&username, &isSuperuser, &canLogin) {
		if !canLogin {
			continue
		}

		permissionsQuery := fmt.Sprintf("SELECT role FROM system_auth.role_members WHERE member = '%s'", username)
		permsIter := d.session.Query(permissionsQuery).Iter()
		permissions := make([]string, 0)
		var perm string
		for permsIter.Scan(&perm) {
			permissions = append(permissions, perm)
		}
		permsIter.Close()

		users = append(users, models.UserInfo{
			Username:    username,
			Permissions: permissions,
			IsSuperuser: isSuperuser,
		})
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("ошибка получения списка пользователей: %w", err)
	}

	return users, nil
}

func (d *CassandraDriver) UpdateUser(ctx context.Context, username, password string, permissions []string) error {
	if d.session == nil {
		return fmt.Errorf("подключение не установлено")
	}

	if password != "" {
		alterQuery := fmt.Sprintf("ALTER ROLE %s WITH PASSWORD = '%s'", username, password)
		if err := d.session.Query(alterQuery).Exec(); err != nil {
			return fmt.Errorf("ошибка обновления пароля: %w", err)
		}
	}

	if permissions != nil {
		revokeQuery := fmt.Sprintf("REVOKE ALL PERMISSIONS ON ALL KEYSPACES FROM %s", username)
		d.session.Query(revokeQuery).Exec()

		if len(permissions) > 0 {
			for _, perm := range permissions {
				grantQuery := fmt.Sprintf("GRANT %s ON KEYSPACE %s TO %s", perm, d.conn.Database, username)
				if err := d.session.Query(grantQuery).Exec(); err != nil {
					return fmt.Errorf("ошибка обновления прав: %w", err)
				}
			}
		}
	}

	return nil
}

func (d *CassandraDriver) DeleteUser(ctx context.Context, username string) error {
	if d.session == nil {
		return fmt.Errorf("подключение не установлено")
	}

	dropQuery := fmt.Sprintf("DROP ROLE IF EXISTS %s", username)
	if err := d.session.Query(dropQuery).Exec(); err != nil {
		return fmt.Errorf("ошибка удаления пользователя: %w", err)
	}

	return nil
}

