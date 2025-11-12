package database

import (
	"context"
	"crypto/tls"
	"database-manager/models"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
)

type ClickHouseDriver struct {
	conn driver.Conn
	dbConn models.Connection
}

func NewClickHouseDriver() *ClickHouseDriver {
	return &ClickHouseDriver{}
}

func (d *ClickHouseDriver) Connect(ctx context.Context, conn models.Connection) error {
	dsn := fmt.Sprintf("clickhouse://%s:%s@%s:%s/%s",
		conn.Username, conn.Password, conn.Host, conn.Port, conn.Database)
	
	if conn.SSL {
		dsn += "?secure=true"
	}

	options, err := clickhouse.ParseDSN(dsn)
	if err != nil {
		return fmt.Errorf("ошибка парсинга DSN: %w", err)
	}

	if conn.SSL {
		options.TLS = &tls.Config{
			InsecureSkipVerify: false,
		}
	}

	chConn, err := clickhouse.Open(options)
	if err != nil {
		return fmt.Errorf("ошибка подключения к ClickHouse: %w", err)
	}

	if err := chConn.Ping(ctx); err != nil {
		return fmt.Errorf("ошибка ping ClickHouse: %w", err)
	}

	d.conn = chConn
	d.dbConn = conn
	return nil
}

func (d *ClickHouseDriver) Disconnect(ctx context.Context) error {
	if d.conn != nil {
		return d.conn.Close()
	}
	return nil
}

func (d *ClickHouseDriver) IsConnected(ctx context.Context) bool {
	if d.conn == nil {
		return false
	}
	return d.conn.Ping(ctx) == nil
}

func (d *ClickHouseDriver) Ping(ctx context.Context) error {
	if d.conn == nil {
		return fmt.Errorf("подключение не установлено")
	}
	return d.conn.Ping(ctx)
}

func (d *ClickHouseDriver) ExecuteQuery(ctx context.Context, query string) (*models.QueryResponse, error) {
	if d.conn == nil {
		return nil, fmt.Errorf("подключение не установлено")
	}

	startTime := time.Now()
	rows, err := d.conn.Query(ctx, query)
	if err != nil {
		return &models.QueryResponse{
			Error: err.Error(),
		}, nil
	}
	defer rows.Close()

	columns := rows.Columns()
	columnTypes := rows.ColumnTypes()

	rowsData := make([]map[string]interface{}, 0)
	for rows.Next() {
		values := make([]interface{}, len(columns))
		valuePtrs := make([]interface{}, len(columns))
		for i := range values {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			continue
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			val := values[i]
			if columnTypes[i].DatabaseTypeName() == "DateTime" || columnTypes[i].DatabaseTypeName() == "Date" {
				if t, ok := val.(time.Time); ok {
					val = t.Format(time.RFC3339)
				}
			}
			row[col] = val
		}
		rowsData = append(rowsData, row)
	}

	executionTime := time.Since(startTime).Milliseconds()

	return &models.QueryResponse{
		Columns:       columns,
		Rows:          rowsData,
		RowCount:      len(rowsData),
		ExecutionTime: executionTime,
	}, nil
}

func (d *ClickHouseDriver) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	if d.conn == nil {
		return fmt.Errorf("подключение не установлено")
	}

	query := fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", name)
	return d.conn.Exec(ctx, query)
}

func (d *ClickHouseDriver) ListDatabases(ctx context.Context) ([]models.DatabaseInfo, error) {
	if d.conn == nil {
		return nil, fmt.Errorf("подключение не установлено")
	}

	query := "SELECT name, engine, data_path FROM system.databases WHERE name NOT IN ('system', 'information_schema', 'INFORMATION_SCHEMA') ORDER BY name"
	rows, err := d.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения списка баз данных: %w", err)
	}
	defer rows.Close()

	databases := make([]models.DatabaseInfo, 0)
	for rows.Next() {
		var db models.DatabaseInfo
		var engine, dataPath string
		err := rows.Scan(&db.Name, &engine, &dataPath)
		if err != nil {
			continue
		}

		sizeQuery := fmt.Sprintf("SELECT formatReadableSize(sum(bytes)) FROM system.parts WHERE database = '%s' AND active = 1", db.Name)
		sizeRows, err := d.conn.Query(ctx, sizeQuery)
		if err == nil {
			if sizeRows.Next() {
				sizeRows.Scan(&db.Size)
			}
			sizeRows.Close()
		}

		databases = append(databases, db)
	}

	return databases, nil
}

func (d *ClickHouseDriver) UpdateDatabase(ctx context.Context, oldName, newName string, options map[string]interface{}) error {
	if d.conn == nil {
		return fmt.Errorf("подключение не установлено")
	}

	if newName != "" && newName != oldName {
		query := fmt.Sprintf("RENAME DATABASE %s TO %s", oldName, newName)
		if err := d.conn.Exec(ctx, query); err != nil {
			return fmt.Errorf("ошибка переименования базы данных: %w", err)
		}
	}

	return nil
}

func (d *ClickHouseDriver) DeleteDatabase(ctx context.Context, name string) error {
	if d.conn == nil {
		return fmt.Errorf("подключение не установлено")
	}

	query := fmt.Sprintf("DROP DATABASE IF EXISTS %s", name)
	if err := d.conn.Exec(ctx, query); err != nil {
		return fmt.Errorf("ошибка удаления базы данных: %w", err)
	}

	return nil
}

func (d *ClickHouseDriver) CreateTable(ctx context.Context, name string, columns []models.TableColumn) error {
	if d.conn == nil {
		return fmt.Errorf("подключение не установлено")
	}

	if len(columns) == 0 {
		return fmt.Errorf("необходимо указать хотя бы одну колонку")
	}

	cols := make([]string, 0, len(columns))
	for _, col := range columns {
		colDef := fmt.Sprintf("  %s %s", col.Name, col.Type)
		if !col.Nullable {
			colDef += " NOT NULL"
		}
		cols = append(cols, colDef)
	}

	query := fmt.Sprintf("CREATE TABLE %s (\n%s\n) ENGINE = MergeTree() ORDER BY tuple()", name, strings.Join(cols, ",\n"))

	return d.conn.Exec(ctx, query)
}

func (d *ClickHouseDriver) ListTables(ctx context.Context) ([]models.TableInfo, error) {
	if d.conn == nil {
		return nil, fmt.Errorf("подключение не установлено")
	}

	query := "SELECT name, database, total_rows, formatReadableSize(total_bytes) as size FROM system.tables WHERE database = currentDatabase() AND engine LIKE '%MergeTree%' ORDER BY name"
	rows, err := d.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения списка таблиц: %w", err)
	}
	defer rows.Close()

	tables := make([]models.TableInfo, 0)
	for rows.Next() {
		var table models.TableInfo
		var rowCount sql.NullInt64
		var size sql.NullString
		var databaseName sql.NullString

		err := rows.Scan(&table.Name, &databaseName, &rowCount, &size)
		if err != nil {
			continue
		}

		if databaseName.Valid {
			table.Database = databaseName.String
		} else if d.dbConn.Database != "" {
			table.Database = d.dbConn.Database
		}

		if size.Valid {
			table.Size = size.String
		}
		if rowCount.Valid {
			table.Rows = rowCount.Int64
		}

		tables = append(tables, table)
	}

	return tables, nil
}

func (d *ClickHouseDriver) DeleteTable(ctx context.Context, name string) error {
	if d.conn == nil {
		return fmt.Errorf("подключение не установлено")
	}

	query := fmt.Sprintf("DROP TABLE IF EXISTS %s", name)
	return d.conn.Exec(ctx, query)
}

func (d *ClickHouseDriver) UpdateTable(ctx context.Context, oldName, newName string, columns []models.TableColumn) error {
	if d.conn == nil {
		return fmt.Errorf("подключение не установлено")
	}

	if newName != "" && newName != oldName {
		query := fmt.Sprintf("RENAME TABLE %s TO %s", oldName, newName)
		if err := d.conn.Exec(ctx, query); err != nil {
			return fmt.Errorf("ошибка переименования таблицы: %w", err)
		}
		oldName = newName
	}

	if len(columns) > 0 {
		for _, col := range columns {
			colDef := fmt.Sprintf("%s %s", col.Name, col.Type)
			if !col.Nullable {
				colDef += " NOT NULL"
			}
			query := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s", oldName, colDef)
			if err := d.conn.Exec(ctx, query); err != nil {
				return fmt.Errorf("ошибка добавления колонки %s: %w", col.Name, err)
			}
		}
	}

	return nil
}

func (d *ClickHouseDriver) CreateUser(ctx context.Context, username, password, database string, permissions []string) error {
	if d.conn == nil {
		return fmt.Errorf("подключение не установлено")
	}

	createUserQuery := fmt.Sprintf("CREATE USER IF NOT EXISTS %s IDENTIFIED WITH plaintext_password BY '%s'", username, password)
	if err := d.conn.Exec(ctx, createUserQuery); err != nil {
		return fmt.Errorf("ошибка создания пользователя: %w", err)
	}

	if len(permissions) > 0 {
		grantQuery := fmt.Sprintf("GRANT %s ON %s.* TO %s", strings.Join(permissions, ", "), database, username)
		if err := d.conn.Exec(ctx, grantQuery); err != nil {
			return fmt.Errorf("ошибка выдачи прав: %w", err)
		}
	}

	return nil
}

func (d *ClickHouseDriver) ListUsers(ctx context.Context) ([]models.UserInfo, error) {
	if d.conn == nil {
		return nil, fmt.Errorf("подключение не установлено")
	}

	query := "SELECT name FROM system.users"
	rows, err := d.conn.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения списка пользователей: %w", err)
	}
	defer rows.Close()

	users := make([]models.UserInfo, 0)
	for rows.Next() {
		var username string
		if err := rows.Scan(&username); err != nil {
			continue
		}

		grantsQuery := fmt.Sprintf("SHOW GRANTS FOR %s", username)
		grantsRows, err := d.conn.Query(ctx, grantsQuery)
		permissions := make([]string, 0)
		if err == nil {
			for grantsRows.Next() {
				var grant string
				if err := grantsRows.Scan(&grant); err == nil {
					permissions = append(permissions, grant)
				}
			}
			grantsRows.Close()
		}

		isSuperuser := false
		for _, perm := range permissions {
			if strings.Contains(strings.ToUpper(perm), "ALL") || strings.Contains(strings.ToUpper(perm), "ADMIN") {
				isSuperuser = true
				break
			}
		}

		users = append(users, models.UserInfo{
			Username:    username,
			Permissions: permissions,
			IsSuperuser: isSuperuser,
		})
	}

	return users, nil
}

func (d *ClickHouseDriver) UpdateUser(ctx context.Context, username, password string, permissions []string) error {
	if d.conn == nil {
		return fmt.Errorf("подключение не установлено")
	}

	if password != "" {
		alterQuery := fmt.Sprintf("ALTER USER %s IDENTIFIED WITH plaintext_password BY '%s'", username, password)
		if err := d.conn.Exec(ctx, alterQuery); err != nil {
			return fmt.Errorf("ошибка обновления пароля: %w", err)
		}
	}

	if permissions != nil {
		revokeQuery := fmt.Sprintf("REVOKE ALL ON *.* FROM %s", username)
		d.conn.Exec(ctx, revokeQuery)

		if len(permissions) > 0 {
			for _, perm := range permissions {
				grantQuery := fmt.Sprintf("GRANT %s ON %s.* TO %s", perm, d.dbConn.Database, username)
				if d.dbConn.Database == "" {
					grantQuery = fmt.Sprintf("GRANT %s ON *.* TO %s", perm, username)
				}
				if err := d.conn.Exec(ctx, grantQuery); err != nil {
					return fmt.Errorf("ошибка обновления прав: %w", err)
				}
			}
		}
	}

	return nil
}

func (d *ClickHouseDriver) DeleteUser(ctx context.Context, username string) error {
	if d.conn == nil {
		return fmt.Errorf("подключение не установлено")
	}

	dropQuery := fmt.Sprintf("DROP USER IF EXISTS %s", username)
	if err := d.conn.Exec(ctx, dropQuery); err != nil {
		return fmt.Errorf("ошибка удаления пользователя: %w", err)
	}

	return nil
}

