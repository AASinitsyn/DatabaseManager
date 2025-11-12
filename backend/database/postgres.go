package database

import (
	"context"
	"crypto/tls"
	"database/sql"
	"database-manager/models"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgreSQLDriver struct {
	pool *pgxpool.Pool
	conn models.Connection
}

func NewPostgreSQLDriver() *PostgreSQLDriver {
	return &PostgreSQLDriver{}
}

func (d *PostgreSQLDriver) Connect(ctx context.Context, conn models.Connection) error {
	// Используем прямое создание конфигурации вместо DSN строки
	// чтобы избежать проблем с экранированием паролей со спецсимволами
	port := conn.Port
	if port == "" {
		port = "5432"
	}

	// Проверяем, что пароль не пустой
	if conn.Password == "" {
		return fmt.Errorf("пароль не указан для подключения")
	}

	// Создаем конфигурацию напрямую
	config, err := pgxpool.ParseConfig("")
	if err != nil {
		return fmt.Errorf("ошибка создания конфигурации: %w", err)
	}

	// Устанавливаем параметры подключения напрямую
	config.ConnConfig.Host = conn.Host
	config.ConnConfig.Port = func() uint16 {
		var p uint16
		fmt.Sscanf(port, "%d", &p)
		if p == 0 {
			return 5432
		}
		return p
	}()
	config.ConnConfig.User = conn.Username
	config.ConnConfig.Password = conn.Password
	config.ConnConfig.Database = conn.Database
	
	if conn.SSL {
		config.ConnConfig.TLSConfig = &tls.Config{
			InsecureSkipVerify: false,
		}
	}

	// Увеличиваем таймауты для медленных подключений
	config.ConnConfig.ConnectTimeout = 15 * time.Second

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return fmt.Errorf("ошибка подключения к PostgreSQL: %w (хост=%s, порт=%s, пользователь=%s, база=%s, длина_пароля=%d)", 
			err, conn.Host, port, conn.Username, conn.Database, len(conn.Password))
	}

	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		return fmt.Errorf("ошибка ping PostgreSQL: %w (хост=%s, порт=%s, пользователь=%s, база=%s)", 
			err, conn.Host, port, conn.Username, conn.Database)
	}

	d.pool = pool
	d.conn = conn
	return nil
}

func (d *PostgreSQLDriver) Disconnect(ctx context.Context) error {
	if d.pool != nil {
		d.pool.Close()
		d.pool = nil
	}
	return nil
}

func (d *PostgreSQLDriver) IsConnected(ctx context.Context) bool {
	if d.pool == nil {
		return false
	}
	return d.pool.Ping(ctx) == nil
}

func (d *PostgreSQLDriver) Ping(ctx context.Context) error {
	if d.pool == nil {
		return fmt.Errorf("подключение не установлено")
	}
	return d.pool.Ping(ctx)
}

func (d *PostgreSQLDriver) ExecuteQuery(ctx context.Context, query string) (*models.QueryResponse, error) {
	if d.pool == nil {
		return nil, fmt.Errorf("подключение не установлено")
	}

	startTime := time.Now()
	rows, err := d.pool.Query(ctx, query)
	if err != nil {
		return &models.QueryResponse{
			Error: err.Error(),
		}, nil
	}
	defer rows.Close()

	columns := make([]string, 0)
	fieldDescriptions := rows.FieldDescriptions()
	for _, desc := range fieldDescriptions {
		columns = append(columns, string(desc.Name))
	}

	rowsData := make([]map[string]interface{}, 0)
	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			continue
		}

		row := make(map[string]interface{})
		for i, col := range columns {
			if i < len(values) {
				row[col] = values[i]
			}
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

func (d *PostgreSQLDriver) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	if d.pool == nil {
		return fmt.Errorf("подключение не установлено")
	}

	query := fmt.Sprintf("CREATE DATABASE %s", name)
	
	if owner, ok := options["owner"].(string); ok && owner != "" {
		query += fmt.Sprintf(" OWNER = %s", owner)
	}
	
	if encoding, ok := options["encoding"].(string); ok && encoding != "" {
		query += fmt.Sprintf(" ENCODING = '%s'", encoding)
	}
	
	if locale, ok := options["locale"].(string); ok && locale != "" {
		query += fmt.Sprintf(" LC_COLLATE = '%s' LC_CTYPE = '%s'", locale, locale)
	}

	_, err := d.pool.Exec(ctx, query)
	return err
}

func (d *PostgreSQLDriver) ListDatabases(ctx context.Context) ([]models.DatabaseInfo, error) {
	if d.pool == nil {
		return nil, fmt.Errorf("подключение не установлено")
	}

	query := `
		SELECT 
			datname as name,
			pg_catalog.pg_get_userbyid(datdba) as owner,
			pg_size_pretty(pg_database_size(datname)) as size,
			pg_encoding_to_char(encoding) as encoding,
			datcollate as collation
		FROM pg_catalog.pg_database
		WHERE datistemplate = false
		ORDER BY datname
	`

	rows, err := d.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения списка баз данных: %w", err)
	}
	defer rows.Close()

	databases := make([]models.DatabaseInfo, 0)
	for rows.Next() {
		var db models.DatabaseInfo
		err := rows.Scan(&db.Name, &db.Owner, &db.Size, &db.Encoding, &db.Collation)
		if err != nil {
			continue
		}
		databases = append(databases, db)
	}

	return databases, nil
}

func (d *PostgreSQLDriver) UpdateDatabase(ctx context.Context, oldName, newName string, options map[string]interface{}) error {
	if d.pool == nil {
		return fmt.Errorf("подключение не установлено")
	}

	if newName != "" && newName != oldName {
		query := fmt.Sprintf("ALTER DATABASE %s RENAME TO %s", oldName, newName)
		_, err := d.pool.Exec(ctx, query)
		if err != nil {
			return fmt.Errorf("ошибка переименования базы данных: %w", err)
		}
	}

	if owner, ok := options["owner"].(string); ok && owner != "" {
		dbName := newName
		if dbName == "" {
			dbName = oldName
		}
		query := fmt.Sprintf("ALTER DATABASE %s OWNER TO %s", dbName, owner)
		_, err := d.pool.Exec(ctx, query)
		if err != nil {
			return fmt.Errorf("ошибка изменения владельца: %w", err)
		}
	}

	return nil
}

func (d *PostgreSQLDriver) DeleteDatabase(ctx context.Context, name string) error {
	if d.pool == nil {
		return fmt.Errorf("подключение не установлено")
	}

	query := fmt.Sprintf("DROP DATABASE IF EXISTS %s", name)
	_, err := d.pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("ошибка удаления базы данных: %w", err)
	}

	return nil
}

func (d *PostgreSQLDriver) CreateTable(ctx context.Context, name string, columns []models.TableColumn) error {
	if d.pool == nil {
		return fmt.Errorf("подключение не установлено")
	}

	if len(columns) == 0 {
		return fmt.Errorf("необходимо указать хотя бы одну колонку")
	}

	cols := make([]string, 0, len(columns))
	for _, col := range columns {
		colDef := fmt.Sprintf("  %s %s", col.Name, col.Type)
		if col.PrimaryKey {
			colDef += " PRIMARY KEY"
		}
		if !col.Nullable {
			colDef += " NOT NULL"
		}
		if col.Unique && !col.PrimaryKey {
			colDef += " UNIQUE"
		}
		cols = append(cols, colDef)
	}

	var query string
	if len(cols) == 1 {
		query = fmt.Sprintf("CREATE TABLE %s (\n%s\n)", name, cols[0])
	} else {
		query = fmt.Sprintf("CREATE TABLE %s (\n%s", name, cols[0])
		for i := 1; i < len(cols); i++ {
			query += ",\n" + cols[i]
		}
		query += "\n)"
	}

	_, err := d.pool.Exec(ctx, query)
	return err
}

func (d *PostgreSQLDriver) ListTables(ctx context.Context) ([]models.TableInfo, error) {
	if d.pool == nil {
		return nil, fmt.Errorf("подключение не установлено")
	}

	query := `
		SELECT 
			t.table_name,
			current_database() as database_name,
			pg_size_pretty(pg_total_relation_size(quote_ident(t.table_schema)||'.'||quote_ident(t.table_name))) as size,
			(SELECT reltuples::bigint FROM pg_class WHERE relname = t.table_name) as row_count
		FROM information_schema.tables t
		WHERE t.table_schema = 'public'
			AND t.table_type = 'BASE TABLE'
		ORDER BY t.table_name
	`

	rows, err := d.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения списка таблиц: %w", err)
	}
	defer rows.Close()

	tables := make([]models.TableInfo, 0)
	for rows.Next() {
		var table models.TableInfo
		var size sql.NullString
		var rowCount sql.NullInt64
		var databaseName sql.NullString

		err := rows.Scan(&table.Name, &databaseName, &size, &rowCount)
		if err != nil {
			continue
		}

		if databaseName.Valid {
			table.Database = databaseName.String
		} else if d.conn.Database != "" {
			table.Database = d.conn.Database
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

func (d *PostgreSQLDriver) DeleteTable(ctx context.Context, name string) error {
	if d.pool == nil {
		return fmt.Errorf("подключение не установлено")
	}

	query := fmt.Sprintf("DROP TABLE IF EXISTS %s CASCADE", name)
	_, err := d.pool.Exec(ctx, query)
	if err != nil {
		return fmt.Errorf("ошибка удаления таблицы: %w", err)
	}

	return nil
}

func (d *PostgreSQLDriver) UpdateTable(ctx context.Context, oldName, newName string, columns []models.TableColumn) error {
	if d.pool == nil {
		return fmt.Errorf("подключение не установлено")
	}

	if newName != "" && newName != oldName {
		query := fmt.Sprintf("ALTER TABLE %s RENAME TO %s", oldName, newName)
		_, err := d.pool.Exec(ctx, query)
		if err != nil {
			return fmt.Errorf("ошибка переименования таблицы: %w", err)
		}
		oldName = newName
	}

	if len(columns) > 0 {
		for _, col := range columns {
			colDef := fmt.Sprintf("%s %s", col.Name, col.Type)
			if col.PrimaryKey {
				colDef += " PRIMARY KEY"
			}
			if !col.Nullable {
				colDef += " NOT NULL"
			}
			if col.Unique && !col.PrimaryKey {
				colDef += " UNIQUE"
			}

			query := fmt.Sprintf("ALTER TABLE %s ADD COLUMN IF NOT EXISTS %s", oldName, colDef)
			_, err := d.pool.Exec(ctx, query)
			if err != nil {
				return fmt.Errorf("ошибка добавления колонки %s: %w", col.Name, err)
			}
		}
	}

	return nil
}

func (d *PostgreSQLDriver) CreateUser(ctx context.Context, username, password, database string, permissions []string) error {
	if d.pool == nil {
		return fmt.Errorf("подключение не установлено")
	}

	createUserQuery := fmt.Sprintf("CREATE USER %s WITH PASSWORD '%s'", username, password)
	_, err := d.pool.Exec(ctx, createUserQuery)
	if err != nil {
		return fmt.Errorf("ошибка создания пользователя: %w", err)
	}

	if len(permissions) > 0 {
		grantQuery := fmt.Sprintf("GRANT %s TO %s", permissions[0], username)
		if len(permissions) > 1 {
			permsStr := permissions[0]
			for i := 1; i < len(permissions); i++ {
				permsStr += ", " + permissions[i]
			}
			grantQuery = fmt.Sprintf("GRANT %s TO %s", permsStr, username)
		}
		_, err = d.pool.Exec(ctx, grantQuery)
		if err != nil {
			return fmt.Errorf("ошибка выдачи прав: %w", err)
		}
	}

	return nil
}

func (d *PostgreSQLDriver) ListUsers(ctx context.Context) ([]models.UserInfo, error) {
	if d.pool == nil {
		return nil, fmt.Errorf("подключение не установлено")
	}

	query := `
		SELECT 
			rolname as username,
			rolsuper as is_superuser,
			ARRAY(
				SELECT b.rolname 
				FROM pg_catalog.pg_auth_members m 
				JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid) 
				WHERE m.member = r.oid
			) as permissions
		FROM pg_catalog.pg_roles r
		WHERE rolcanlogin = true
		ORDER BY rolname
	`

	rows, err := d.pool.Query(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения списка пользователей: %w", err)
	}
	defer rows.Close()

	users := make([]models.UserInfo, 0)
	for rows.Next() {
		var username string
		var isSuperuser bool
		var permissions []string

		err := rows.Scan(&username, &isSuperuser, &permissions)
		if err != nil {
			continue
		}

		users = append(users, models.UserInfo{
			Username:     username,
			Permissions:  permissions,
			IsSuperuser:  isSuperuser,
		})
	}

	return users, nil
}

func (d *PostgreSQLDriver) UpdateUser(ctx context.Context, username, password string, permissions []string) error {
	if d.pool == nil {
		return fmt.Errorf("подключение не установлено")
	}

	if password != "" {
		alterQuery := fmt.Sprintf("ALTER USER %s WITH PASSWORD '%s'", username, password)
		_, err := d.pool.Exec(ctx, alterQuery)
		if err != nil {
			return fmt.Errorf("ошибка обновления пароля: %w", err)
		}
	}

	if permissions != nil {
		revokeQuery := fmt.Sprintf("REVOKE ALL PRIVILEGES ON DATABASE %s FROM %s", d.conn.Database, username)
		d.pool.Exec(ctx, revokeQuery)

		if len(permissions) > 0 {
			permsStr := permissions[0]
			for i := 1; i < len(permissions); i++ {
				permsStr += ", " + permissions[i]
			}
			grantQuery := fmt.Sprintf("GRANT %s TO %s", permsStr, username)
			_, err := d.pool.Exec(ctx, grantQuery)
			if err != nil {
				return fmt.Errorf("ошибка обновления прав: %w", err)
			}
		}
	}

	return nil
}

func (d *PostgreSQLDriver) DeleteUser(ctx context.Context, username string) error {
	if d.pool == nil {
		return fmt.Errorf("подключение не установлено")
	}

	dropQuery := fmt.Sprintf("DROP USER IF EXISTS %s", username)
	_, err := d.pool.Exec(ctx, dropQuery)
	if err != nil {
		return fmt.Errorf("ошибка удаления пользователя: %w", err)
	}

	return nil
}

