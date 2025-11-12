package database

import (
	"bytes"
	"context"
	"database-manager/models"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

type DruidDriver struct {
	client  *http.Client
	baseURL string
	conn    models.Connection
}

func NewDruidDriver() *DruidDriver {
	return &DruidDriver{
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

func (d *DruidDriver) Connect(ctx context.Context, conn models.Connection) error {
	scheme := "http"
	if conn.SSL {
		scheme = "https"
	}
	d.baseURL = fmt.Sprintf("%s://%s:%s", scheme, conn.Host, conn.Port)
	d.conn = conn

	if err := d.Ping(ctx); err != nil {
		return fmt.Errorf("ошибка подключения к Druid: %w", err)
	}

	return nil
}

func (d *DruidDriver) Disconnect(ctx context.Context) error {
	d.client = nil
	d.baseURL = ""
	return nil
}

func (d *DruidDriver) IsConnected(ctx context.Context) bool {
	return d.baseURL != "" && d.Ping(ctx) == nil
}

func (d *DruidDriver) Ping(ctx context.Context) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	pingURL := fmt.Sprintf("%s/status", d.baseURL)
	req, err := http.NewRequestWithContext(ctx, "GET", pingURL, nil)
	if err != nil {
		return err
	}

	if d.conn.Username != "" {
		req.SetBasicAuth(d.conn.Username, d.conn.Password)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ошибка ping: статус %d", resp.StatusCode)
	}

	return nil
}

func (d *DruidDriver) ExecuteQuery(ctx context.Context, query string) (*models.QueryResponse, error) {
	if d.baseURL == "" {
		return nil, fmt.Errorf("подключение не установлено")
	}

	startTime := time.Now()

	queryURL := fmt.Sprintf("%s/druid/v2/sql", d.baseURL)
	
	requestBody := map[string]interface{}{
		"query": query,
		"context": map[string]interface{}{
			"sqlTimeZone": "UTC",
		},
	}

	jsonBody, _ := json.Marshal(requestBody)
	req, err := http.NewRequestWithContext(ctx, "POST", queryURL, bytes.NewBuffer(jsonBody))
	if err != nil {
		return &models.QueryResponse{Error: err.Error()}, nil
	}
	req.Header.Set("Content-Type", "application/json")

	if d.conn.Username != "" {
		req.SetBasicAuth(d.conn.Username, d.conn.Password)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return &models.QueryResponse{Error: err.Error()}, nil
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		return &models.QueryResponse{
			Error: fmt.Sprintf("ошибка выполнения запроса: %s", string(respBody)),
		}, nil
	}

	var results []map[string]interface{}
	if err := json.Unmarshal(respBody, &results); err != nil {
		return &models.QueryResponse{Error: err.Error()}, nil
	}

	columns := []string{}
	rowsData := make([]map[string]interface{}, 0)

	if len(results) > 0 {
		for key := range results[0] {
			columns = append(columns, key)
		}
	}

	for _, row := range results {
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

func (d *DruidDriver) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	return fmt.Errorf("Druid не поддерживает создание баз данных. Используйте datasources")
}

func (d *DruidDriver) ListDatabases(ctx context.Context) ([]models.DatabaseInfo, error) {
	if d.baseURL == "" {
		return nil, fmt.Errorf("подключение не установлено")
	}

	queryURL := fmt.Sprintf("%s/druid/v2/sql", d.baseURL)
	query := "SELECT DISTINCT \"datasource\" FROM INFORMATION_SCHEMA.TABLES"

	requestBody := map[string]interface{}{
		"query": query,
	}

	jsonBody, _ := json.Marshal(requestBody)
	req, err := http.NewRequestWithContext(ctx, "POST", queryURL, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	if d.conn.Username != "" {
		req.SetBasicAuth(d.conn.Username, d.conn.Password)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var results []map[string]interface{}
	if err := json.Unmarshal(respBody, &results); err != nil {
		return nil, err
	}

	databases := make([]models.DatabaseInfo, 0)
	for _, row := range results {
		if datasource, ok := row["datasource"].(string); ok {
			databases = append(databases, models.DatabaseInfo{
				Name: datasource,
			})
		}
	}

	return databases, nil
}

func (d *DruidDriver) UpdateDatabase(ctx context.Context, oldName, newName string, options map[string]interface{}) error {
	return fmt.Errorf("Druid не поддерживает переименование datasources")
}

func (d *DruidDriver) DeleteDatabase(ctx context.Context, name string) error {
	return fmt.Errorf("Druid не поддерживает удаление datasources через этот интерфейс")
}

func (d *DruidDriver) CreateTable(ctx context.Context, name string, columns []models.TableColumn) error {
	return fmt.Errorf("Druid не поддерживает создание таблиц напрямую. Используйте ingestion")
}

func (d *DruidDriver) ListTables(ctx context.Context) ([]models.TableInfo, error) {
	if d.baseURL == "" {
		return nil, fmt.Errorf("подключение не установлено")
	}

	queryURL := fmt.Sprintf("%s/druid/v2/sql", d.baseURL)
	query := "SELECT TABLE_NAME, TABLE_SCHEMA FROM INFORMATION_SCHEMA.TABLES"

	requestBody := map[string]interface{}{
		"query": query,
	}

	jsonBody, _ := json.Marshal(requestBody)
	req, err := http.NewRequestWithContext(ctx, "POST", queryURL, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	if d.conn.Username != "" {
		req.SetBasicAuth(d.conn.Username, d.conn.Password)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var results []map[string]interface{}
	if err := json.Unmarshal(respBody, &results); err != nil {
		return nil, err
	}

	tables := make([]models.TableInfo, 0)
	for _, row := range results {
		if tableName, ok := row["TABLE_NAME"].(string); ok {
			database := d.conn.Database
			if schema, ok := row["TABLE_SCHEMA"].(string); ok {
				database = schema
			}
			tables = append(tables, models.TableInfo{
				Name:     tableName,
				Database: database,
			})
		}
	}

	return tables, nil
}

func (d *DruidDriver) DeleteTable(ctx context.Context, name string) error {
	return fmt.Errorf("Druid не поддерживает удаление таблиц через этот интерфейс")
}

func (d *DruidDriver) UpdateTable(ctx context.Context, oldName, newName string, columns []models.TableColumn) error {
	return fmt.Errorf("Druid не поддерживает переименование таблиц")
}

func (d *DruidDriver) CreateUser(ctx context.Context, username, password, database string, permissions []string) error {
	return fmt.Errorf("Druid не поддерживает управление пользователями через этот интерфейс")
}

func (d *DruidDriver) ListUsers(ctx context.Context) ([]models.UserInfo, error) {
	return nil, fmt.Errorf("Druid не поддерживает управление пользователями через этот интерфейс")
}

func (d *DruidDriver) UpdateUser(ctx context.Context, username, password string, permissions []string) error {
	return fmt.Errorf("Druid не поддерживает управление пользователями через этот интерфейс")
}

func (d *DruidDriver) DeleteUser(ctx context.Context, username string) error {
	return fmt.Errorf("Druid не поддерживает управление пользователями через этот интерфейс")
}

