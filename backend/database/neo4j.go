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

type Neo4jDriver struct {
	client  *http.Client
	baseURL string
	conn    models.Connection
}

func NewNeo4jDriver() *Neo4jDriver {
	return &Neo4jDriver{
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

func (d *Neo4jDriver) Connect(ctx context.Context, conn models.Connection) error {
	scheme := "http"
	if conn.SSL {
		scheme = "https"
	}
	d.baseURL = fmt.Sprintf("%s://%s:%s", scheme, conn.Host, conn.Port)
	d.conn = conn

	if err := d.Ping(ctx); err != nil {
		return fmt.Errorf("ошибка подключения к Neo4j: %w", err)
	}

	return nil
}

func (d *Neo4jDriver) Disconnect(ctx context.Context) error {
	d.client = nil
	d.baseURL = ""
	return nil
}

func (d *Neo4jDriver) IsConnected(ctx context.Context) bool {
	return d.baseURL != "" && d.Ping(ctx) == nil
}

func (d *Neo4jDriver) Ping(ctx context.Context) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	pingURL := fmt.Sprintf("%s/db/%s", d.baseURL, d.getDatabase())
	req, err := http.NewRequestWithContext(ctx, "GET", pingURL, nil)
	if err != nil {
		return err
	}

	d.setAuth(req)

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

func (d *Neo4jDriver) getDatabase() string {
	if d.conn.Database != "" {
		return d.conn.Database
	}
	return "neo4j"
}

func (d *Neo4jDriver) setAuth(req *http.Request) {
	if d.conn.Username != "" {
		req.SetBasicAuth(d.conn.Username, d.conn.Password)
	}
}

func (d *Neo4jDriver) ExecuteQuery(ctx context.Context, query string) (*models.QueryResponse, error) {
	if d.baseURL == "" {
		return nil, fmt.Errorf("подключение не установлено")
	}

	startTime := time.Now()

	queryURL := fmt.Sprintf("%s/db/%s/tx/commit", d.baseURL, d.getDatabase())

	requestBody := map[string]interface{}{
		"statements": []map[string]interface{}{
			{
				"statement": query,
			},
		},
	}

	jsonBody, _ := json.Marshal(requestBody)
	req, err := http.NewRequestWithContext(ctx, "POST", queryURL, bytes.NewBuffer(jsonBody))
	if err != nil {
		return &models.QueryResponse{Error: err.Error()}, nil
	}
	req.Header.Set("Content-Type", "application/json")
	d.setAuth(req)

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

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return &models.QueryResponse{Error: err.Error()}, nil
	}

	columns := []string{}
	rowsData := make([]map[string]interface{}, 0)

	if results, ok := result["results"].([]interface{}); ok && len(results) > 0 {
		if firstResult, ok := results[0].(map[string]interface{}); ok {
			if data, ok := firstResult["data"].([]interface{}); ok {
				if len(data) > 0 {
					if firstData, ok := data[0].(map[string]interface{}); ok {
						if meta, ok := firstData["meta"].([]interface{}); ok {
							for _, m := range meta {
								if metaMap, ok := m.(map[string]interface{}); ok {
									if col, ok := metaMap["name"].(string); ok {
										columns = append(columns, col)
									}
								}
							}
						}
					}
				}

				for _, dataItem := range data {
					if dataMap, ok := dataItem.(map[string]interface{}); ok {
						if row, ok := dataMap["row"].([]interface{}); ok {
							rowData := make(map[string]interface{})
							for i, col := range columns {
								if i < len(row) {
									rowData[col] = row[i]
								}
							}
							rowsData = append(rowsData, rowData)
						}
					}
				}
			}
		}
	}

	if len(columns) == 0 {
		columns = []string{"result"}
		rowsData = append(rowsData, map[string]interface{}{
			"result": "Запрос выполнен успешно",
		})
	}

	executionTime := time.Since(startTime).Milliseconds()

	return &models.QueryResponse{
		Columns:       columns,
		Rows:          rowsData,
		RowCount:      len(rowsData),
		ExecutionTime: executionTime,
	}, nil
}

func (d *Neo4jDriver) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	queryURL := fmt.Sprintf("%s/db/neo4j/tx/commit", d.baseURL)
	query := fmt.Sprintf("CREATE DATABASE %s IF NOT EXISTS", name)

	requestBody := map[string]interface{}{
		"statements": []map[string]interface{}{
			{
				"statement": query,
			},
		},
	}

	jsonBody, _ := json.Marshal(requestBody)
	req, err := http.NewRequestWithContext(ctx, "POST", queryURL, bytes.NewBuffer(jsonBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	d.setAuth(req)

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ошибка создания базы данных: %s", string(body))
	}

	return nil
}

func (d *Neo4jDriver) ListDatabases(ctx context.Context) ([]models.DatabaseInfo, error) {
	if d.baseURL == "" {
		return nil, fmt.Errorf("подключение не установлено")
	}

	queryURL := fmt.Sprintf("%s/db/neo4j/tx/commit", d.baseURL)
	query := "SHOW DATABASES"

	requestBody := map[string]interface{}{
		"statements": []map[string]interface{}{
			{
				"statement": query,
			},
		},
	}

	jsonBody, _ := json.Marshal(requestBody)
	req, err := http.NewRequestWithContext(ctx, "POST", queryURL, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	d.setAuth(req)

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, err
	}

	databases := make([]models.DatabaseInfo, 0)
	if results, ok := result["results"].([]interface{}); ok && len(results) > 0 {
		if firstResult, ok := results[0].(map[string]interface{}); ok {
			if data, ok := firstResult["data"].([]interface{}); ok {
				for _, dataItem := range data {
					if dataMap, ok := dataItem.(map[string]interface{}); ok {
						if row, ok := dataMap["row"].([]interface{}); ok && len(row) > 0 {
							if dbName, ok := row[0].(string); ok {
								databases = append(databases, models.DatabaseInfo{
									Name: dbName,
								})
							}
						}
					}
				}
			}
		}
	}

	return databases, nil
}

func (d *Neo4jDriver) UpdateDatabase(ctx context.Context, oldName, newName string, options map[string]interface{}) error {
	return fmt.Errorf("Neo4j не поддерживает переименование баз данных")
}

func (d *Neo4jDriver) DeleteDatabase(ctx context.Context, name string) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	queryURL := fmt.Sprintf("%s/db/neo4j/tx/commit", d.baseURL)
	query := fmt.Sprintf("DROP DATABASE %s IF EXISTS", name)

	requestBody := map[string]interface{}{
		"statements": []map[string]interface{}{
			{
				"statement": query,
			},
		},
	}

	jsonBody, _ := json.Marshal(requestBody)
	req, err := http.NewRequestWithContext(ctx, "POST", queryURL, bytes.NewBuffer(jsonBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	d.setAuth(req)

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ошибка удаления базы данных: %s", string(body))
	}

	return nil
}

func (d *Neo4jDriver) CreateTable(ctx context.Context, name string, columns []models.TableColumn) error {
	return fmt.Errorf("Neo4j не поддерживает создание таблиц. Используйте узлы и связи")
}

func (d *Neo4jDriver) ListTables(ctx context.Context) ([]models.TableInfo, error) {
	if d.baseURL == "" {
		return nil, fmt.Errorf("подключение не установлено")
	}

	queryURL := fmt.Sprintf("%s/db/%s/tx/commit", d.baseURL, d.getDatabase())
	query := "CALL db.labels()"

	requestBody := map[string]interface{}{
		"statements": []map[string]interface{}{
			{
				"statement": query,
			},
		},
	}

	jsonBody, _ := json.Marshal(requestBody)
	req, err := http.NewRequestWithContext(ctx, "POST", queryURL, bytes.NewBuffer(jsonBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	d.setAuth(req)

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, err
	}

	tables := make([]models.TableInfo, 0)
	if results, ok := result["results"].([]interface{}); ok && len(results) > 0 {
		if firstResult, ok := results[0].(map[string]interface{}); ok {
			if data, ok := firstResult["data"].([]interface{}); ok {
				for _, dataItem := range data {
					if dataMap, ok := dataItem.(map[string]interface{}); ok {
						if row, ok := dataMap["row"].([]interface{}); ok {
							for _, label := range row {
								if labelStr, ok := label.(string); ok {
									tables = append(tables, models.TableInfo{
										Name:     labelStr,
										Database: d.getDatabase(),
									})
								}
							}
						}
					}
				}
			}
		}
	}

	return tables, nil
}

func (d *Neo4jDriver) DeleteTable(ctx context.Context, name string) error {
	return fmt.Errorf("Neo4j не поддерживает удаление меток напрямую")
}

func (d *Neo4jDriver) UpdateTable(ctx context.Context, oldName, newName string, columns []models.TableColumn) error {
	return fmt.Errorf("Neo4j не поддерживает переименование меток")
}

func (d *Neo4jDriver) CreateUser(ctx context.Context, username, password, database string, permissions []string) error {
	return fmt.Errorf("Neo4j не поддерживает управление пользователями через этот интерфейс")
}

func (d *Neo4jDriver) ListUsers(ctx context.Context) ([]models.UserInfo, error) {
	return nil, fmt.Errorf("Neo4j не поддерживает управление пользователями через этот интерфейс")
}

func (d *Neo4jDriver) UpdateUser(ctx context.Context, username, password string, permissions []string) error {
	return fmt.Errorf("Neo4j не поддерживает управление пользователями через этот интерфейс")
}

func (d *Neo4jDriver) DeleteUser(ctx context.Context, username string) error {
	return fmt.Errorf("Neo4j не поддерживает управление пользователями через этот интерфейс")
}

