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

type CouchbaseDriver struct {
	client  *http.Client
	baseURL string
	conn    models.Connection
}

func NewCouchbaseDriver() *CouchbaseDriver {
	return &CouchbaseDriver{
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

func (d *CouchbaseDriver) Connect(ctx context.Context, conn models.Connection) error {
	scheme := "http"
	if conn.SSL {
		scheme = "https"
	}
	d.baseURL = fmt.Sprintf("%s://%s:%s", scheme, conn.Host, conn.Port)
	d.conn = conn

	if err := d.Ping(ctx); err != nil {
		return fmt.Errorf("ошибка подключения к Couchbase: %w", err)
	}

	return nil
}

func (d *CouchbaseDriver) Disconnect(ctx context.Context) error {
	d.client = nil
	d.baseURL = ""
	return nil
}

func (d *CouchbaseDriver) IsConnected(ctx context.Context) bool {
	return d.baseURL != "" && d.Ping(ctx) == nil
}

func (d *CouchbaseDriver) Ping(ctx context.Context) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	pingURL := fmt.Sprintf("%s/pools", d.baseURL)
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

func (d *CouchbaseDriver) ExecuteQuery(ctx context.Context, query string) (*models.QueryResponse, error) {
	if d.baseURL == "" {
		return nil, fmt.Errorf("подключение не установлено")
	}

	startTime := time.Now()

	queryURL := fmt.Sprintf("%s/query/service", d.baseURL)
	
	requestBody := map[string]interface{}{
		"statement": query,
	}
	if d.conn.Database != "" {
		requestBody["use_legacy_alias"] = false
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

	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return &models.QueryResponse{Error: err.Error()}, nil
	}

	columns := []string{}
	rowsData := make([]map[string]interface{}, 0)

	if results, ok := result["results"].([]interface{}); ok {
		if len(results) > 0 {
			if firstResult, ok := results[0].(map[string]interface{}); ok {
				for key := range firstResult {
					columns = append(columns, key)
				}
			}
		}

		for _, res := range results {
			if resMap, ok := res.(map[string]interface{}); ok {
				rowsData = append(rowsData, resMap)
			}
		}
	}

	executionTime := time.Since(startTime).Milliseconds()

	return &models.QueryResponse{
		Columns:       columns,
		Rows:          rowsData,
		RowCount:      len(rowsData),
		ExecutionTime: executionTime,
	}, nil
}

func (d *CouchbaseDriver) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	bucketURL := fmt.Sprintf("%s/pools/default/buckets", d.baseURL)
	
	body := map[string]interface{}{
		"name":         name,
		"bucketType":   "couchbase",
		"ramQuotaMB":   100,
		"replicaNumber": 1,
	}

	if options != nil {
		if ramQuota, ok := options["ramQuotaMB"].(float64); ok {
			body["ramQuotaMB"] = int(ramQuota)
		}
		if replicaNum, ok := options["replicaNumber"].(float64); ok {
			body["replicaNumber"] = int(replicaNum)
		}
	}

	jsonBody, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, "POST", bucketURL, bytes.NewBuffer(jsonBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")

	if d.conn.Username != "" {
		req.SetBasicAuth(d.conn.Username, d.conn.Password)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ошибка создания bucket: %s", string(body))
	}

	return nil
}

func (d *CouchbaseDriver) ListDatabases(ctx context.Context) ([]models.DatabaseInfo, error) {
	if d.baseURL == "" {
		return nil, fmt.Errorf("подключение не установлено")
	}

	bucketURL := fmt.Sprintf("%s/pools/default/buckets", d.baseURL)
	req, err := http.NewRequestWithContext(ctx, "GET", bucketURL, nil)
	if err != nil {
		return nil, err
	}

	if d.conn.Username != "" {
		req.SetBasicAuth(d.conn.Username, d.conn.Password)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	respBody, _ := io.ReadAll(resp.Body)
	var buckets []map[string]interface{}
	if err := json.Unmarshal(respBody, &buckets); err != nil {
		return nil, err
	}

	databases := make([]models.DatabaseInfo, 0)
	for _, bucket := range buckets {
		if name, ok := bucket["name"].(string); ok {
			size := "N/A"
			if quota, ok := bucket["quota"].(map[string]interface{}); ok {
				if ram, ok := quota["ram"].(float64); ok {
					size = fmt.Sprintf("%.0f MB", ram/(1024*1024))
				}
			}
			databases = append(databases, models.DatabaseInfo{
				Name: name,
				Size: size,
			})
		}
	}

	return databases, nil
}

func (d *CouchbaseDriver) UpdateDatabase(ctx context.Context, oldName, newName string, options map[string]interface{}) error {
	return fmt.Errorf("Couchbase не поддерживает переименование buckets")
}

func (d *CouchbaseDriver) DeleteDatabase(ctx context.Context, name string) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	bucketURL := fmt.Sprintf("%s/pools/default/buckets/%s", d.baseURL, name)
	req, err := http.NewRequestWithContext(ctx, "DELETE", bucketURL, nil)
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
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ошибка удаления bucket: %s", string(body))
	}

	return nil
}

func (d *CouchbaseDriver) CreateTable(ctx context.Context, name string, columns []models.TableColumn) error {
	return fmt.Errorf("Couchbase не поддерживает создание таблиц напрямую. Используйте коллекции")
}

func (d *CouchbaseDriver) ListTables(ctx context.Context) ([]models.TableInfo, error) {
	if d.baseURL == "" {
		return nil, fmt.Errorf("подключение не установлено")
	}

	queryURL := fmt.Sprintf("%s/query/service", d.baseURL)
	query := "SELECT name FROM system:keyspaces"

	requestBody := map[string]interface{}{
		"statement": query,
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
	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, err
	}

	tables := make([]models.TableInfo, 0)
	if results, ok := result["results"].([]interface{}); ok {
		for _, res := range results {
			if resMap, ok := res.(map[string]interface{}); ok {
				if name, ok := resMap["name"].(string); ok {
					tables = append(tables, models.TableInfo{
						Name:     name,
						Database: d.conn.Database,
					})
				}
			}
		}
	}

	return tables, nil
}

func (d *CouchbaseDriver) DeleteTable(ctx context.Context, name string) error {
	return fmt.Errorf("Couchbase не поддерживает удаление коллекций через этот интерфейс")
}

func (d *CouchbaseDriver) UpdateTable(ctx context.Context, oldName, newName string, columns []models.TableColumn) error {
	return fmt.Errorf("Couchbase не поддерживает переименование коллекций")
}

func (d *CouchbaseDriver) CreateUser(ctx context.Context, username, password, database string, permissions []string) error {
	return fmt.Errorf("Couchbase не поддерживает управление пользователями через этот интерфейс")
}

func (d *CouchbaseDriver) ListUsers(ctx context.Context) ([]models.UserInfo, error) {
	return nil, fmt.Errorf("Couchbase не поддерживает управление пользователями через этот интерфейс")
}

func (d *CouchbaseDriver) UpdateUser(ctx context.Context, username, password string, permissions []string) error {
	return fmt.Errorf("Couchbase не поддерживает управление пользователями через этот интерфейс")
}

func (d *CouchbaseDriver) DeleteUser(ctx context.Context, username string) error {
	return fmt.Errorf("Couchbase не поддерживает управление пользователями через этот интерфейс")
}

