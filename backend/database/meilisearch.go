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

type MeilisearchDriver struct {
	client *http.Client
	baseURL string
	conn   models.Connection
}

func NewMeilisearchDriver() *MeilisearchDriver {
	return &MeilisearchDriver{
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

func (d *MeilisearchDriver) Connect(ctx context.Context, conn models.Connection) error {
	scheme := "http"
	if conn.SSL {
		scheme = "https"
	}
	d.baseURL = fmt.Sprintf("%s://%s:%s", scheme, conn.Host, conn.Port)
	d.conn = conn

	if err := d.Ping(ctx); err != nil {
		return fmt.Errorf("ошибка подключения к Meilisearch: %w", err)
	}

	return nil
}

func (d *MeilisearchDriver) Disconnect(ctx context.Context) error {
	d.client = nil
	d.baseURL = ""
	return nil
}

func (d *MeilisearchDriver) IsConnected(ctx context.Context) bool {
	return d.baseURL != "" && d.Ping(ctx) == nil
}

func (d *MeilisearchDriver) Ping(ctx context.Context) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	req, err := http.NewRequestWithContext(ctx, "GET", fmt.Sprintf("%s/health", d.baseURL), nil)
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

func (d *MeilisearchDriver) ExecuteQuery(ctx context.Context, query string) (*models.QueryResponse, error) {
	if d.baseURL == "" {
		return nil, fmt.Errorf("подключение не установлено")
	}

	startTime := time.Now()

	var searchQuery map[string]interface{}
	if err := json.Unmarshal([]byte(query), &searchQuery); err != nil {
		return &models.QueryResponse{
			Error: fmt.Sprintf("ошибка парсинга запроса: %v", err),
		}, nil
	}

	index := d.conn.Database
	if index == "" {
		return &models.QueryResponse{
			Error: "необходимо указать индекс в поле database подключения",
		}, nil
	}

	url := fmt.Sprintf("%s/indexes/%s/search", d.baseURL, index)
	body, _ := json.Marshal(searchQuery)

	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(body))
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

	hits, _ := result["hits"].([]interface{})

	columns := []string{}
	rowsData := make([]map[string]interface{}, 0)

	if len(hits) > 0 {
		if firstHit, ok := hits[0].(map[string]interface{}); ok {
			for key := range firstHit {
				columns = append(columns, key)
			}
		}
	}

	for _, hit := range hits {
		if hitMap, ok := hit.(map[string]interface{}); ok {
			row := make(map[string]interface{})
			for key, value := range hitMap {
				row[key] = value
			}
			rowsData = append(rowsData, row)
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

func (d *MeilisearchDriver) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	body := map[string]interface{}{
		"uid": name,
	}

	if options != nil {
		if primaryKey, ok := options["primaryKey"].(string); ok {
			body["primaryKey"] = primaryKey
		}
	}

	jsonBody, _ := json.Marshal(body)

	url := fmt.Sprintf("%s/indexes", d.baseURL)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	if d.conn.Username != "" {
		req.SetBasicAuth(d.conn.Username, d.conn.Password)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ошибка создания индекса: %s", string(body))
	}

	return nil
}

func (d *MeilisearchDriver) ListDatabases(ctx context.Context) ([]models.DatabaseInfo, error) {
	if d.baseURL == "" {
		return nil, fmt.Errorf("подключение не установлено")
	}

	url := fmt.Sprintf("%s/indexes", d.baseURL)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("ошибка создания запроса: %w", err)
	}

	if d.conn.Username != "" {
		req.SetBasicAuth(d.conn.Username, d.conn.Password)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ошибка выполнения запроса: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ошибка получения списка индексов: статус %d, ответ: %s", resp.StatusCode, string(body))
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("ошибка парсинга ответа: %w", err)
	}

	indices, _ := result["results"].([]interface{})

	databases := make([]models.DatabaseInfo, 0)
	for _, idx := range indices {
		if idxMap, ok := idx.(map[string]interface{}); ok {
			indexName, _ := idxMap["uid"].(string)
			if indexName == "" {
				continue
			}

			size := "N/A"
			if primaryKey, ok := idxMap["primaryKey"].(string); ok && primaryKey != "" {
				size = fmt.Sprintf("PrimaryKey: %s", primaryKey)
			}
			if stats, ok := idxMap["stats"].(map[string]interface{}); ok {
				if numberOfDocuments, ok := stats["numberOfDocuments"].(float64); ok {
					size = fmt.Sprintf("%.0f документов", numberOfDocuments)
				}
			}

			databases = append(databases, models.DatabaseInfo{
				Name: indexName,
				Size: size,
			})
		}
	}

	return databases, nil
}

func (d *MeilisearchDriver) UpdateDatabase(ctx context.Context, oldName, newName string, options map[string]interface{}) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	if newName != "" && newName != oldName {
		updateURL := fmt.Sprintf("%s/indexes/%s", d.baseURL, oldName)
		updateBody := map[string]interface{}{
			"primaryKey": options["primaryKey"],
		}

		if options != nil && options["primaryKey"] != nil {
			jsonBody, _ := json.Marshal(updateBody)
			req, err := http.NewRequestWithContext(ctx, "PATCH", updateURL, bytes.NewBuffer(jsonBody))
			if err == nil {
				req.Header.Set("Content-Type", "application/json")
				if d.conn.Username != "" {
					req.SetBasicAuth(d.conn.Username, d.conn.Password)
				}
				d.client.Do(req)
			}
		}

		return fmt.Errorf("Meilisearch не поддерживает переименование индексов. Создайте новый индекс и переиндексируйте данные")
	}

	return nil
}

func (d *MeilisearchDriver) DeleteDatabase(ctx context.Context, name string) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	url := fmt.Sprintf("%s/indexes/%s", d.baseURL, name)
	req, err := http.NewRequestWithContext(ctx, "DELETE", url, nil)
	if err != nil {
		return fmt.Errorf("ошибка создания запроса: %w", err)
	}

	if d.conn.Username != "" {
		req.SetBasicAuth(d.conn.Username, d.conn.Password)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("ошибка выполнения запроса: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ошибка удаления индекса: статус %d, ответ: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (d *MeilisearchDriver) CreateTable(ctx context.Context, name string, columns []models.TableColumn) error {
	return fmt.Errorf("Meilisearch не поддерживает создание таблиц напрямую. Используйте создание индекса")
}

func (d *MeilisearchDriver) ListTables(ctx context.Context) ([]models.TableInfo, error) {
	if d.baseURL == "" {
		return nil, fmt.Errorf("подключение не установлено")
	}

	url := fmt.Sprintf("%s/indexes", d.baseURL)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("ошибка создания запроса: %w", err)
	}

	if d.conn.Username != "" {
		req.SetBasicAuth(d.conn.Username, d.conn.Password)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ошибка выполнения запроса: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ошибка получения списка индексов: статус %d, ответ: %s", resp.StatusCode, string(body))
	}

	respBody, _ := io.ReadAll(resp.Body)
	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, fmt.Errorf("ошибка парсинга ответа: %w", err)
	}

	indices, _ := result["results"].([]interface{})

	tables := make([]models.TableInfo, 0)
	for _, idx := range indices {
		if idxMap, ok := idx.(map[string]interface{}); ok {
			indexName, _ := idxMap["uid"].(string)
			if indexName == "" {
				continue
			}

			size := "N/A"
			docsCount := int64(0)
			if stats, ok := idxMap["stats"].(map[string]interface{}); ok {
				if numberOfDocuments, ok := stats["numberOfDocuments"].(float64); ok {
					docsCount = int64(numberOfDocuments)
					size = fmt.Sprintf("%.0f документов", numberOfDocuments)
				}
			} else {
				if primaryKey, ok := idxMap["primaryKey"].(string); ok && primaryKey != "" {
					size = fmt.Sprintf("PrimaryKey: %s", primaryKey)
				}
			}

			tables = append(tables, models.TableInfo{
				Name:     indexName,
				Database: d.conn.Database,
				Size:     size,
				Rows:     docsCount,
			})
		}
	}

	return tables, nil
}

func (d *MeilisearchDriver) DeleteTable(ctx context.Context, name string) error {
	return d.DeleteDatabase(ctx, name)
}

func (d *MeilisearchDriver) UpdateTable(ctx context.Context, oldName, newName string, columns []models.TableColumn) error {
	return fmt.Errorf("Meilisearch не поддерживает переименование индексов напрямую")
}

func (d *MeilisearchDriver) CreateUser(ctx context.Context, username, password, database string, permissions []string) error {
	return fmt.Errorf("Meilisearch не поддерживает управление пользователями через API")
}

func (d *MeilisearchDriver) ListUsers(ctx context.Context) ([]models.UserInfo, error) {
	return nil, fmt.Errorf("Meilisearch не поддерживает управление пользователями через API")
}

func (d *MeilisearchDriver) UpdateUser(ctx context.Context, username, password string, permissions []string) error {
	return fmt.Errorf("Meilisearch не поддерживает управление пользователями через API")
}

func (d *MeilisearchDriver) DeleteUser(ctx context.Context, username string) error {
	return fmt.Errorf("Meilisearch не поддерживает управление пользователями через API")
}

