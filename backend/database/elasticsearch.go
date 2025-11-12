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

type ElasticsearchDriver struct {
	client *http.Client
	baseURL string
	conn   models.Connection
}

func NewElasticsearchDriver() *ElasticsearchDriver {
	return &ElasticsearchDriver{
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

func (d *ElasticsearchDriver) Connect(ctx context.Context, conn models.Connection) error {
	scheme := "http"
	if conn.SSL {
		scheme = "https"
	}
	d.baseURL = fmt.Sprintf("%s://%s:%s", scheme, conn.Host, conn.Port)
	d.conn = conn

	if err := d.Ping(ctx); err != nil {
		return fmt.Errorf("ошибка подключения к Elasticsearch: %w", err)
	}

	return nil
}

func (d *ElasticsearchDriver) Disconnect(ctx context.Context) error {
	d.client = nil
	d.baseURL = ""
	return nil
}

func (d *ElasticsearchDriver) IsConnected(ctx context.Context) bool {
	return d.baseURL != "" && d.Ping(ctx) == nil
}

func (d *ElasticsearchDriver) Ping(ctx context.Context) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	req, err := http.NewRequestWithContext(ctx, "GET", d.baseURL, nil)
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

func (d *ElasticsearchDriver) ExecuteQuery(ctx context.Context, query string) (*models.QueryResponse, error) {
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
		index = "_all"
	}

	url := fmt.Sprintf("%s/%s/_search", d.baseURL, index)
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

	hits, _ := result["hits"].(map[string]interface{})
	hitsList, _ := hits["hits"].([]interface{})

	columns := []string{"_id", "_source"}
	rowsData := make([]map[string]interface{}, 0)

	for _, hit := range hitsList {
		hitMap := hit.(map[string]interface{})
		row := make(map[string]interface{})
		row["_id"] = hitMap["_id"]
		
		if source, ok := hitMap["_source"].(map[string]interface{}); ok {
			for key, value := range source {
				if !contains(columns, key) {
					columns = append(columns, key)
				}
				row[key] = value
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

func (d *ElasticsearchDriver) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	settings := map[string]interface{}{
		"number_of_shards":   1,
		"number_of_replicas": 1,
	}

	if options != nil {
		if shards, ok := options["shards"].(float64); ok {
			settings["number_of_shards"] = int(shards)
		}
		if replicas, ok := options["replicas"].(float64); ok {
			settings["number_of_replicas"] = int(replicas)
		}
	}

	body := map[string]interface{}{"settings": settings}
	jsonBody, _ := json.Marshal(body)

	url := fmt.Sprintf("%s/%s", d.baseURL, name)
	req, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewBuffer(jsonBody))
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

func (d *ElasticsearchDriver) ListDatabases(ctx context.Context) ([]models.DatabaseInfo, error) {
	if d.baseURL == "" {
		return nil, fmt.Errorf("подключение не установлено")
	}

	url := fmt.Sprintf("%s/_cat/indices?format=json&h=index,store.size", d.baseURL)
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
	var indices []map[string]interface{}
	if err := json.Unmarshal(respBody, &indices); err != nil {
		return nil, fmt.Errorf("ошибка парсинга ответа: %w", err)
	}

	databases := make([]models.DatabaseInfo, 0)
	for _, idx := range indices {
		indexName, _ := idx["index"].(string)
		if indexName == "" || indexName[0] == '.' {
			continue
		}

		size, _ := idx["store.size"].(string)
		if size == "" {
			size = "N/A"
		}

		databases = append(databases, models.DatabaseInfo{
			Name: indexName,
			Size: size,
		})
	}

	return databases, nil
}

func (d *ElasticsearchDriver) UpdateDatabase(ctx context.Context, oldName, newName string, options map[string]interface{}) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	if newName != "" && newName != oldName {
		reindexURL := fmt.Sprintf("%s/_reindex", d.baseURL)
		reindexBody := map[string]interface{}{
			"source": map[string]interface{}{
				"index": oldName,
			},
			"dest": map[string]interface{}{
				"index": newName,
			},
		}

		jsonBody, _ := json.Marshal(reindexBody)
		req, err := http.NewRequestWithContext(ctx, "POST", reindexURL, bytes.NewBuffer(jsonBody))
		if err != nil {
			return fmt.Errorf("ошибка создания запроса: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		if d.conn.Username != "" {
			req.SetBasicAuth(d.conn.Username, d.conn.Password)
		}

		resp, err := d.client.Do(req)
		if err != nil {
			return fmt.Errorf("ошибка выполнения запроса: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return fmt.Errorf("ошибка переиндексации: статус %d, ответ: %s", resp.StatusCode, string(body))
		}

		deleteURL := fmt.Sprintf("%s/%s", d.baseURL, oldName)
		delReq, err := http.NewRequestWithContext(ctx, "DELETE", deleteURL, nil)
		if err == nil {
			if d.conn.Username != "" {
				delReq.SetBasicAuth(d.conn.Username, d.conn.Password)
			}
			d.client.Do(delReq)
		}
	}

	return nil
}

func (d *ElasticsearchDriver) DeleteDatabase(ctx context.Context, name string) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	url := fmt.Sprintf("%s/%s", d.baseURL, name)
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

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ошибка удаления индекса: статус %d, ответ: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (d *ElasticsearchDriver) CreateTable(ctx context.Context, name string, columns []models.TableColumn) error {
	return fmt.Errorf("Elasticsearch не поддерживает создание таблиц напрямую")
}

func (d *ElasticsearchDriver) ListTables(ctx context.Context) ([]models.TableInfo, error) {
	if d.baseURL == "" {
		return nil, fmt.Errorf("подключение не установлено")
	}

	url := fmt.Sprintf("%s/_cat/indices?format=json&h=index,docs.count,store.size", d.baseURL)
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
	var indices []map[string]interface{}
	if err := json.Unmarshal(respBody, &indices); err != nil {
		return nil, fmt.Errorf("ошибка парсинга ответа: %w", err)
	}

	tables := make([]models.TableInfo, 0)
	for _, idx := range indices {
		indexName, _ := idx["index"].(string)
		if indexName == "" || indexName[0] == '.' {
			continue
		}

		size, _ := idx["store.size"].(string)
		if size == "" {
			size = "N/A"
		}

		docsCount := int64(0)
		if countStr, ok := idx["docs.count"].(string); ok && countStr != "" {
			fmt.Sscanf(countStr, "%d", &docsCount)
		}

		tables = append(tables, models.TableInfo{
			Name:     indexName,
			Database: d.conn.Database,
			Size:     size,
			Rows:     docsCount,
		})
	}

	return tables, nil
}

func (d *ElasticsearchDriver) DeleteTable(ctx context.Context, name string) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	url := fmt.Sprintf("%s/%s", d.baseURL, name)
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

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ошибка удаления индекса: статус %d, ответ: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (d *ElasticsearchDriver) UpdateTable(ctx context.Context, oldName, newName string, columns []models.TableColumn) error {
	return fmt.Errorf("Elasticsearch не поддерживает переименование индексов напрямую. Используйте reindex API")
}

func (d *ElasticsearchDriver) CreateUser(ctx context.Context, username, password, database string, permissions []string) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	url := fmt.Sprintf("%s/_security/user/%s", d.baseURL, username)
	
	userData := map[string]interface{}{
		"password": password,
	}
	
	if len(permissions) > 0 {
		userData["roles"] = permissions
	} else {
		userData["roles"] = []string{}
	}

	jsonBody, _ := json.Marshal(userData)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return fmt.Errorf("ошибка создания запроса: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	if d.conn.Username != "" {
		req.SetBasicAuth(d.conn.Username, d.conn.Password)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("ошибка выполнения запроса: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("X-Pack Security не установлен или недостаточно прав доступа")
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ошибка создания пользователя: статус %d, ответ: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (d *ElasticsearchDriver) ListUsers(ctx context.Context) ([]models.UserInfo, error) {
	if d.baseURL == "" {
		return nil, fmt.Errorf("подключение не установлено")
	}

	url := fmt.Sprintf("%s/_security/user", d.baseURL)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("ошибка создания запроса: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	if d.conn.Username != "" {
		req.SetBasicAuth(d.conn.Username, d.conn.Password)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ошибка выполнения запроса: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusForbidden {
		return nil, fmt.Errorf("X-Pack Security не установлен или недостаточно прав доступа")
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ошибка получения пользователей: статус %d, ответ: %s", resp.StatusCode, string(body))
	}

	respBody, _ := io.ReadAll(resp.Body)
	var usersMap map[string]interface{}
	if err := json.Unmarshal(respBody, &usersMap); err != nil {
		return nil, fmt.Errorf("ошибка парсинга ответа: %w", err)
	}

	users := make([]models.UserInfo, 0)
	for username, userData := range usersMap {
		if userMap, ok := userData.(map[string]interface{}); ok {
			permissions := make([]string, 0)
			if roles, ok := userMap["roles"].([]interface{}); ok {
				for _, role := range roles {
					if roleStr, ok := role.(string); ok {
						permissions = append(permissions, roleStr)
					}
				}
			}

			isSuperuser := false
			for _, perm := range permissions {
				if perm == "superuser" || perm == "all" {
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
	}

	return users, nil
}

func (d *ElasticsearchDriver) UpdateUser(ctx context.Context, username, password string, permissions []string) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	url := fmt.Sprintf("%s/_security/user/%s", d.baseURL, username)
	
	updateData := make(map[string]interface{})
	if password != "" {
		updateData["password"] = password
	}
	if permissions != nil {
		updateData["roles"] = permissions
	}

	jsonBody, _ := json.Marshal(updateData)
	req, err := http.NewRequestWithContext(ctx, "PUT", url, bytes.NewBuffer(jsonBody))
	if err != nil {
		return fmt.Errorf("ошибка создания запроса: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	if d.conn.Username != "" {
		req.SetBasicAuth(d.conn.Username, d.conn.Password)
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return fmt.Errorf("ошибка выполнения запроса: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("X-Pack Security не установлен или недостаточно прав доступа")
	}

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ошибка обновления пользователя: статус %d, ответ: %s", resp.StatusCode, string(body))
	}

	return nil
}

func (d *ElasticsearchDriver) DeleteUser(ctx context.Context, username string) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	url := fmt.Sprintf("%s/_security/user/%s", d.baseURL, username)
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

	if resp.StatusCode == http.StatusNotFound || resp.StatusCode == http.StatusForbidden {
		return fmt.Errorf("X-Pack Security не установлен или недостаточно прав доступа")
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ошибка удаления пользователя: статус %d, ответ: %s", resp.StatusCode, string(body))
	}

	return nil
}

