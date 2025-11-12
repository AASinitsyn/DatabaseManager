package database

import (
	"bytes"
	"context"
	"database-manager/models"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type InfluxDBDriver struct {
	client   *http.Client
	baseURL  string
	conn     models.Connection
	version  string
}

func NewInfluxDBDriver() *InfluxDBDriver {
	return &InfluxDBDriver{
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

func (d *InfluxDBDriver) Connect(ctx context.Context, conn models.Connection) error {
	scheme := "http"
	if conn.SSL {
		scheme = "https"
	}
	d.baseURL = fmt.Sprintf("%s://%s:%s", scheme, conn.Host, conn.Port)
	d.conn = conn

	if err := d.detectVersion(ctx); err != nil {
		return fmt.Errorf("ошибка определения версии InfluxDB: %w", err)
	}

	if err := d.Ping(ctx); err != nil {
		return fmt.Errorf("ошибка подключения к InfluxDB: %w", err)
	}

	return nil
}

func (d *InfluxDBDriver) detectVersion(ctx context.Context) error {
	pingURL := fmt.Sprintf("%s/ping", d.baseURL)
	req, err := http.NewRequestWithContext(ctx, "GET", pingURL, nil)
	if err != nil {
		return err
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	version := resp.Header.Get("X-Influxdb-Version")
	if version != "" {
		if strings.HasPrefix(version, "2.") {
			d.version = "2"
		} else {
			d.version = "1"
		}
	} else {
		d.version = "1"
	}

	return nil
}

func (d *InfluxDBDriver) Disconnect(ctx context.Context) error {
	d.client = nil
	d.baseURL = ""
	return nil
}

func (d *InfluxDBDriver) IsConnected(ctx context.Context) bool {
	return d.baseURL != "" && d.Ping(ctx) == nil
}

func (d *InfluxDBDriver) Ping(ctx context.Context) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	pingURL := fmt.Sprintf("%s/ping", d.baseURL)
	req, err := http.NewRequestWithContext(ctx, "GET", pingURL, nil)
	if err != nil {
		return err
	}

	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusOK {
		return fmt.Errorf("ошибка ping: статус %d", resp.StatusCode)
	}

	return nil
}

func (d *InfluxDBDriver) ExecuteQuery(ctx context.Context, query string) (*models.QueryResponse, error) {
	if d.baseURL == "" {
		return nil, fmt.Errorf("подключение не установлено")
	}

	if d.version == "2" {
		return d.executeQueryV2(ctx, query)
	}
	return d.executeQueryV1(ctx, query)
}

func (d *InfluxDBDriver) executeQueryV1(ctx context.Context, query string) (*models.QueryResponse, error) {
	startTime := time.Now()
	queryURL := fmt.Sprintf("%s/query", d.baseURL)
	params := url.Values{}
	params.Set("db", d.conn.Database)
	params.Set("q", query)
	if d.conn.Username != "" {
		params.Set("u", d.conn.Username)
		params.Set("p", d.conn.Password)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", queryURL+"?"+params.Encode(), nil)
	if err != nil {
		return &models.QueryResponse{Error: err.Error()}, nil
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

	if results, ok := result["results"].([]interface{}); ok && len(results) > 0 {
		if firstResult, ok := results[0].(map[string]interface{}); ok {
			if series, ok := firstResult["series"].([]interface{}); ok {
				for _, s := range series {
					if ser, ok := s.(map[string]interface{}); ok {
						if cols, ok := ser["columns"].([]interface{}); ok {
							for _, col := range cols {
								if colStr, ok := col.(string); ok {
									columns = append(columns, colStr)
								}
							}
						}
						if values, ok := ser["values"].([]interface{}); ok {
							for _, valRow := range values {
								if valArray, ok := valRow.([]interface{}); ok {
									row := make(map[string]interface{})
									for i, col := range columns {
										if i < len(valArray) {
											row[col] = valArray[i]
										}
									}
									rowsData = append(rowsData, row)
								}
							}
						}
					}
				}
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

func (d *InfluxDBDriver) executeQueryV2(ctx context.Context, query string) (*models.QueryResponse, error) {
	startTime := time.Now()
	queryURL := fmt.Sprintf("%s/api/v2/query", d.baseURL)
	
	org := d.conn.Database
	if org == "" {
		org = "my-org"
	}

	body := map[string]interface{}{
		"query": query,
		"org":   org,
	}

	jsonBody, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, "POST", queryURL, bytes.NewBuffer(jsonBody))
	if err != nil {
		return &models.QueryResponse{Error: err.Error()}, nil
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/csv")

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

	columns := []string{}
	rowsData := make([]map[string]interface{}, 0)

	lines := strings.Split(string(respBody), "\n")
	if len(lines) > 1 {
		header := strings.Split(lines[0], ",")
		columns = header
		for _, line := range lines[1:] {
			if line == "" {
				continue
			}
			values := strings.Split(line, ",")
			row := make(map[string]interface{})
			for i, col := range columns {
				if i < len(values) {
					row[col] = values[i]
				}
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

func (d *InfluxDBDriver) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	if d.version == "2" {
		return d.createDatabaseV2(ctx, name, options)
	}
	return d.createDatabaseV1(ctx, name, options)
}

func (d *InfluxDBDriver) createDatabaseV1(ctx context.Context, name string, options map[string]interface{}) error {
	queryURL := fmt.Sprintf("%s/query", d.baseURL)
	query := fmt.Sprintf("CREATE DATABASE %s", name)
	params := url.Values{}
	params.Set("q", query)
	if d.conn.Username != "" {
		params.Set("u", d.conn.Username)
		params.Set("p", d.conn.Password)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", queryURL+"?"+params.Encode(), nil)
	if err != nil {
		return err
	}

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

func (d *InfluxDBDriver) createDatabaseV2(ctx context.Context, name string, options map[string]interface{}) error {
	bucketURL := fmt.Sprintf("%s/api/v2/buckets", d.baseURL)
	
	org := d.conn.Database
	if org == "" {
		org = "my-org"
	}

	body := map[string]interface{}{
		"name": name,
		"org":  org,
	}

	jsonBody, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, "POST", bucketURL, bytes.NewBuffer(jsonBody))
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

	if resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ошибка создания bucket: %s", string(body))
	}

	return nil
}

func (d *InfluxDBDriver) ListDatabases(ctx context.Context) ([]models.DatabaseInfo, error) {
	if d.baseURL == "" {
		return nil, fmt.Errorf("подключение не установлено")
	}

	if d.version == "2" {
		return d.listDatabasesV2(ctx)
	}
	return d.listDatabasesV1(ctx)
}

func (d *InfluxDBDriver) listDatabasesV1(ctx context.Context) ([]models.DatabaseInfo, error) {
	queryURL := fmt.Sprintf("%s/query", d.baseURL)
	params := url.Values{}
	params.Set("q", "SHOW DATABASES")
	if d.conn.Username != "" {
		params.Set("u", d.conn.Username)
		params.Set("p", d.conn.Password)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", queryURL+"?"+params.Encode(), nil)
	if err != nil {
		return nil, err
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

	databases := make([]models.DatabaseInfo, 0)
	if results, ok := result["results"].([]interface{}); ok && len(results) > 0 {
		if firstResult, ok := results[0].(map[string]interface{}); ok {
			if series, ok := firstResult["series"].([]interface{}); ok && len(series) > 0 {
				if ser, ok := series[0].(map[string]interface{}); ok {
					if values, ok := ser["values"].([]interface{}); ok {
						for _, valRow := range values {
							if valArray, ok := valRow.([]interface{}); ok && len(valArray) > 0 {
								if dbName, ok := valArray[0].(string); ok {
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
	}

	return databases, nil
}

func (d *InfluxDBDriver) listDatabasesV2(ctx context.Context) ([]models.DatabaseInfo, error) {
	bucketURL := fmt.Sprintf("%s/api/v2/buckets", d.baseURL)
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
			databases = append(databases, models.DatabaseInfo{
				Name: name,
			})
		}
	}

	return databases, nil
}

func (d *InfluxDBDriver) UpdateDatabase(ctx context.Context, oldName, newName string, options map[string]interface{}) error {
	return fmt.Errorf("InfluxDB не поддерживает переименование баз данных")
}

func (d *InfluxDBDriver) DeleteDatabase(ctx context.Context, name string) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	if d.version == "2" {
		return d.deleteDatabaseV2(ctx, name)
	}
	return d.deleteDatabaseV1(ctx, name)
}

func (d *InfluxDBDriver) deleteDatabaseV1(ctx context.Context, name string) error {
	queryURL := fmt.Sprintf("%s/query", d.baseURL)
	query := fmt.Sprintf("DROP DATABASE %s", name)
	params := url.Values{}
	params.Set("q", query)
	if d.conn.Username != "" {
		params.Set("u", d.conn.Username)
		params.Set("p", d.conn.Password)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", queryURL+"?"+params.Encode(), nil)
	if err != nil {
		return err
	}

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

func (d *InfluxDBDriver) deleteDatabaseV2(ctx context.Context, name string) error {
	bucketURL := fmt.Sprintf("%s/api/v2/buckets", d.baseURL)
	req, err := http.NewRequestWithContext(ctx, "GET", bucketURL, nil)
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

	respBody, _ := io.ReadAll(resp.Body)
	var buckets []map[string]interface{}
	if err := json.Unmarshal(respBody, &buckets); err != nil {
		return err
	}

	var bucketID string
	for _, bucket := range buckets {
		if bucketName, ok := bucket["name"].(string); ok && bucketName == name {
			if id, ok := bucket["id"].(string); ok {
				bucketID = id
				break
			}
		}
	}

	if bucketID == "" {
		return fmt.Errorf("bucket не найден")
	}

	deleteURL := fmt.Sprintf("%s/api/v2/buckets/%s", d.baseURL, bucketID)
	delReq, err := http.NewRequestWithContext(ctx, "DELETE", deleteURL, nil)
	if err != nil {
		return err
	}

	if d.conn.Username != "" {
		delReq.SetBasicAuth(d.conn.Username, d.conn.Password)
	}

	delResp, err := d.client.Do(delReq)
	if err != nil {
		return err
	}
	defer delResp.Body.Close()

	if delResp.StatusCode != http.StatusNoContent && delResp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(delResp.Body)
		return fmt.Errorf("ошибка удаления bucket: %s", string(body))
	}

	return nil
}

func (d *InfluxDBDriver) CreateTable(ctx context.Context, name string, columns []models.TableColumn) error {
	return fmt.Errorf("InfluxDB не поддерживает создание таблиц напрямую. Используйте измерения (measurements)")
}

func (d *InfluxDBDriver) ListTables(ctx context.Context) ([]models.TableInfo, error) {
	if d.baseURL == "" {
		return nil, fmt.Errorf("подключение не установлено")
	}

	if d.version == "1" {
		queryURL := fmt.Sprintf("%s/query", d.baseURL)
		params := url.Values{}
		params.Set("db", d.conn.Database)
		params.Set("q", "SHOW MEASUREMENTS")
		if d.conn.Username != "" {
			params.Set("u", d.conn.Username)
			params.Set("p", d.conn.Password)
		}

		req, err := http.NewRequestWithContext(ctx, "GET", queryURL+"?"+params.Encode(), nil)
		if err != nil {
			return nil, err
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
		if results, ok := result["results"].([]interface{}); ok && len(results) > 0 {
			if firstResult, ok := results[0].(map[string]interface{}); ok {
				if series, ok := firstResult["series"].([]interface{}); ok && len(series) > 0 {
					if ser, ok := series[0].(map[string]interface{}); ok {
						if values, ok := ser["values"].([]interface{}); ok {
							for _, valRow := range values {
								if valArray, ok := valRow.([]interface{}); ok && len(valArray) > 0 {
									if measName, ok := valArray[0].(string); ok {
										tables = append(tables, models.TableInfo{
											Name: measName,
											Database: d.conn.Database,
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

	return nil, fmt.Errorf("InfluxDB v2 не поддерживает список измерений через этот интерфейс")
}

func (d *InfluxDBDriver) DeleteTable(ctx context.Context, name string) error {
	return fmt.Errorf("InfluxDB не поддерживает удаление измерений напрямую")
}

func (d *InfluxDBDriver) UpdateTable(ctx context.Context, oldName, newName string, columns []models.TableColumn) error {
	return fmt.Errorf("InfluxDB не поддерживает переименование измерений")
}

func (d *InfluxDBDriver) CreateUser(ctx context.Context, username, password, database string, permissions []string) error {
	return fmt.Errorf("InfluxDB не поддерживает управление пользователями через этот интерфейс")
}

func (d *InfluxDBDriver) ListUsers(ctx context.Context) ([]models.UserInfo, error) {
	return nil, fmt.Errorf("InfluxDB не поддерживает управление пользователями через этот интерфейс")
}

func (d *InfluxDBDriver) UpdateUser(ctx context.Context, username, password string, permissions []string) error {
	return fmt.Errorf("InfluxDB не поддерживает управление пользователями через этот интерфейс")
}

func (d *InfluxDBDriver) DeleteUser(ctx context.Context, username string) error {
	return fmt.Errorf("InfluxDB не поддерживает управление пользователями через этот интерфейс")
}

