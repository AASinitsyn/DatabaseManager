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

type RabbitMQDriver struct {
	client  *http.Client
	baseURL string
	conn    models.Connection
}

func NewRabbitMQDriver() *RabbitMQDriver {
	return &RabbitMQDriver{
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

func (d *RabbitMQDriver) Connect(ctx context.Context, conn models.Connection) error {
	scheme := "http"
	if conn.SSL {
		scheme = "https"
	}
	d.baseURL = fmt.Sprintf("%s://%s:%s", scheme, conn.Host, conn.Port)
	d.conn = conn

	if err := d.Ping(ctx); err != nil {
		return fmt.Errorf("ошибка подключения к RabbitMQ: %w", err)
	}

	return nil
}

func (d *RabbitMQDriver) Disconnect(ctx context.Context) error {
	d.client = nil
	d.baseURL = ""
	return nil
}

func (d *RabbitMQDriver) IsConnected(ctx context.Context) bool {
	return d.baseURL != "" && d.Ping(ctx) == nil
}

func (d *RabbitMQDriver) Ping(ctx context.Context) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	pingURL := fmt.Sprintf("%s/api/overview", d.baseURL)
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

func (d *RabbitMQDriver) ExecuteQuery(ctx context.Context, query string) (*models.QueryResponse, error) {
	return &models.QueryResponse{
		Error: "RabbitMQ не поддерживает SQL запросы. Используйте RabbitMQ Management API",
	}, nil
}

func (d *RabbitMQDriver) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	vhost := name
	if vhost == "" {
		vhost = "/"
	}

	vhostURL := fmt.Sprintf("%s/api/vhosts/%s", d.baseURL, vhost)
	req, err := http.NewRequestWithContext(ctx, "PUT", vhostURL, nil)
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

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusCreated && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ошибка создания vhost: %s", string(body))
	}

	return nil
}

func (d *RabbitMQDriver) ListDatabases(ctx context.Context) ([]models.DatabaseInfo, error) {
	if d.baseURL == "" {
		return nil, fmt.Errorf("подключение не установлено")
	}

	vhostsURL := fmt.Sprintf("%s/api/vhosts", d.baseURL)
	req, err := http.NewRequestWithContext(ctx, "GET", vhostsURL, nil)
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
	var vhosts []map[string]interface{}
	if err := json.Unmarshal(respBody, &vhosts); err != nil {
		return nil, err
	}

	databases := make([]models.DatabaseInfo, 0)
	for _, vhost := range vhosts {
		if name, ok := vhost["name"].(string); ok {
			databases = append(databases, models.DatabaseInfo{
				Name: name,
			})
		}
	}

	return databases, nil
}

func (d *RabbitMQDriver) UpdateDatabase(ctx context.Context, oldName, newName string, options map[string]interface{}) error {
	return fmt.Errorf("RabbitMQ не поддерживает переименование vhosts")
}

func (d *RabbitMQDriver) DeleteDatabase(ctx context.Context, name string) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	vhostURL := fmt.Sprintf("%s/api/vhosts/%s", d.baseURL, name)
	req, err := http.NewRequestWithContext(ctx, "DELETE", vhostURL, nil)
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

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ошибка удаления vhost: %s", string(body))
	}

	return nil
}

func (d *RabbitMQDriver) CreateTable(ctx context.Context, name string, columns []models.TableColumn) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	vhost := d.conn.Database
	if vhost == "" {
		vhost = "/"
	}

	queueURL := fmt.Sprintf("%s/api/queues/%s/%s", d.baseURL, vhost, name)
	
	body := map[string]interface{}{
		"auto_delete": false,
		"durable":     true,
	}

	jsonBody, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, "PUT", queueURL, bytes.NewBuffer(jsonBody))
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
		return fmt.Errorf("ошибка создания очереди: %s", string(body))
	}

	return nil
}

func (d *RabbitMQDriver) ListTables(ctx context.Context) ([]models.TableInfo, error) {
	if d.baseURL == "" {
		return nil, fmt.Errorf("подключение не установлено")
	}

	vhost := d.conn.Database
	if vhost == "" {
		vhost = "/"
	}

	queuesURL := fmt.Sprintf("%s/api/queues/%s", d.baseURL, vhost)
	req, err := http.NewRequestWithContext(ctx, "GET", queuesURL, nil)
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
	var queues []map[string]interface{}
	if err := json.Unmarshal(respBody, &queues); err != nil {
		return nil, err
	}

	tables := make([]models.TableInfo, 0)
	for _, queue := range queues {
		if name, ok := queue["name"].(string); ok {
			messages := int64(0)
			if msg, ok := queue["messages"].(float64); ok {
				messages = int64(msg)
			}
			tables = append(tables, models.TableInfo{
				Name:     name,
				Database: vhost,
				Rows:     messages,
			})
		}
	}

	return tables, nil
}

func (d *RabbitMQDriver) DeleteTable(ctx context.Context, name string) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	vhost := d.conn.Database
	if vhost == "" {
		vhost = "/"
	}

	queueURL := fmt.Sprintf("%s/api/queues/%s/%s", d.baseURL, vhost, name)
	req, err := http.NewRequestWithContext(ctx, "DELETE", queueURL, nil)
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

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("ошибка удаления очереди: %s", string(body))
	}

	return nil
}

func (d *RabbitMQDriver) UpdateTable(ctx context.Context, oldName, newName string, columns []models.TableColumn) error {
	return fmt.Errorf("RabbitMQ не поддерживает переименование очередей")
}

func (d *RabbitMQDriver) CreateUser(ctx context.Context, username, password, database string, permissions []string) error {
	return fmt.Errorf("RabbitMQ не поддерживает управление пользователями через этот интерфейс")
}

func (d *RabbitMQDriver) ListUsers(ctx context.Context) ([]models.UserInfo, error) {
	return nil, fmt.Errorf("RabbitMQ не поддерживает управление пользователями через этот интерфейс")
}

func (d *RabbitMQDriver) UpdateUser(ctx context.Context, username, password string, permissions []string) error {
	return fmt.Errorf("RabbitMQ не поддерживает управление пользователями через этот интерфейс")
}

func (d *RabbitMQDriver) DeleteUser(ctx context.Context, username string) error {
	return fmt.Errorf("RabbitMQ не поддерживает управление пользователями через этот интерфейс")
}

