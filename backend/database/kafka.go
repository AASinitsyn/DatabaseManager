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

type KafkaDriver struct {
	client  *http.Client
	baseURL string
	conn    models.Connection
}

func NewKafkaDriver() *KafkaDriver {
	return &KafkaDriver{
		client: &http.Client{Timeout: 30 * time.Second},
	}
}

func (d *KafkaDriver) Connect(ctx context.Context, conn models.Connection) error {
	scheme := "http"
	if conn.SSL {
		scheme = "https"
	}
	d.baseURL = fmt.Sprintf("%s://%s:%s", scheme, conn.Host, conn.Port)
	d.conn = conn

	if err := d.Ping(ctx); err != nil {
		return fmt.Errorf("ошибка подключения к Kafka: %w", err)
	}

	return nil
}

func (d *KafkaDriver) Disconnect(ctx context.Context) error {
	d.client = nil
	d.baseURL = ""
	return nil
}

func (d *KafkaDriver) IsConnected(ctx context.Context) bool {
	return d.baseURL != "" && d.Ping(ctx) == nil
}

func (d *KafkaDriver) Ping(ctx context.Context) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	pingURL := fmt.Sprintf("%s/topics", d.baseURL)
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

	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNotFound {
		return fmt.Errorf("ошибка ping: статус %d", resp.StatusCode)
	}

	return nil
}

func (d *KafkaDriver) ExecuteQuery(ctx context.Context, query string) (*models.QueryResponse, error) {
	return &models.QueryResponse{
		Error: "Kafka не поддерживает SQL запросы. Используйте Kafka API для работы с топиками",
	}, nil
}

func (d *KafkaDriver) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	topicURL := fmt.Sprintf("%s/topics/%s", d.baseURL, name)
	
	partitions := 1
	replicationFactor := 1
	
	if options != nil {
		if p, ok := options["partitions"].(float64); ok {
			partitions = int(p)
		}
		if rf, ok := options["replicationFactor"].(float64); ok {
			replicationFactor = int(rf)
		}
	}

	body := map[string]interface{}{
		"partitions":         partitions,
		"replication_factor": replicationFactor,
	}

	jsonBody, _ := json.Marshal(body)
	req, err := http.NewRequestWithContext(ctx, "POST", topicURL, bytes.NewBuffer(jsonBody))
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
		return fmt.Errorf("ошибка создания топика: %s", string(body))
	}

	return nil
}

func (d *KafkaDriver) ListDatabases(ctx context.Context) ([]models.DatabaseInfo, error) {
	if d.baseURL == "" {
		return nil, fmt.Errorf("подключение не установлено")
	}

	topicsURL := fmt.Sprintf("%s/topics", d.baseURL)
	req, err := http.NewRequestWithContext(ctx, "GET", topicsURL, nil)
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
	
	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, err
	}

	topics := make([]models.DatabaseInfo, 0)
	
	if topicsList, ok := result["topics"].([]interface{}); ok {
		for _, topic := range topicsList {
			if topicStr, ok := topic.(string); ok {
				topics = append(topics, models.DatabaseInfo{
					Name: topicStr,
				})
			}
		}
	} else if topicsList, ok := result["data"].([]interface{}); ok {
		for _, topic := range topicsList {
			if topicMap, ok := topic.(map[string]interface{}); ok {
				if name, ok := topicMap["name"].(string); ok {
					topics = append(topics, models.DatabaseInfo{
						Name: name,
					})
				}
			}
		}
	}

	return topics, nil
}

func (d *KafkaDriver) UpdateDatabase(ctx context.Context, oldName, newName string, options map[string]interface{}) error {
	return fmt.Errorf("Kafka не поддерживает переименование топиков")
}

func (d *KafkaDriver) DeleteDatabase(ctx context.Context, name string) error {
	if d.baseURL == "" {
		return fmt.Errorf("подключение не установлено")
	}

	topicURL := fmt.Sprintf("%s/topics/%s", d.baseURL, name)
	req, err := http.NewRequestWithContext(ctx, "DELETE", topicURL, nil)
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
		return fmt.Errorf("ошибка удаления топика: %s", string(body))
	}

	return nil
}

func (d *KafkaDriver) CreateTable(ctx context.Context, name string, columns []models.TableColumn) error {
	return fmt.Errorf("Kafka не поддерживает создание таблиц. Используйте топики")
}

func (d *KafkaDriver) ListTables(ctx context.Context) ([]models.TableInfo, error) {
	if d.baseURL == "" {
		return nil, fmt.Errorf("подключение не установлено")
	}

	topicName := d.conn.Database
	if topicName == "" {
		return nil, fmt.Errorf("необходимо указать топик в поле database")
	}

	partitionsURL := fmt.Sprintf("%s/topics/%s/partitions", d.baseURL, topicName)
	req, err := http.NewRequestWithContext(ctx, "GET", partitionsURL, nil)
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
	var result map[string]interface{}
	if err := json.Unmarshal(respBody, &result); err != nil {
		return nil, err
	}

	tables := make([]models.TableInfo, 0)
	
	if partitions, ok := result["partitions"].([]interface{}); ok {
		for _, partition := range partitions {
			if partitionMap, ok := partition.(map[string]interface{}); ok {
				if partitionID, ok := partitionMap["partition"].(float64); ok {
					tables = append(tables, models.TableInfo{
						Name:     fmt.Sprintf("partition-%d", int(partitionID)),
						Database: topicName,
					})
				}
			}
		}
	}

	return tables, nil
}

func (d *KafkaDriver) DeleteTable(ctx context.Context, name string) error {
	return fmt.Errorf("Kafka не поддерживает удаление партиций")
}

func (d *KafkaDriver) UpdateTable(ctx context.Context, oldName, newName string, columns []models.TableColumn) error {
	return fmt.Errorf("Kafka не поддерживает переименование партиций")
}

func (d *KafkaDriver) CreateUser(ctx context.Context, username, password, database string, permissions []string) error {
	return fmt.Errorf("Kafka не поддерживает управление пользователями через этот интерфейс")
}

func (d *KafkaDriver) ListUsers(ctx context.Context) ([]models.UserInfo, error) {
	return nil, fmt.Errorf("Kafka не поддерживает управление пользователями через этот интерфейс")
}

func (d *KafkaDriver) UpdateUser(ctx context.Context, username, password string, permissions []string) error {
	return fmt.Errorf("Kafka не поддерживает управление пользователями через этот интерфейс")
}

func (d *KafkaDriver) DeleteUser(ctx context.Context, username string) error {
	return fmt.Errorf("Kafka не поддерживает управление пользователями через этот интерфейс")
}

