package database

import (
	"context"
	"database-manager/models"
	"fmt"
	"strings"
	"time"

	"github.com/aerospike/aerospike-client-go/v6"
)

type AerospikeDriver struct {
	client *aerospike.Client
	conn   models.Connection
}

func NewAerospikeDriver() *AerospikeDriver {
	return &AerospikeDriver{}
}

func (d *AerospikeDriver) Connect(ctx context.Context, conn models.Connection) error {
	host := aerospike.NewHost(conn.Host, 3000)
	if conn.Port != "" {
		port := 3000
		fmt.Sscanf(conn.Port, "%d", &port)
		host = aerospike.NewHost(conn.Host, port)
	}

	policy := aerospike.NewClientPolicy()
	policy.User = conn.Username
	policy.Password = conn.Password
	policy.Timeout = 10 * time.Second

	client, err := aerospike.NewClientWithPolicyAndHost(policy, host)
	if err != nil {
		return fmt.Errorf("ошибка подключения к Aerospike: %w", err)
	}

	if !client.IsConnected() {
		return fmt.Errorf("не удалось установить подключение к Aerospike")
	}

	d.client = client
	d.conn = conn
	return nil
}

func (d *AerospikeDriver) Disconnect(ctx context.Context) error {
	if d.client != nil {
		d.client.Close()
		d.client = nil
	}
	return nil
}

func (d *AerospikeDriver) IsConnected(ctx context.Context) bool {
	return d.client != nil && d.client.IsConnected()
}

func (d *AerospikeDriver) Ping(ctx context.Context) error {
	if d.client == nil {
		return fmt.Errorf("подключение не установлено")
	}
	if !d.client.IsConnected() {
		return fmt.Errorf("подключение разорвано")
	}
	return nil
}

func (d *AerospikeDriver) ExecuteQuery(ctx context.Context, query string) (*models.QueryResponse, error) {
	if d.client == nil {
		return nil, fmt.Errorf("подключение не установлено")
	}

	return &models.QueryResponse{
		Error: "Aerospike не поддерживает SQL-подобные запросы напрямую. Используйте AQL или клиентские операции",
	}, nil
}

func (d *AerospikeDriver) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	return fmt.Errorf("namespace в Aerospike создается через конфигурационный файл aerospike.conf")
}

func (d *AerospikeDriver) ListDatabases(ctx context.Context) ([]models.DatabaseInfo, error) {
	if d.client == nil {
		return nil, fmt.Errorf("подключение не установлено")
	}

	nodes := d.client.GetNodes()
	if len(nodes) == 0 {
		return []models.DatabaseInfo{}, nil
	}

	node := nodes[0]
	infoPolicy := aerospike.NewInfoPolicy()
	namespaces, err := node.RequestInfo(infoPolicy, "namespaces")
	if err != nil {
		return nil, fmt.Errorf("ошибка получения списка namespace: %w", err)
	}

	nsList := namespaces["namespaces"]
	if nsList == "" {
		return []models.DatabaseInfo{}, nil
	}

	nsArray := strings.Split(nsList, ";")
	databases := make([]models.DatabaseInfo, 0, len(nsArray))
	for _, ns := range nsArray {
		ns = strings.TrimSpace(ns)
		if ns != "" {
			databases = append(databases, models.DatabaseInfo{
				Name: ns,
			})
		}
	}

	return databases, nil
}

func (d *AerospikeDriver) UpdateDatabase(ctx context.Context, oldName, newName string, options map[string]interface{}) error {
	return fmt.Errorf("переименование namespace в Aerospike требует изменения конфигурационного файла aerospike.conf и перезапуска кластера")
}

func (d *AerospikeDriver) DeleteDatabase(ctx context.Context, name string) error {
	return fmt.Errorf("удаление namespace в Aerospike требует изменения конфигурационного файла aerospike.conf и перезапуска кластера")
}

func (d *AerospikeDriver) CreateTable(ctx context.Context, name string, columns []models.TableColumn) error {
	return fmt.Errorf("Aerospike не использует таблицы в традиционном смысле. Используйте sets внутри namespace")
}

func (d *AerospikeDriver) ListTables(ctx context.Context) ([]models.TableInfo, error) {
	return []models.TableInfo{}, fmt.Errorf("Aerospike не использует таблицы в традиционном смысле. Используйте sets внутри namespace")
}

func (d *AerospikeDriver) DeleteTable(ctx context.Context, name string) error {
	return fmt.Errorf("Aerospike не использует таблицы в традиционном смысле. Используйте sets внутри namespace")
}

func (d *AerospikeDriver) UpdateTable(ctx context.Context, oldName, newName string, columns []models.TableColumn) error {
	return fmt.Errorf("Aerospike не использует таблицы в традиционном смысле. Используйте sets внутри namespace")
}

func (d *AerospikeDriver) CreateUser(ctx context.Context, username, password, database string, permissions []string) error {
	return fmt.Errorf("создание пользователей в Aerospike требует Enterprise Edition и настройки через aerospike.conf")
}

func (d *AerospikeDriver) ListUsers(ctx context.Context) ([]models.UserInfo, error) {
	if d.client == nil {
		return nil, fmt.Errorf("подключение не установлено")
	}

	if d.conn.Username == "" || d.conn.Password == "" {
		return nil, fmt.Errorf("управление пользователями в Aerospike требует Enterprise Edition и аутентификации администратора. Пожалуйста, используйте Enterprise Edition с настроенным XDR и Security")
	}

	return nil, fmt.Errorf("управление пользователями в Aerospike доступно только в Enterprise Edition через административные команды. Используйте asadm или настройте через aerospike.conf")
}

func (d *AerospikeDriver) UpdateUser(ctx context.Context, username, password string, permissions []string) error {
	if d.client == nil {
		return fmt.Errorf("подключение не установлено")
	}

	return fmt.Errorf("обновление пользователей в Aerospike доступно только в Enterprise Edition. Используйте команду 'asadm -e \"admin update user %s\"' или настройте через aerospike.conf", username)
}

func (d *AerospikeDriver) DeleteUser(ctx context.Context, username string) error {
	if d.client == nil {
		return fmt.Errorf("подключение не установлено")
	}

	return fmt.Errorf("удаление пользователей в Aerospike доступно только в Enterprise Edition. Используйте команду 'asadm -e \"admin drop user %s\"' или настройте через aerospike.conf", username)
}

