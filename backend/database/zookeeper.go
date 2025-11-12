package database

import (
	"context"
	"database-manager/models"
	"fmt"
	"time"

	"github.com/go-zookeeper/zk"
)

type ZookeeperDriver struct {
	conn *zk.Conn
	connInfo models.Connection
}

func NewZookeeperDriver() *ZookeeperDriver {
	return &ZookeeperDriver{}
}

func (d *ZookeeperDriver) Connect(ctx context.Context, conn models.Connection) error {
	servers := []string{fmt.Sprintf("%s:%s", conn.Host, conn.Port)}
	
	var err error
	d.conn, _, err = zk.Connect(servers, 10*time.Second)
	if err != nil {
		return fmt.Errorf("ошибка подключения к Zookeeper: %w", err)
	}

	if conn.Username != "" && conn.Password != "" {
		auth := fmt.Sprintf("%s:%s", conn.Username, conn.Password)
		if err := d.conn.AddAuth("digest", []byte(auth)); err != nil {
			return fmt.Errorf("ошибка аутентификации: %w", err)
		}
	}

	d.connInfo = conn
	return nil
}

func (d *ZookeeperDriver) Disconnect(ctx context.Context) error {
	if d.conn != nil {
		d.conn.Close()
		d.conn = nil
	}
	return nil
}

func (d *ZookeeperDriver) IsConnected(ctx context.Context) bool {
	if d.conn == nil {
		return false
	}
	return d.conn.State() == zk.StateConnected || d.conn.State() == zk.StateHasSession
}

func (d *ZookeeperDriver) Ping(ctx context.Context) error {
	if d.conn == nil {
		return fmt.Errorf("подключение не установлено")
	}
	
	_, _, err := d.conn.Get("/")
	return err
}

func (d *ZookeeperDriver) ExecuteQuery(ctx context.Context, query string) (*models.QueryResponse, error) {
	return &models.QueryResponse{
		Error: "Zookeeper не поддерживает SQL запросы. Используйте Zookeeper API для работы с узлами",
	}, nil
}

func (d *ZookeeperDriver) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	if d.conn == nil {
		return fmt.Errorf("подключение не установлено")
	}

	path := name
	if path[0] != '/' {
		path = "/" + path
	}

	flags := int32(0)
	if options != nil {
		if ephemeral, ok := options["ephemeral"].(bool); ok && ephemeral {
			flags |= zk.FlagEphemeral
		}
		if sequence, ok := options["sequence"].(bool); ok && sequence {
			flags |= zk.FlagSequence
		}
	}

	data := []byte("")
	if options != nil {
		if dataStr, ok := options["data"].(string); ok {
			data = []byte(dataStr)
		}
	}

	_, err := d.conn.Create(path, data, flags, zk.WorldACL(zk.PermAll))
	return err
}

func (d *ZookeeperDriver) ListDatabases(ctx context.Context) ([]models.DatabaseInfo, error) {
	if d.conn == nil {
		return nil, fmt.Errorf("подключение не установлено")
	}

	children, _, err := d.conn.Children("/")
	if err != nil {
		return nil, err
	}

	databases := make([]models.DatabaseInfo, 0)
	for _, child := range children {
		if child[0] != '.' {
			databases = append(databases, models.DatabaseInfo{
				Name: "/" + child,
			})
		}
	}

	return databases, nil
}

func (d *ZookeeperDriver) UpdateDatabase(ctx context.Context, oldName, newName string, options map[string]interface{}) error {
	return fmt.Errorf("Zookeeper не поддерживает переименование узлов напрямую")
}

func (d *ZookeeperDriver) DeleteDatabase(ctx context.Context, name string) error {
	if d.conn == nil {
		return fmt.Errorf("подключение не установлено")
	}

	path := name
	if path[0] != '/' {
		path = "/" + path
	}

	version := int32(-1)
	return d.conn.Delete(path, version)
}

func (d *ZookeeperDriver) CreateTable(ctx context.Context, name string, columns []models.TableColumn) error {
	return d.CreateDatabase(context.Background(), name, nil)
}

func (d *ZookeeperDriver) ListTables(ctx context.Context) ([]models.TableInfo, error) {
	if d.conn == nil {
		return nil, fmt.Errorf("подключение не установлено")
	}

	basePath := d.connInfo.Database
	if basePath == "" {
		basePath = "/"
	}

	children, stat, err := d.conn.Children(basePath)
	if err != nil {
		return nil, err
	}

	tables := make([]models.TableInfo, 0)
	for _, child := range children {
		if child[0] != '.' {
			childPath := basePath
			if basePath != "/" {
				childPath += "/" + child
			} else {
				childPath = "/" + child
			}
			
			size := "N/A"
			if stat != nil {
				size = fmt.Sprintf("%d bytes", stat.DataLength)
			}
			
			tables = append(tables, models.TableInfo{
				Name:     child,
				Database: basePath,
				Size:     size,
			})
		}
	}

	return tables, nil
}

func (d *ZookeeperDriver) DeleteTable(ctx context.Context, name string) error {
	return d.DeleteDatabase(context.Background(), name)
}

func (d *ZookeeperDriver) UpdateTable(ctx context.Context, oldName, newName string, columns []models.TableColumn) error {
	return fmt.Errorf("Zookeeper не поддерживает переименование узлов напрямую")
}

func (d *ZookeeperDriver) CreateUser(ctx context.Context, username, password, database string, permissions []string) error {
	return fmt.Errorf("Zookeeper не поддерживает управление пользователями через этот интерфейс")
}

func (d *ZookeeperDriver) ListUsers(ctx context.Context) ([]models.UserInfo, error) {
	return nil, fmt.Errorf("Zookeeper не поддерживает управление пользователями через этот интерфейс")
}

func (d *ZookeeperDriver) UpdateUser(ctx context.Context, username, password string, permissions []string) error {
	return fmt.Errorf("Zookeeper не поддерживает управление пользователями через этот интерфейс")
}

func (d *ZookeeperDriver) DeleteUser(ctx context.Context, username string) error {
	return fmt.Errorf("Zookeeper не поддерживает управление пользователями через этот интерфейс")
}

