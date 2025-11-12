package database

import (
	"context"
	"database-manager/models"
	"fmt"
	"sync"
	"time"
)

type ConnectionManager struct {
	drivers map[string]DatabaseDriver
	factory *DriverFactory
	mu      sync.RWMutex
}

func NewConnectionManager() *ConnectionManager {
	return &ConnectionManager{
		drivers: make(map[string]DatabaseDriver),
		factory: NewDriverFactory(),
	}
}

func (m *ConnectionManager) Connect(ctx context.Context, conn models.Connection) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	driver := m.factory.CreateDriver(conn.Type)
	if driver == nil {
		return fmt.Errorf("неподдерживаемый тип БД: %s", conn.Type)
	}

	if err := driver.Connect(ctx, conn); err != nil {
		return fmt.Errorf("ошибка подключения: %w", err)
	}

	m.drivers[conn.ID] = driver
	return nil
}

func (m *ConnectionManager) Disconnect(connectionID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	driver, exists := m.drivers[connectionID]
	if !exists {
		return fmt.Errorf("подключение с ID %s не найдено", connectionID)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := driver.Disconnect(ctx); err != nil {
		return fmt.Errorf("ошибка отключения: %w", err)
	}

	delete(m.drivers, connectionID)
	return nil
}

func (m *ConnectionManager) GetDriver(connectionID string) (DatabaseDriver, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	driver, exists := m.drivers[connectionID]
	if !exists {
		return nil, fmt.Errorf("подключение с ID %s не найдено", connectionID)
	}

	return driver, nil
}

func (m *ConnectionManager) IsConnected(connectionID string) bool {
	m.mu.RLock()
	defer m.mu.RUnlock()

	driver, exists := m.drivers[connectionID]
	if !exists {
		return false
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	return driver.IsConnected(ctx)
}

func (m *ConnectionManager) RestoreConnections(ctx context.Context, connections []models.Connection) error {
	for _, conn := range connections {
		if conn.Connected {
			if err := m.Connect(ctx, conn); err != nil {
				fmt.Printf("Не удалось восстановить подключение %s: %v\n", conn.ID, err)
				continue
			}
		}
	}
	return nil
}

func (m *ConnectionManager) CloseAll() {
	m.mu.Lock()
	defer m.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for id, driver := range m.drivers {
		driver.Disconnect(ctx)
		delete(m.drivers, id)
	}
}

