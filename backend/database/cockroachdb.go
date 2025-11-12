package database

import (
	"context"
	"database-manager/models"
)

type CockroachDBDriver struct {
	*PostgreSQLDriver
}

func NewCockroachDBDriver() *CockroachDBDriver {
	return &CockroachDBDriver{
		PostgreSQLDriver: NewPostgreSQLDriver(),
	}
}

func (d *CockroachDBDriver) Connect(ctx context.Context, conn models.Connection) error {
	return d.PostgreSQLDriver.Connect(ctx, conn)
}

