package database

import (
	"context"
	"database-manager/models"
)

type SupabaseDriver struct {
	*PostgreSQLDriver
}

func NewSupabaseDriver() *SupabaseDriver {
	return &SupabaseDriver{
		PostgreSQLDriver: NewPostgreSQLDriver(),
	}
}

func (d *SupabaseDriver) Connect(ctx context.Context, conn models.Connection) error {
	return d.PostgreSQLDriver.Connect(ctx, conn)
}

