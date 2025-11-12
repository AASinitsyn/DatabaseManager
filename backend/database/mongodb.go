package database

import (
	"context"
	"database-manager/models"
	"fmt"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

type MongoDBDriver struct {
	client *mongo.Client
	conn   models.Connection
}

func NewMongoDBDriver() *MongoDBDriver {
	return &MongoDBDriver{}
}

func (d *MongoDBDriver) Connect(ctx context.Context, conn models.Connection) error {
	dsn := fmt.Sprintf("mongodb://%s:%s@%s:%s/%s",
		conn.Username, conn.Password, conn.Host, conn.Port, conn.Database)
	
	if conn.SSL {
		dsn += "?ssl=true"
	}

	clientOptions := options.Client().ApplyURI(dsn)
	client, err := mongo.Connect(ctx, clientOptions)
	if err != nil {
		return fmt.Errorf("ошибка подключения к MongoDB: %w", err)
	}

	if err := client.Ping(ctx, nil); err != nil {
		return fmt.Errorf("ошибка ping MongoDB: %w", err)
	}

	d.client = client
	d.conn = conn
	return nil
}

func (d *MongoDBDriver) Disconnect(ctx context.Context) error {
	if d.client != nil {
		return d.client.Disconnect(ctx)
	}
	return nil
}

func (d *MongoDBDriver) IsConnected(ctx context.Context) bool {
	if d.client == nil {
		return false
	}
	return d.client.Ping(ctx, nil) == nil
}

func (d *MongoDBDriver) Ping(ctx context.Context) error {
	if d.client == nil {
		return fmt.Errorf("подключение не установлено")
	}
	return d.client.Ping(ctx, nil)
}

func (d *MongoDBDriver) ExecuteQuery(ctx context.Context, query string) (*models.QueryResponse, error) {
	if d.client == nil {
		return nil, fmt.Errorf("подключение не установлено")
	}

	startTime := time.Now()
	
	var filter bson.M
	if err := bson.UnmarshalExtJSON([]byte(query), true, &filter); err != nil {
		return &models.QueryResponse{
			Error: fmt.Sprintf("ошибка парсинга запроса: %v", err),
		}, nil
	}

	db := d.client.Database(d.conn.Database)
	collection := db.Collection("collection_name")
	
	cursor, err := collection.Find(ctx, filter)
	if err != nil {
		return &models.QueryResponse{
			Error: err.Error(),
		}, nil
	}
	defer cursor.Close(ctx)

	var results []bson.M
	if err := cursor.All(ctx, &results); err != nil {
		return &models.QueryResponse{
			Error: err.Error(),
		}, nil
	}

	columns := []string{"_id"}
	rowsData := make([]map[string]interface{}, 0)
	
	for _, result := range results {
		row := make(map[string]interface{})
		for key, value := range result {
			if key == "_id" {
				row["_id"] = value
			} else {
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

func (d *MongoDBDriver) CreateDatabase(ctx context.Context, name string, options map[string]interface{}) error {
	if d.client == nil {
		return fmt.Errorf("подключение не установлено")
	}

	db := d.client.Database(name)
	collection := db.Collection("init")
	_, err := collection.InsertOne(ctx, bson.M{"init": true})
	return err
}

func (d *MongoDBDriver) ListDatabases(ctx context.Context) ([]models.DatabaseInfo, error) {
	if d.client == nil {
		return nil, fmt.Errorf("подключение не установлено")
	}

	databases, err := d.client.ListDatabaseNames(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("ошибка получения списка баз данных: %w", err)
	}

	result := make([]models.DatabaseInfo, 0, len(databases))
	for _, dbName := range databases {
		if dbName == "admin" || dbName == "local" || dbName == "config" {
			continue
		}

		db := d.client.Database(dbName)
		var stats bson.M
		err := db.RunCommand(ctx, bson.D{{Key: "dbStats", Value: 1}}).Decode(&stats)
		size := "N/A"
		if err == nil {
			if dataSize, ok := stats["dataSize"].(int64); ok {
				size = fmt.Sprintf("%.2f MB", float64(dataSize)/(1024*1024))
			}
		}

		result = append(result, models.DatabaseInfo{
			Name: dbName,
			Size: size,
		})
	}

	return result, nil
}

func (d *MongoDBDriver) UpdateDatabase(ctx context.Context, oldName, newName string, options map[string]interface{}) error {
	if d.client == nil {
		return fmt.Errorf("подключение не установлено")
	}

	if newName != "" && newName != oldName {
		oldDb := d.client.Database(oldName)
		newDb := d.client.Database(newName)

		collections, err := oldDb.ListCollectionNames(ctx, bson.M{})
		if err != nil {
			return fmt.Errorf("ошибка получения списка коллекций: %w", err)
		}

		for _, collName := range collections {
			oldColl := oldDb.Collection(collName)
			newColl := newDb.Collection(collName)

			cursor, err := oldColl.Find(ctx, bson.M{})
			if err != nil {
				continue
			}

			var docs []interface{}
			if err := cursor.All(ctx, &docs); err != nil {
				cursor.Close(ctx)
				continue
			}
			cursor.Close(ctx)

			if len(docs) > 0 {
				_, err = newColl.InsertMany(ctx, docs)
				if err != nil {
					return fmt.Errorf("ошибка копирования коллекции %s: %w", collName, err)
				}
			}
		}

		if err := oldDb.Drop(ctx); err != nil {
			return fmt.Errorf("ошибка удаления старой базы данных: %w", err)
		}
	}

	return nil
}

func (d *MongoDBDriver) DeleteDatabase(ctx context.Context, name string) error {
	if d.client == nil {
		return fmt.Errorf("подключение не установлено")
	}

	db := d.client.Database(name)
	if err := db.Drop(ctx); err != nil {
		return fmt.Errorf("ошибка удаления базы данных: %w", err)
	}

	return nil
}

func (d *MongoDBDriver) CreateTable(ctx context.Context, name string, columns []models.TableColumn) error {
	if d.client == nil {
		return fmt.Errorf("подключение не установлено")
	}

	db := d.client.Database(d.conn.Database)
	return db.CreateCollection(ctx, name)
}

func (d *MongoDBDriver) ListTables(ctx context.Context) ([]models.TableInfo, error) {
	if d.client == nil {
		return nil, fmt.Errorf("подключение не установлено")
	}

	db := d.client.Database(d.conn.Database)
	collections, err := db.ListCollectionNames(ctx, bson.M{})
	if err != nil {
		return nil, fmt.Errorf("ошибка получения списка коллекций: %w", err)
	}

	tables := make([]models.TableInfo, 0, len(collections))
	for _, collName := range collections {
		coll := db.Collection(collName)
		count, _ := coll.CountDocuments(ctx, bson.M{})
		
		stats := db.RunCommand(ctx, bson.D{{Key: "collStats", Value: collName}})
		var statsResult bson.M
		size := "N/A"
		if stats.Decode(&statsResult) == nil {
			if sizeVal, ok := statsResult["size"].(int64); ok {
				size = fmt.Sprintf("%.2f MB", float64(sizeVal)/(1024*1024))
			}
		}

		tables = append(tables, models.TableInfo{
			Name:     collName,
			Database: d.conn.Database,
			Size:     size,
			Rows:     count,
		})
	}

	return tables, nil
}

func (d *MongoDBDriver) DeleteTable(ctx context.Context, name string) error {
	if d.client == nil {
		return fmt.Errorf("подключение не установлено")
	}

	db := d.client.Database(d.conn.Database)
	return db.Collection(name).Drop(ctx)
}

func (d *MongoDBDriver) UpdateTable(ctx context.Context, oldName, newName string, columns []models.TableColumn) error {
	if d.client == nil {
		return fmt.Errorf("подключение не установлено")
	}

	if newName != "" && newName != oldName {
		adminDb := d.client.Database("admin")
		command := bson.D{
			{Key: "renameCollection", Value: fmt.Sprintf("%s.%s", d.conn.Database, oldName)},
			{Key: "to", Value: fmt.Sprintf("%s.%s", d.conn.Database, newName)},
		}
		var result bson.M
		err := adminDb.RunCommand(ctx, command).Decode(&result)
		if err != nil {
			return fmt.Errorf("ошибка переименования коллекции: %w", err)
		}
	}

	if len(columns) > 0 {
		return fmt.Errorf("изменение структуры коллекций в MongoDB не поддерживается через ALTER. Используйте миграции данных")
	}

	return nil
}

func (d *MongoDBDriver) CreateUser(ctx context.Context, username, password, database string, permissions []string) error {
	if d.client == nil {
		return fmt.Errorf("подключение не установлено")
	}

	dbName := database
	if dbName == "" {
		dbName = d.conn.Database
	}

	db := d.client.Database(dbName)
	
	roles := make([]bson.M, 0)
	for _, perm := range permissions {
		roles = append(roles, bson.M{
			"role": perm,
			"db":   dbName,
		})
	}

	command := bson.D{
		{Key: "createUser", Value: username},
		{Key: "pwd", Value: password},
		{Key: "roles", Value: roles},
	}

	var result bson.M
	err := db.RunCommand(ctx, command).Decode(&result)
	return err
}

func (d *MongoDBDriver) ListUsers(ctx context.Context) ([]models.UserInfo, error) {
	if d.client == nil {
		return nil, fmt.Errorf("подключение не установлено")
	}

	db := d.client.Database(d.conn.Database)
	command := bson.D{{Key: "usersInfo", Value: 1}}

	var result bson.M
	err := db.RunCommand(ctx, command).Decode(&result)
	if err != nil {
		return nil, fmt.Errorf("ошибка получения списка пользователей: %w", err)
	}

	users := make([]models.UserInfo, 0)
	if usersData, ok := result["users"].(bson.A); ok {
		for _, userData := range usersData {
			if userMap, ok := userData.(bson.M); ok {
				username := ""
				if u, ok := userMap["user"].(string); ok {
					username = u
				}

				permissions := make([]string, 0)
				if roles, ok := userMap["roles"].(bson.A); ok {
					for _, roleData := range roles {
						if roleMap, ok := roleData.(bson.M); ok {
							if role, ok := roleMap["role"].(string); ok {
								permissions = append(permissions, role)
							}
						}
					}
				}

				isSuperuser := false
				for _, perm := range permissions {
					if perm == "root" || perm == "userAdminAnyDatabase" {
						isSuperuser = true
						break
					}
				}

				if username != "" {
					users = append(users, models.UserInfo{
						Username:    username,
						Permissions: permissions,
						IsSuperuser: isSuperuser,
					})
				}
			}
		}
	}

	return users, nil
}

func (d *MongoDBDriver) UpdateUser(ctx context.Context, username, password string, permissions []string) error {
	if d.client == nil {
		return fmt.Errorf("подключение не установлено")
	}

	db := d.client.Database(d.conn.Database)

	if password != "" {
		updateCommand := bson.D{
			{Key: "updateUser", Value: username},
			{Key: "pwd", Value: password},
		}

		var result bson.M
		err := db.RunCommand(ctx, updateCommand).Decode(&result)
		if err != nil {
			return fmt.Errorf("ошибка обновления пароля: %w", err)
		}
	}

	if permissions != nil {
		roles := make([]bson.M, 0)
		for _, perm := range permissions {
			roles = append(roles, bson.M{
				"role": perm,
				"db":   d.conn.Database,
			})
		}

		grantRolesCommand := bson.D{
			{Key: "grantRolesToUser", Value: username},
			{Key: "roles", Value: roles},
		}

		var result bson.M
		err := db.RunCommand(ctx, grantRolesCommand).Decode(&result)
		if err != nil {
			return fmt.Errorf("ошибка обновления прав: %w", err)
		}
	}

	return nil
}

func (d *MongoDBDriver) DeleteUser(ctx context.Context, username string) error {
	if d.client == nil {
		return fmt.Errorf("подключение не установлено")
	}

	db := d.client.Database(d.conn.Database)
	command := bson.D{
		{Key: "dropUser", Value: username},
	}

	var result bson.M
	err := db.RunCommand(ctx, command).Decode(&result)
	if err != nil {
		return fmt.Errorf("ошибка удаления пользователя: %w", err)
	}

	return nil
}

func contains(slice []string, item string) bool {
	for _, s := range slice {
		if s == item {
			return true
		}
	}
	return false
}

