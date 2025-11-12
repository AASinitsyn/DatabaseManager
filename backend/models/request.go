package models

type LoginRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
}

type RegisterRequest struct {
	Username string `json:"username"`
	Password string `json:"password"`
	Email    string `json:"email,omitempty"`
}

type LoginResponse struct {
	Token string `json:"token"`
	User  User   `json:"user"`
}

type QueryRequest struct {
	ConnectionID string `json:"connectionId"`
	Query        string `json:"query"`
}

type QueryResponse struct {
	Columns      []string                 `json:"columns"`
	Rows         []map[string]interface{} `json:"rows"`
	RowCount     int                      `json:"rowCount"`
	ExecutionTime int64                   `json:"executionTime"`
	Error        string                   `json:"error,omitempty"`
}

type CreateDatabaseRequest struct {
	ConnectionID string                 `json:"connectionId"`
	Name         string                 `json:"name"`
	Options      map[string]interface{} `json:"options,omitempty"`
}

type UpdateDatabaseRequest struct {
	ConnectionID string                 `json:"connectionId"`
	OldName      string                 `json:"oldName"`
	NewName      string                 `json:"newName"`
	Options      map[string]interface{} `json:"options,omitempty"`
}

type DeleteDatabaseRequest struct {
	ConnectionID string `json:"connectionId"`
	Name         string `json:"name"`
}

type CreateTableRequest struct {
	ConnectionID string                 `json:"connectionId"`
	Name         string                 `json:"name"`
	Columns      []TableColumn          `json:"columns"`
}

type UpdateTableRequest struct {
	ConnectionID string        `json:"connectionId"`
	OldName      string        `json:"oldName"`
	NewName      string        `json:"newName"`
	Columns      []TableColumn `json:"columns"`
}

type TableColumn struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Nullable   bool   `json:"nullable"`
	PrimaryKey bool   `json:"primaryKey"`
	Unique     bool   `json:"unique"`
}

type TableInfo struct {
	Name     string        `json:"name"`
	Database string        `json:"database,omitempty"`
	Columns  []TableColumn `json:"columns,omitempty"`
	Size     string        `json:"size,omitempty"`
	Rows     int64         `json:"rows,omitempty"`
}

type CreateUserRequest struct {
	ConnectionID string   `json:"connectionId"`
	Username     string   `json:"username"`
	Password     string   `json:"password"`
	Database     string   `json:"database,omitempty"`
	Permissions  []string `json:"permissions"`
}

type UpdateUserRequest struct {
	ConnectionID string   `json:"connectionId"`
	Username     string   `json:"username"`
	Password     string   `json:"password,omitempty"`
	Permissions  []string `json:"permissions,omitempty"`
}

type ListUsersRequest struct {
	ConnectionID string `json:"connectionId"`
}

type DeleteUserRequest struct {
	ConnectionID string `json:"connectionId"`
	Username     string `json:"username"`
}

type UserInfo struct {
	Username    string   `json:"username"`
	Permissions []string `json:"permissions,omitempty"`
	IsSuperuser bool    `json:"isSuperuser,omitempty"`
}

type DatabaseInfo struct {
	Name      string `json:"name"`
	Owner     string `json:"owner,omitempty"`
	Size      string `json:"size,omitempty"`
	Encoding  string `json:"encoding,omitempty"`
	Collation string `json:"collation,omitempty"`
}

type ErrorResponse struct {
	Error string `json:"error"`
}

