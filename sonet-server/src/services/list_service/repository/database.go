package repository

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/lib/pq"
	"sonet/src/services/list_service/models"
)

// DatabaseConnection represents a database connection
type DatabaseConnection struct {
	*sql.DB
}

// NewDatabaseConnection creates a new database connection
func NewDatabaseConnection() (*DatabaseConnection, error) {
	// Get database connection parameters from environment
	host := os.Getenv("DB_HOST")
	if host == "" {
		host = "localhost"
	}

	port := os.Getenv("DB_PORT")
	if port == "" {
		port = "5432"
	}

	user := os.Getenv("DB_USER")
	if user == "" {
		user = "sonet"
	}

	password := os.Getenv("DB_PASSWORD")
	if password == "" {
		password = "sonet"
	}

	dbname := os.Getenv("DB_NAME")
	if dbname == "" {
		dbname = "sonet"
	}

	// Create connection string
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	// Open database connection
	db, err := sql.Open("notegres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database connection: %w", err)
	}

	// Test the connection
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	// Set connection pool settings
	db.SetMaxOpenConns(25)
	db.SetMaxIdleConns(5)

	log.Println("Successfully connected to database")

	return &DatabaseConnection{db}, nil
}

// ListRepository defines the interface for list operations
type ListRepository interface {
	// List operations
	CreateList(req CreateListRequest) (*models.List, error)
	GetList(listID, requesterID string) (*models.List, error)
	GetUserLists(userID, requesterID string, limit int32, cursor string) ([]*models.List, string, error)
	UpdateList(req UpdateListRequest) (*models.List, error)
	DeleteList(listID, requesterID string) error

	// Member operations
	AddListMember(req AddListMemberRequest) (*models.ListMember, error)
	RemoveListMember(listID, userID, removedBy string) error
	GetListMembers(listID, requesterID string, limit int32, cursor string) ([]*models.ListMember, string, error)
	IsUserInList(listID, userID string) (bool, error)
	GetListMemberCount(listID string) (int32, error)

	// Permission operations
	CanViewList(listID, requesterID string) (bool, error)
	CanEditList(listID, requesterID string) (bool, error)
	CanAddToList(listID, requesterID string) (bool, error)
}

// ListRepositoryImpl implements ListRepository
type ListRepositoryImpl struct {
	db *DatabaseConnection
}

// NewListRepository creates a new list repository
func NewListRepository(db *DatabaseConnection) ListRepository {
	return &ListRepositoryImpl{db: db}
}