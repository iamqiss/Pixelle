package repository

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/lib/pq"
	"sonet/src/services/starterpack_service/models"
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

// StarterpackRepository defines the interface for starterpack operations
type StarterpackRepository interface {
	// Starterpack operations
	CreateStarterpack(req models.CreateStarterpackRequest) (*models.Starterpack, error)
	GetStarterpack(starterpackID, requesterID string) (*models.Starterpack, error)
	GetUserStarterpacks(userID, requesterID string, limit int32, cursor string) ([]*models.Starterpack, string, error)
	UpdateStarterpack(req models.UpdateStarterpackRequest) (*models.Starterpack, error)
	DeleteStarterpack(starterpackID, requesterID string) error

	// Item operations
	AddStarterpackItem(req models.AddStarterpackItemRequest) (*models.StarterpackItem, error)
	RemoveStarterpackItem(starterpackID, itemID, removedBy string) error
	GetStarterpackItems(starterpackID, requesterID string, limit int32, cursor string) ([]*models.StarterpackItem, string, error)
	GetStarterpackItemCount(starterpackID string) (int32, error)

	// Permission operations
	CanViewStarterpack(starterpackID, requesterID string) (bool, error)
	CanEditStarterpack(starterpackID, requesterID string) (bool, error)
	CanAddToStarterpack(starterpackID, requesterID string) (bool, error)

	// Discovery operations
	GetSuggestedStarterpacks(userID string, limit int32, cursor string) ([]*models.Starterpack, string, error)
}

// StarterpackRepositoryImpl implements StarterpackRepository
type StarterpackRepositoryImpl struct {
	db *DatabaseConnection
}

// NewStarterpackRepository creates a new starterpack repository
func NewStarterpackRepository(db *DatabaseConnection) StarterpackRepository {
	return &StarterpackRepositoryImpl{db: db}
}