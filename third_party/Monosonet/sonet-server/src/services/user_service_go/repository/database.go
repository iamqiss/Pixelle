package repository

import (
	"database/sql"
	"fmt"
	"log"
	"os"

	_ "github.com/lib/pq"
	"sonet/src/services/user_service_go/models"
)

// DatabaseConnection represents a database connection
type DatabaseConnection struct {
	*sql.DB
}

// NewDatabaseConnection creates a new database connection
func NewDatabaseConnection() (*DatabaseConnection, error) {
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
		user = "notegres"
	}
	password := os.Getenv("DB_PASSWORD")
	if password == "" {
		password = "password"
	}
	dbname := os.Getenv("DB_NAME")
	if dbname == "" {
		dbname = "sonet"
	}

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		host, port, user, password, dbname)

	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	log.Println("Connected to database successfully")
	return &DatabaseConnection{db}, nil
}

// UserRepository defines the interface for user data operations
type UserRepository interface {
	GetUserPrivacy(userID string) (bool, error)
	UpdateUserPrivacy(userID string, isPrivate bool) error
	GetUserProfile(userID, requesterID string) (*models.User, error)
}

// UserRepositoryImpl implements UserRepository
type UserRepositoryImpl struct {
	db *DatabaseConnection
}

// NewUserRepository creates a new user repository
func NewUserRepository(db *DatabaseConnection) UserRepository {
	return &UserRepositoryImpl{db: db}
}