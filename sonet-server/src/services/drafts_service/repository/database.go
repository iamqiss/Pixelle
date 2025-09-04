package repository

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"time"

	_ "github.com/lib/pq"
	"google.golang.org/protobuf/types/known/timestamppb"
	pb "sonet/src/services/drafts_service/proto"
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

	db, err := sql.Open("notegres", connStr)
	if err != nil {
		return nil, fmt.Errorf("failed to open database: %w", err)
	}

	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	log.Println("Connected to database successfully")
	return &DatabaseConnection{db}, nil
}

// DraftsRepository defines the interface for draft data operations
type DraftsRepository interface {
	CreateDraft(draft *Draft) error
	GetUserDrafts(userID string, limit int32, cursor string, includeAutoSaved bool) ([]*Draft, string, error)
	GetDraft(draftID, userID string) (*Draft, error)
	UpdateDraft(draft *Draft) error
	DeleteDraft(draftID, userID string) error
	AutoSaveDraft(userID string, content string, replyToURI, quoteURI, mentionHandle string, images []DraftImage, video *DraftVideo, labels []string, threadgate, interactionSettings []byte) (*Draft, error)
}

// Draft represents a note draft
type Draft struct {
	DraftID              string    `json:"draft_id"`
	UserID               string    `json:"user_id"`
	Title                string    `json:"title"`
	Content              string    `json:"content"`
	ReplyToURI           string    `json:"reply_to_uri"`
	QuoteURI             string    `json:"quote_uri"`
	MentionHandle        string    `json:"mention_handle"`
	Images               []DraftImage `json:"images"`
	Video                *DraftVideo `json:"video"`
	Labels               []string  `json:"labels"`
	Threadgate           []byte    `json:"threadgate"`
	InteractionSettings  []byte    `json:"interaction_settings"`
	CreatedAt            time.Time `json:"created_at"`
	UpdatedAt            time.Time `json:"updated_at"`
	IsAutoSaved          bool      `json:"is_auto_saved"`
}

// DraftImage represents an image in a draft
type DraftImage struct {
	URI     string `json:"uri"`
	Width   int32  `json:"width"`
	Height  int32  `json:"height"`
	AltText string `json:"alt_text"`
}

// DraftVideo represents a video in a draft
type DraftVideo struct {
	URI    string `json:"uri"`
	Width  int32  `json:"width"`
	Height int32  `json:"height"`
}

// ToProto converts Draft to protobuf message
func (d *Draft) ToProto() *pb.Draft {
	images := make([]*pb.DraftImage, len(d.Images))
	for i, img := range d.Images {
		images[i] = &pb.DraftImage{
			Uri:     img.URI,
			Width:   img.Width,
			Height:  img.Height,
			AltText: img.AltText,
		}
	}

	var video *pb.DraftVideo
	if d.Video != nil {
		video = &pb.DraftVideo{
			Uri:    d.Video.URI,
			Width:  d.Video.Width,
			Height: d.Video.Height,
		}
	}

	return &pb.Draft{
		DraftId:             d.DraftID,
		UserId:              d.UserID,
		Title:               d.Title,
		Content:             d.Content,
		ReplyToUri:          d.ReplyToURI,
		QuoteUri:            d.QuoteURI,
		MentionHandle:       d.MentionHandle,
		Images:              images,
		Video:               video,
		Labels:              d.Labels,
		Threadgate:          d.Threadgate,
		InteractionSettings: d.InteractionSettings,
		CreatedAt:           timestamppb.New(d.CreatedAt),
		UpdatedAt:           timestamppb.New(d.UpdatedAt),
		IsAutoSaved:         d.IsAutoSaved,
	}
}

// DraftsRepositoryImpl implements DraftsRepository
type DraftsRepositoryImpl struct {
	db *DatabaseConnection
}

// NewDraftsRepository creates a new drafts repository
func NewDraftsRepository(db *DatabaseConnection) DraftsRepository {
	return &DraftsRepositoryImpl{db: db}
}