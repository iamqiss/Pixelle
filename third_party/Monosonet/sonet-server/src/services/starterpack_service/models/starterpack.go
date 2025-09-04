package models

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	pb "sonet/src/services/starterpack_service/proto"
)

// ItemType represents the type of a starterpack item
type ItemType string

const (
	ItemTypeProfile ItemType = "profile"
	ItemTypeFeed    ItemType = "feed"
)

// Starterpack represents a user starterpack
type Starterpack struct {
	StarterpackID string    `json:"starterpack_id" db:"starterpack_id"`
	CreatorID     string    `json:"creator_id" db:"creator_id"`
	Name          string    `json:"name" db:"name"`
	Description   string    `json:"description" db:"description"`
	AvatarURL     string    `json:"avatar_url" db:"avatar_url"`
	IsPublic      bool      `json:"is_public" db:"is_public"`
	CreatedAt     time.Time `json:"created_at" db:"created_at"`
	UpdatedAt     time.Time `json:"updated_at" db:"updated_at"`
	ItemCount     int32     `json:"item_count" db:"item_count"`
}

// StarterpackItem represents an item in a starterpack
type StarterpackItem struct {
	ItemID         string    `json:"item_id" db:"item_id"`
	StarterpackID  string    `json:"starterpack_id" db:"starterpack_id"`
	ItemType       ItemType  `json:"item_type" db:"item_type"`
	ItemURI        string    `json:"item_uri" db:"item_uri"`
	ItemOrder      int32     `json:"item_order" db:"item_order"`
	CreatedAt      time.Time `json:"created_at" db:"created_at"`
}

// ToProto converts a Starterpack to protobuf
func (s *Starterpack) ToProto() *pb.Starterpack {
	return &pb.Starterpack{
		StarterpackId: s.StarterpackID,
		CreatorId:     s.CreatorID,
		Name:          s.Name,
		Description:   s.Description,
		AvatarUrl:     s.AvatarURL,
		IsPublic:      s.IsPublic,
		CreatedAt:     timestamppb.New(s.CreatedAt),
		UpdatedAt:     timestamppb.New(s.UpdatedAt),
		ItemCount:     s.ItemCount,
	}
}

// ToProto converts a StarterpackItem to protobuf
func (si *StarterpackItem) ToProto() *pb.StarterpackItem {
	return &pb.StarterpackItem{
		ItemId:        si.ItemID,
		StarterpackId: si.StarterpackID,
		ItemType:      si.ItemType.ToProto(),
		ItemUri:       si.ItemURI,
		ItemOrder:     si.ItemOrder,
		CreatedAt:     timestamppb.New(si.CreatedAt),
	}
}

// ToProto converts ItemType to protobuf enum
func (it ItemType) ToProto() pb.ItemType {
	switch it {
	case ItemTypeProfile:
		return pb.ItemType_ITEM_TYPE_PROFILE
	case ItemTypeFeed:
		return pb.ItemType_ITEM_TYPE_FEED
	default:
		return pb.ItemType_ITEM_TYPE_UNSPECIFIED
	}
}

// FromProto converts protobuf enum to ItemType
func ItemTypeFromProto(it pb.ItemType) ItemType {
	switch it {
	case pb.ItemType_ITEM_TYPE_PROFILE:
		return ItemTypeProfile
	case pb.ItemType_ITEM_TYPE_FEED:
		return ItemTypeFeed
	default:
		return ItemTypeProfile
	}
}

// CreateStarterpackRequest represents the request to create a starterpack
type CreateStarterpackRequest struct {
	CreatorID   string `json:"creator_id"`
	Name        string `json:"name"`
	Description string `json:"description"`
	AvatarURL   string `json:"avatar_url"`
	IsPublic    bool   `json:"is_public"`
}

// UpdateStarterpackRequest represents the request to update a starterpack
type UpdateStarterpackRequest struct {
	StarterpackID string `json:"starterpack_id"`
	RequesterID   string `json:"requester_id"`
	Name          string `json:"name"`
	Description   string `json:"description"`
	AvatarURL     string `json:"avatar_url"`
	IsPublic      bool   `json:"is_public"`
}

// AddStarterpackItemRequest represents the request to add an item to a starterpack
type AddStarterpackItemRequest struct {
	StarterpackID string   `json:"starterpack_id"`
	ItemType      ItemType `json:"item_type"`
	ItemURI       string   `json:"item_uri"`
	ItemOrder     int32    `json:"item_order"`
	AddedBy       string   `json:"added_by"`
}

// Pagination represents pagination parameters
type Pagination struct {
	Limit  int32  `json:"limit"`
	Cursor string `json:"cursor"`
}

// StarterpackQuery represents a query for starterpacks
type StarterpackQuery struct {
	UserID     string     `json:"user_id"`
	RequesterID string    `json:"requester_id"`
	Pagination Pagination `json:"pagination"`
}

// ItemQuery represents a query for starterpack items
type ItemQuery struct {
	StarterpackID string     `json:"starterpack_id"`
	RequesterID   string     `json:"requester_id"`
	Pagination    Pagination `json:"pagination"`
}