package models

import (
	"time"

	"google.golang.org/protobuf/types/known/timestamppb"
	pb "sonet/src/services/list_service/proto"
)

// ListType represents the type of a list
type ListType string

const (
	ListTypeCustom       ListType = "custom"
	ListTypeCloseFriends ListType = "close_friends"
	ListTypeFamily       ListType = "family"
	ListTypeWork         ListType = "work"
	ListTypeSchool       ListType = "school"
)

// List represents a user list
type List struct {
	ListID      string    `json:"list_id" db:"list_id"`
	OwnerID     string    `json:"owner_id" db:"owner_id"`
	Name        string    `json:"name" db:"name"`
	Description string    `json:"description" db:"description"`
	IsPublic    bool      `json:"is_public" db:"is_public"`
	ListType    ListType  `json:"list_type" db:"list_type"`
	CreatedAt   time.Time `json:"created_at" db:"created_at"`
	UpdatedAt   time.Time `json:"updated_at" db:"updated_at"`
	MemberCount int32     `json:"member_count" db:"member_count"`
}

// ListMember represents a member of a list
type ListMember struct {
	ListID   string    `json:"list_id" db:"list_id"`
	UserID   string    `json:"user_id" db:"user_id"`
	AddedBy  string    `json:"added_by" db:"added_by"`
	Notes    string    `json:"notes" db:"notes"`
	AddedAt  time.Time `json:"added_at" db:"added_at"`
}

// ToProto converts a List to protobuf
func (l *List) ToProto() *pb.List {
	return &pb.List{
		ListId:      l.ListID,
		OwnerId:     l.OwnerID,
		Name:        l.Name,
		Description: l.Description,
		IsPublic:    l.IsPublic,
		ListType:    l.ListType.ToProto(),
		CreatedAt:   timestamppb.New(l.CreatedAt),
		UpdatedAt:   timestamppb.New(l.UpdatedAt),
		MemberCount: l.MemberCount,
	}
}

// ToProto converts a ListMember to protobuf
func (lm *ListMember) ToProto() *pb.ListMember {
	return &pb.ListMember{
		ListId:  lm.ListID,
		UserId:  lm.UserID,
		AddedBy: lm.AddedBy,
		Notes:   lm.Notes,
		AddedAt: timestamppb.New(lm.AddedAt),
	}
}

// ToProto converts ListType to protobuf enum
func (lt ListType) ToProto() pb.ListType {
	switch lt {
	case ListTypeCustom:
		return pb.ListType_LIST_TYPE_CUSTOM
	case ListTypeCloseFriends:
		return pb.ListType_LIST_TYPE_CLOSE_FRIENDS
	case ListTypeFamily:
		return pb.ListType_LIST_TYPE_FAMILY
	case ListTypeWork:
		return pb.ListType_LIST_TYPE_WORK
	case ListTypeSchool:
		return pb.ListType_LIST_TYPE_SCHOOL
	default:
		return pb.ListType_LIST_TYPE_UNSPECIFIED
	}
}

// FromProto converts protobuf enum to ListType
func ListTypeFromProto(lt pb.ListType) ListType {
	switch lt {
	case pb.ListType_LIST_TYPE_CUSTOM:
		return ListTypeCustom
	case pb.ListType_LIST_TYPE_CLOSE_FRIENDS:
		return ListTypeCloseFriends
	case pb.ListType_LIST_TYPE_FAMILY:
		return ListTypeFamily
	case pb.ListType_LIST_TYPE_WORK:
		return ListTypeWork
	case pb.ListType_LIST_TYPE_SCHOOL:
		return ListTypeSchool
	default:
		return ListTypeCustom
	}
}

// CreateListRequest represents the request to create a list
type CreateListRequest struct {
	OwnerID     string   `json:"owner_id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	IsPublic    bool     `json:"is_public"`
	ListType    ListType `json:"list_type"`
}

// UpdateListRequest represents the request to update a list
type UpdateListRequest struct {
	ListID      string   `json:"list_id"`
	RequesterID string   `json:"requester_id"`
	Name        string   `json:"name"`
	Description string   `json:"description"`
	IsPublic    bool     `json:"is_public"`
	ListType    ListType `json:"list_type"`
}

// AddListMemberRequest represents the request to add a member to a list
type AddListMemberRequest struct {
	ListID  string `json:"list_id"`
	UserID  string `json:"user_id"`
	AddedBy string `json:"added_by"`
	Notes   string `json:"notes"`
}

// Pagination represents pagination parameters
type Pagination struct {
	Limit  int32  `json:"limit"`
	Cursor string `json:"cursor"`
}

// ListQuery represents a query for lists
type ListQuery struct {
	UserID     string     `json:"user_id"`
	RequesterID string    `json:"requester_id"`
	Pagination Pagination `json:"pagination"`
}

// MemberQuery represents a query for list members
type MemberQuery struct {
	ListID     string     `json:"list_id"`
	RequesterID string    `json:"requester_id"`
	Pagination Pagination `json:"pagination"`
}