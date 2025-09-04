package models

import (
	"time"
	"google.golang.org/protobuf/types/known/timestamppb"
	pb "sonet/src/services/user_service_go/proto"
)

// User represents a user in the system
type User struct {
	UserID        string    `json:"user_id"`
	Username      string    `json:"username"`
	DisplayName   string    `json:"display_name"`
	Bio           string    `json:"bio"`
	AvatarURL     string    `json:"avatar_url"`
	BannerURL     string    `json:"banner_url"`
	Location      string    `json:"location"`
	Website       string    `json:"website"`
	IsVerified    bool      `json:"is_verified"`
	IsPrivate     bool      `json:"is_private"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	Stats         UserStats `json:"stats"`
	Viewer        UserViewer `json:"viewer"`
}

// UserStats represents user statistics
type UserStats struct {
	FollowerCount  int64 `json:"follower_count"`
	FollowingCount int64 `json:"following_count"`
	NoteCount      int64 `json:"note_count"`
	LikeCount      int64 `json:"like_count"`
	RenoteCount    int64 `json:"renote_count"`
	CommentCount   int64 `json:"comment_count"`
}

// UserViewer represents viewer information for a user
type UserViewer struct {
	Following   bool `json:"following"`
	FollowedBy  bool `json:"followed_by"`
	Muted       bool `json:"muted"`
	BlockedBy   bool `json:"blocked_by"`
	Blocking    bool `json:"blocking"`
}

// ToProto converts User to protobuf message
func (u *User) ToProto() *pb.UserProfile {
	return &pb.UserProfile{
		UserId:      u.UserID,
		Username:    u.Username,
		DisplayName: u.DisplayName,
		Bio:         u.Bio,
		AvatarUrl:   u.AvatarURL,
		BannerUrl:   u.BannerURL,
		Location:    u.Location,
		Website:     u.Website,
		IsVerified:  u.IsVerified,
		IsPrivate:   u.IsPrivate,
		CreatedAt:   timestamppb.New(u.CreatedAt),
		UpdatedAt:   timestamppb.New(u.UpdatedAt),
		Stats: &pb.UserStats{
			FollowerCount:  u.Stats.FollowerCount,
			FollowingCount: u.Stats.FollowingCount,
			NoteCount:      u.Stats.NoteCount,
			LikeCount:      u.Stats.LikeCount,
			RenoteCount:    u.Stats.RenoteCount,
			CommentCount:   u.Stats.CommentCount,
		},
		Viewer: &pb.UserViewer{
			Following:  u.Viewer.Following,
			FollowedBy: u.Viewer.FollowedBy,
			Muted:      u.Viewer.Muted,
			BlockedBy:  u.Viewer.BlockedBy,
			Blocking:   u.Viewer.Blocking,
		},
	}
}

// FromProto converts protobuf message to User
func (u *User) FromProto(proto *pb.UserProfile) {
	u.UserID = proto.UserId
	u.Username = proto.Username
	u.DisplayName = proto.DisplayName
	u.Bio = proto.Bio
	u.AvatarURL = proto.AvatarUrl
	u.BannerURL = proto.BannerUrl
	u.Location = proto.Location
	u.Website = proto.Website
	u.IsVerified = proto.IsVerified
	u.IsPrivate = proto.IsPrivate
	if proto.CreatedAt != nil {
		u.CreatedAt = proto.CreatedAt.AsTime()
	}
	if proto.UpdatedAt != nil {
		u.UpdatedAt = proto.UpdatedAt.AsTime()
	}
	if proto.Stats != nil {
		u.Stats.FollowerCount = proto.Stats.FollowerCount
		u.Stats.FollowingCount = proto.Stats.FollowingCount
		u.Stats.NoteCount = proto.Stats.NoteCount
		u.Stats.LikeCount = proto.Stats.LikeCount
		u.Stats.RenoteCount = proto.Stats.RenoteCount
		u.Stats.CommentCount = proto.Stats.CommentCount
	}
	if proto.Viewer != nil {
		u.Viewer.Following = proto.Viewer.Following
		u.Viewer.FollowedBy = proto.Viewer.FollowedBy
		u.Viewer.Muted = proto.Viewer.Muted
		u.Viewer.BlockedBy = proto.Viewer.BlockedBy
		u.Viewer.Blocking = proto.Viewer.Blocking
	}
}