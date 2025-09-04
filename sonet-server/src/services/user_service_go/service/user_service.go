package service

import (
	"context"
	"log"

	pb "sonet/src/services/user_service_go/proto"
	"sonet/src/services/user_service_go/repository"
)

// UserService implements the gRPC UserService
type UserService struct {
	pb.UnimplementedUserServiceServer
	repo repository.UserRepository
}

// NewUserService creates a new user service
func NewUserService(repo repository.UserRepository) *UserService {
	return &UserService{repo: repo}
}

// GetUserPrivacy retrieves user privacy settings
func (s *UserService) GetUserPrivacy(ctx context.Context, req *pb.GetUserPrivacyRequest) (*pb.GetUserPrivacyResponse, error) {
	log.Printf("Getting privacy settings for user %s", req.UserId)

	if req.UserId == "" {
		return &pb.GetUserPrivacyResponse{
			Success:      false,
			ErrorMessage: "user_id is required",
		}, nil
	}

	isPrivate, err := s.repo.GetUserPrivacy(req.UserId)
	if err != nil {
		log.Printf("Failed to get user privacy: %v", err)
		return &pb.GetUserPrivacyResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.GetUserPrivacyResponse{
		Success:   true,
		IsPrivate: isPrivate,
	}, nil
}

// UpdateUserPrivacy updates user privacy settings
func (s *UserService) UpdateUserPrivacy(ctx context.Context, req *pb.UpdateUserPrivacyRequest) (*pb.UpdateUserPrivacyResponse, error) {
	log.Printf("Updating privacy settings for user %s: is_private=%v", req.UserId, req.IsPrivate)

	if req.UserId == "" {
		return &pb.UpdateUserPrivacyResponse{
			Success:      false,
			ErrorMessage: "user_id is required",
		}, nil
	}

	err := s.repo.UpdateUserPrivacy(req.UserId, req.IsPrivate)
	if err != nil {
		log.Printf("Failed to update user privacy: %v", err)
		return &pb.UpdateUserPrivacyResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.UpdateUserPrivacyResponse{
		Success:   true,
		IsPrivate: req.IsPrivate,
	}, nil
}

// GetUserProfile retrieves a user profile with privacy check
func (s *UserService) GetUserProfile(ctx context.Context, req *pb.GetUserProfileRequest) (*pb.GetUserProfileResponse, error) {
	log.Printf("Getting profile for user %s (requested by %s)", req.UserId, req.RequesterId)

	if req.UserId == "" {
		return &pb.GetUserProfileResponse{
			Success:      false,
			ErrorMessage: "user_id is required",
		}, nil
	}

	user, err := s.repo.GetUserProfile(req.UserId, req.RequesterId)
	if err != nil {
		log.Printf("Failed to get user profile: %v", err)
		return &pb.GetUserProfileResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.GetUserProfileResponse{
		Success: true,
		Profile: user.ToProto(),
	}, nil
}