package service

import (
	"context"
	"log"

	pb "sonet/src/services/list_service/proto"
	"sonet/src/services/list_service/repository"
	"sonet/src/services/list_service/models"
)

// ListService implements the gRPC ListService
type ListService struct {
	pb.UnimplementedListServiceServer
	repo repository.ListRepository
}

// NewListService creates a new list service
func NewListService(repo repository.ListRepository) *ListService {
	return &ListService{
		repo: repo,
	}
}

// CreateList creates a new list
func (s *ListService) CreateList(ctx context.Context, req *pb.CreateListRequest) (*pb.CreateListResponse, error) {
	log.Printf("Creating list for user %s: %s", req.OwnerId, req.Name)

	// Validate request
	if req.OwnerId == "" {
		return &pb.CreateListResponse{
			Success:      false,
			ErrorMessage: "owner_id is required",
		}, nil
	}

	if req.Name == "" {
		return &pb.CreateListResponse{
			Success:      false,
			ErrorMessage: "name is required",
		}, nil
	}

	// Convert request to internal model
	createReq := repository.CreateListRequest{
		OwnerID:     req.OwnerId,
		Name:        req.Name,
		Description: req.Description,
		IsPublic:    req.IsPublic,
		ListType:    models.ListTypeFromProto(req.ListType),
	}

	// Create list
	list, err := s.repo.CreateList(createReq)
	if err != nil {
		log.Printf("Failed to create list: %v", err)
		return &pb.CreateListResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.CreateListResponse{
		Success: true,
		List:    list.ToProto(),
	}, nil
}

// GetList retrieves a list by ID
func (s *ListService) GetList(ctx context.Context, req *pb.GetListRequest) (*pb.GetListResponse, error) {
	log.Printf("Getting list %s for user %s", req.ListId, req.RequesterId)

	// Validate request
	if req.ListId == "" {
		return &pb.GetListResponse{
			Success:      false,
			ErrorMessage: "list_id is required",
		}, nil
	}

	// Get list
	list, err := s.repo.GetList(req.ListId, req.RequesterId)
	if err != nil {
		log.Printf("Failed to get list: %v", err)
		return &pb.GetListResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.GetListResponse{
		Success: true,
		List:    list.ToProto(),
	}, nil
}

// GetUserLists retrieves lists owned by a user
func (s *ListService) GetUserLists(ctx context.Context, req *pb.GetUserListsRequest) (*pb.GetUserListsResponse, error) {
	log.Printf("Getting lists for user %s (requested by %s)", req.UserId, req.RequesterId)

	// Validate request
	if req.UserId == "" {
		return &pb.GetUserListsResponse{
			Success:      false,
			ErrorMessage: "user_id is required",
		}, nil
	}

	// Set default limit
	limit := req.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	// Get lists
	lists, nextCursor, err := s.repo.GetUserLists(req.UserId, req.RequesterId, limit, req.Cursor)
	if err != nil {
		log.Printf("Failed to get user lists: %v", err)
		return &pb.GetUserListsResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	// Convert to protobuf
	pbLists := make([]*pb.List, len(lists))
	for i, list := range lists {
		pbLists[i] = list.ToProto()
	}

	return &pb.GetUserListsResponse{
		Success:    true,
		Lists:      pbLists,
		NextCursor: nextCursor,
	}, nil
}

// UpdateList updates a list
func (s *ListService) UpdateList(ctx context.Context, req *pb.UpdateListRequest) (*pb.UpdateListResponse, error) {
	log.Printf("Updating list %s by user %s", req.ListId, req.RequesterId)

	// Validate request
	if req.ListId == "" {
		return &pb.UpdateListResponse{
			Success:      false,
			ErrorMessage: "list_id is required",
		}, nil
	}

	if req.RequesterId == "" {
		return &pb.UpdateListResponse{
			Success:      false,
			ErrorMessage: "requester_id is required",
		}, nil
	}

	// Convert request to internal model
	updateReq := repository.UpdateListRequest{
		ListID:      req.ListId,
		RequesterID: req.RequesterId,
		Name:        req.Name,
		Description: req.Description,
		IsPublic:    req.IsPublic,
		ListType:    models.ListTypeFromProto(req.ListType),
	}

	// Update list
	list, err := s.repo.UpdateList(updateReq)
	if err != nil {
		log.Printf("Failed to update list: %v", err)
		return &pb.UpdateListResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.UpdateListResponse{
		Success: true,
		List:    list.ToProto(),
	}, nil
}

// DeleteList deletes a list
func (s *ListService) DeleteList(ctx context.Context, req *pb.DeleteListRequest) (*pb.DeleteListResponse, error) {
	log.Printf("Deleting list %s by user %s", req.ListId, req.RequesterId)

	// Validate request
	if req.ListId == "" {
		return &pb.DeleteListResponse{
			Success:      false,
			ErrorMessage: "list_id is required",
		}, nil
	}

	if req.RequesterId == "" {
		return &pb.DeleteListResponse{
			Success:      false,
			ErrorMessage: "requester_id is required",
		}, nil
	}

	// Delete list
	err := s.repo.DeleteList(req.ListId, req.RequesterId)
	if err != nil {
		log.Printf("Failed to delete list: %v", err)
		return &pb.DeleteListResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.DeleteListResponse{
		Success: true,
	}, nil
}

// AddListMember adds a member to a list
func (s *ListService) AddListMember(ctx context.Context, req *pb.AddListMemberRequest) (*pb.AddListMemberResponse, error) {
	log.Printf("Adding user %s to list %s by user %s", req.UserId, req.ListId, req.AddedBy)

	// Validate request
	if req.ListId == "" {
		return &pb.AddListMemberResponse{
			Success:      false,
			ErrorMessage: "list_id is required",
		}, nil
	}

	if req.UserId == "" {
		return &pb.AddListMemberResponse{
			Success:      false,
			ErrorMessage: "user_id is required",
		}, nil
	}

	if req.AddedBy == "" {
		return &pb.AddListMemberResponse{
			Success:      false,
			ErrorMessage: "added_by is required",
		}, nil
	}

	// Convert request to internal model
	addReq := repository.AddListMemberRequest{
		ListID:  req.ListId,
		UserID:  req.UserId,
		AddedBy: req.AddedBy,
		Notes:   req.Notes,
	}

	// Add member
	member, err := s.repo.AddListMember(addReq)
	if err != nil {
		log.Printf("Failed to add list member: %v", err)
		return &pb.AddListMemberResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.AddListMemberResponse{
		Success: true,
		Member:  member.ToProto(),
	}, nil
}

// RemoveListMember removes a member from a list
func (s *ListService) RemoveListMember(ctx context.Context, req *pb.RemoveListMemberRequest) (*pb.RemoveListMemberResponse, error) {
	log.Printf("Removing user %s from list %s by user %s", req.UserId, req.ListId, req.RemovedBy)

	// Validate request
	if req.ListId == "" {
		return &pb.RemoveListMemberResponse{
			Success:      false,
			ErrorMessage: "list_id is required",
		}, nil
	}

	if req.UserId == "" {
		return &pb.RemoveListMemberResponse{
			Success:      false,
			ErrorMessage: "user_id is required",
		}, nil
	}

	if req.RemovedBy == "" {
		return &pb.RemoveListMemberResponse{
			Success:      false,
			ErrorMessage: "removed_by is required",
		}, nil
	}

	// Remove member
	err := s.repo.RemoveListMember(req.ListId, req.UserId, req.RemovedBy)
	if err != nil {
		log.Printf("Failed to remove list member: %v", err)
		return &pb.RemoveListMemberResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.RemoveListMemberResponse{
		Success: true,
	}, nil
}

// GetListMembers retrieves members of a list
func (s *ListService) GetListMembers(ctx context.Context, req *pb.GetListMembersRequest) (*pb.GetListMembersResponse, error) {
	log.Printf("Getting members for list %s (requested by %s)", req.ListId, req.RequesterId)

	// Validate request
	if req.ListId == "" {
		return &pb.GetListMembersResponse{
			Success:      false,
			ErrorMessage: "list_id is required",
		}, nil
	}

	// Set default limit
	limit := req.Limit
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}

	// Get members
	members, nextCursor, err := s.repo.GetListMembers(req.ListId, req.RequesterId, limit, req.Cursor)
	if err != nil {
		log.Printf("Failed to get list members: %v", err)
		return &pb.GetListMembersResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	// Convert to protobuf
	pbMembers := make([]*pb.ListMember, len(members))
	for i, member := range members {
		pbMembers[i] = member.ToProto()
	}

	return &pb.GetListMembersResponse{
		Success:    true,
		Members:    pbMembers,
		NextCursor: nextCursor,
	}, nil
}

// IsUserInList checks if a user is in a list
func (s *ListService) IsUserInList(ctx context.Context, req *pb.IsUserInListRequest) (*pb.IsUserInListResponse, error) {
	log.Printf("Checking if user %s is in list %s (requested by %s)", req.UserId, req.ListId, req.RequesterId)

	// Validate request
	if req.ListId == "" {
		return &pb.IsUserInListResponse{
			Success:      false,
			ErrorMessage: "list_id is required",
		}, nil
	}

	if req.UserId == "" {
		return &pb.IsUserInListResponse{
			Success:      false,
			ErrorMessage: "user_id is required",
		}, nil
	}

	// Check if user can view this list
	canView, err := s.repo.CanViewList(req.ListId, req.RequesterId)
	if err != nil {
		log.Printf("Failed to check list permissions: %v", err)
		return &pb.IsUserInListResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	if !canView {
		return &pb.IsUserInListResponse{
			Success:      false,
			ErrorMessage: "permission denied",
		}, nil
	}

	// Check if user is in list
	isMember, err := s.repo.IsUserInList(req.ListId, req.UserId)
	if err != nil {
		log.Printf("Failed to check if user is in list: %v", err)
		return &pb.IsUserInListResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.IsUserInListResponse{
		Success:   true,
		IsMember:  isMember,
	}, nil
}