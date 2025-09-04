package service

import (
	"context"
	"log"

	pb "sonet/src/services/starterpack_service/proto"
	"sonet/src/services/starterpack_service/repository"
	"sonet/src/services/starterpack_service/models"
)

// StarterpackService implements the gRPC StarterpackService
type StarterpackService struct {
	pb.UnimplementedStarterpackServiceServer
	repo repository.StarterpackRepository
}

// NewStarterpackService creates a new starterpack service
func NewStarterpackService(repo repository.StarterpackRepository) *StarterpackService {
	return &StarterpackService{repo: repo}
}

// CreateStarterpack creates a new starterpack
func (s *StarterpackService) CreateStarterpack(ctx context.Context, req *pb.CreateStarterpackRequest) (*pb.CreateStarterpackResponse, error) {
	log.Printf("Creating starterpack for user %s: %s", req.CreatorId, req.Name)

	// Validate request
	if req.CreatorId == "" {
		return &pb.CreateStarterpackResponse{
			Success:      false,
			ErrorMessage: "creator_id is required",
		}, nil
	}

	if req.Name == "" {
		return &pb.CreateStarterpackResponse{
			Success:      false,
			ErrorMessage: "name is required",
		}, nil
	}

	// Convert request to internal model
	createReq := models.CreateStarterpackRequest{
		CreatorID:   req.CreatorId,
		Name:        req.Name,
		Description: req.Description,
		AvatarURL:   req.AvatarUrl,
		IsPublic:    req.IsPublic,
	}

	// Create starterpack
	starterpack, err := s.repo.CreateStarterpack(createReq)
	if err != nil {
		log.Printf("Failed to create starterpack: %v", err)
		return &pb.CreateStarterpackResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.CreateStarterpackResponse{
		Success:     true,
		Starterpack: starterpack.ToProto(),
	}, nil
}

// GetStarterpack retrieves a starterpack by ID
func (s *StarterpackService) GetStarterpack(ctx context.Context, req *pb.GetStarterpackRequest) (*pb.GetStarterpackResponse, error) {
	log.Printf("Getting starterpack %s for user %s", req.StarterpackId, req.RequesterId)

	// Validate request
	if req.StarterpackId == "" {
		return &pb.GetStarterpackResponse{
			Success:      false,
			ErrorMessage: "starterpack_id is required",
		}, nil
	}

	// Get starterpack
	starterpack, err := s.repo.GetStarterpack(req.StarterpackId, req.RequesterId)
	if err != nil {
		log.Printf("Failed to get starterpack: %v", err)
		return &pb.GetStarterpackResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.GetStarterpackResponse{
		Success:     true,
		Starterpack: starterpack.ToProto(),
	}, nil
}

// GetUserStarterpacks retrieves starterpacks created by a user
func (s *StarterpackService) GetUserStarterpacks(ctx context.Context, req *pb.GetUserStarterpacksRequest) (*pb.GetUserStarterpacksResponse, error) {
	log.Printf("Getting starterpacks for user %s (requested by %s)", req.UserId, req.RequesterId)

	// Validate request
	if req.UserId == "" {
		return &pb.GetUserStarterpacksResponse{
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

	// Get starterpacks
	starterpacks, nextCursor, err := s.repo.GetUserStarterpacks(req.UserId, req.RequesterId, limit, req.Cursor)
	if err != nil {
		log.Printf("Failed to get user starterpacks: %v", err)
		return &pb.GetUserStarterpacksResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	// Convert to protobuf
	pbStarterpacks := make([]*pb.Starterpack, len(starterpacks))
	for i, starterpack := range starterpacks {
		pbStarterpacks[i] = starterpack.ToProto()
	}

	return &pb.GetUserStarterpacksResponse{
		Success:      true,
		Starterpacks: pbStarterpacks,
		NextCursor:   nextCursor,
	}, nil
}

// UpdateStarterpack updates a starterpack
func (s *StarterpackService) UpdateStarterpack(ctx context.Context, req *pb.UpdateStarterpackRequest) (*pb.UpdateStarterpackResponse, error) {
	log.Printf("Updating starterpack %s by user %s", req.StarterpackId, req.RequesterId)

	// Validate request
	if req.StarterpackId == "" {
		return &pb.UpdateStarterpackResponse{
			Success:      false,
			ErrorMessage: "starterpack_id is required",
		}, nil
	}

	if req.RequesterId == "" {
		return &pb.UpdateStarterpackResponse{
			Success:      false,
			ErrorMessage: "requester_id is required",
		}, nil
	}

	// Convert request to internal model
	updateReq := models.UpdateStarterpackRequest{
		StarterpackID: req.StarterpackId,
		RequesterID:   req.RequesterId,
		Name:          req.Name,
		Description:   req.Description,
		AvatarURL:     req.AvatarUrl,
		IsPublic:      req.IsPublic,
	}

	// Update starterpack
	starterpack, err := s.repo.UpdateStarterpack(updateReq)
	if err != nil {
		log.Printf("Failed to update starterpack: %v", err)
		return &pb.UpdateStarterpackResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.UpdateStarterpackResponse{
		Success:     true,
		Starterpack: starterpack.ToProto(),
	}, nil
}

// DeleteStarterpack deletes a starterpack
func (s *StarterpackService) DeleteStarterpack(ctx context.Context, req *pb.DeleteStarterpackRequest) (*pb.DeleteStarterpackResponse, error) {
	log.Printf("Deleting starterpack %s by user %s", req.StarterpackId, req.RequesterId)

	// Validate request
	if req.StarterpackId == "" {
		return &pb.DeleteStarterpackResponse{
			Success:      false,
			ErrorMessage: "starterpack_id is required",
		}, nil
	}

	if req.RequesterId == "" {
		return &pb.DeleteStarterpackResponse{
			Success:      false,
			ErrorMessage: "requester_id is required",
		}, nil
	}

	// Delete starterpack
	err := s.repo.DeleteStarterpack(req.StarterpackId, req.RequesterId)
	if err != nil {
		log.Printf("Failed to delete starterpack: %v", err)
		return &pb.DeleteStarterpackResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.DeleteStarterpackResponse{
		Success: true,
	}, nil
}

// AddStarterpackItem adds an item to a starterpack
func (s *StarterpackService) AddStarterpackItem(ctx context.Context, req *pb.AddStarterpackItemRequest) (*pb.AddStarterpackItemResponse, error) {
	log.Printf("Adding item to starterpack %s by user %s", req.StarterpackId, req.AddedBy)

	// Validate request
	if req.StarterpackId == "" {
		return &pb.AddStarterpackItemResponse{
			Success:      false,
			ErrorMessage: "starterpack_id is required",
		}, nil
	}

	if req.ItemUri == "" {
		return &pb.AddStarterpackItemResponse{
			Success:      false,
			ErrorMessage: "item_uri is required",
		}, nil
	}

	if req.AddedBy == "" {
		return &pb.AddStarterpackItemResponse{
			Success:      false,
			ErrorMessage: "added_by is required",
		}, nil
	}

	// Convert request to internal model
	addReq := models.AddStarterpackItemRequest{
		StarterpackID: req.StarterpackId,
		ItemType:      models.ItemTypeFromProto(req.ItemType),
		ItemURI:       req.ItemUri,
		ItemOrder:     req.ItemOrder,
		AddedBy:       req.AddedBy,
	}

	// Add item
	item, err := s.repo.AddStarterpackItem(addReq)
	if err != nil {
		log.Printf("Failed to add starterpack item: %v", err)
		return &pb.AddStarterpackItemResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.AddStarterpackItemResponse{
		Success: true,
		Item:    item.ToProto(),
	}, nil
}

// RemoveStarterpackItem removes an item from a starterpack
func (s *StarterpackService) RemoveStarterpackItem(ctx context.Context, req *pb.RemoveStarterpackItemRequest) (*pb.RemoveStarterpackItemResponse, error) {
	log.Printf("Removing item %s from starterpack %s by user %s", req.ItemId, req.StarterpackId, req.RemovedBy)

	// Validate request
	if req.StarterpackId == "" {
		return &pb.RemoveStarterpackItemResponse{
			Success:      false,
			ErrorMessage: "starterpack_id is required",
		}, nil
	}

	if req.ItemId == "" {
		return &pb.RemoveStarterpackItemResponse{
			Success:      false,
			ErrorMessage: "item_id is required",
		}, nil
	}

	if req.RemovedBy == "" {
		return &pb.RemoveStarterpackItemResponse{
			Success:      false,
			ErrorMessage: "removed_by is required",
		}, nil
	}

	// Remove item
	err := s.repo.RemoveStarterpackItem(req.StarterpackId, req.ItemId, req.RemovedBy)
	if err != nil {
		log.Printf("Failed to remove starterpack item: %v", err)
		return &pb.RemoveStarterpackItemResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.RemoveStarterpackItemResponse{
		Success: true,
	}, nil
}

// GetStarterpackItems retrieves items of a starterpack
func (s *StarterpackService) GetStarterpackItems(ctx context.Context, req *pb.GetStarterpackItemsRequest) (*pb.GetStarterpackItemsResponse, error) {
	log.Printf("Getting items for starterpack %s (requested by %s)", req.StarterpackId, req.RequesterId)

	// Validate request
	if req.StarterpackId == "" {
		return &pb.GetStarterpackItemsResponse{
			Success:      false,
			ErrorMessage: "starterpack_id is required",
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

	// Get items
	items, nextCursor, err := s.repo.GetStarterpackItems(req.StarterpackId, req.RequesterId, limit, req.Cursor)
	if err != nil {
		log.Printf("Failed to get starterpack items: %v", err)
		return &pb.GetStarterpackItemsResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	// Convert to protobuf
	pbItems := make([]*pb.StarterpackItem, len(items))
	for i, item := range items {
		pbItems[i] = item.ToProto()
	}

	return &pb.GetStarterpackItemsResponse{
		Success:    true,
		Items:      pbItems,
		NextCursor: nextCursor,
	}, nil
}

// GetSuggestedStarterpacks gets suggested starterpacks for a user
func (s *StarterpackService) GetSuggestedStarterpacks(ctx context.Context, req *pb.GetSuggestedStarterpacksRequest) (*pb.GetSuggestedStarterpacksResponse, error) {
	log.Printf("Getting suggested starterpacks for user %s", req.UserId)

	// Validate request
	if req.UserId == "" {
		return &pb.GetSuggestedStarterpacksResponse{
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

	// Get suggested starterpacks
	starterpacks, nextCursor, err := s.repo.GetSuggestedStarterpacks(req.UserId, limit, req.Cursor)
	if err != nil {
		log.Printf("Failed to get suggested starterpacks: %v", err)
		return &pb.GetSuggestedStarterpacksResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	// Convert to protobuf
	pbStarterpacks := make([]*pb.Starterpack, len(starterpacks))
	for i, starterpack := range starterpacks {
		pbStarterpacks[i] = starterpack.ToProto()
	}

	return &pb.GetSuggestedStarterpacksResponse{
		Success:      true,
		Starterpacks: pbStarterpacks,
		NextCursor:   nextCursor,
	}, nil
}