package service

import (
	"context"
	"log"

	pb "sonet/src/services/drafts_service/proto"
	"sonet/src/services/drafts_service/repository"
)

// DraftsService implements the gRPC DraftsService
type DraftsService struct {
	pb.UnimplementedDraftsServiceServer
	repo repository.DraftsRepository
}

// NewDraftsService creates a new drafts service
func NewDraftsService(repo repository.DraftsRepository) *DraftsService {
	return &DraftsService{repo: repo}
}

// CreateDraft creates a new draft
func (s *DraftsService) CreateDraft(ctx context.Context, req *pb.CreateDraftRequest) (*pb.CreateDraftResponse, error) {
	log.Printf("Creating draft for user %s", req.UserId)

	if req.UserId == "" {
		return &pb.CreateDraftResponse{
			Success:      false,
			ErrorMessage: "user_id is required",
		}, nil
	}

	if req.Content == "" {
		return &pb.CreateDraftResponse{
			Success:      false,
			ErrorMessage: "content is required",
		}, nil
	}

	// Convert protobuf images to models
	images := make([]repository.DraftImage, len(req.Images))
	for i, img := range req.Images {
		images[i] = repository.DraftImage{
			URI:     img.Uri,
			Width:   img.Width,
			Height:  img.Height,
			AltText: img.AltText,
		}
	}

	// Convert protobuf video to model
	var video *repository.DraftVideo
	if req.Video != nil {
		video = &repository.DraftVideo{
			URI:    req.Video.Uri,
			Width:  req.Video.Width,
			Height: req.Video.Height,
		}
	}

	draft := &repository.Draft{
		UserID:              req.UserId,
		Content:             req.Content,
		ReplyToURI:          req.ReplyToUri,
		QuoteURI:            req.QuoteUri,
		MentionHandle:       req.MentionHandle,
		Images:              images,
		Video:               video,
		Labels:              req.Labels,
		Threadgate:          req.Threadgate,
		InteractionSettings: req.InteractionSettings,
		IsAutoSaved:         req.IsAutoSaved,
	}

	if err := s.repo.CreateDraft(draft); err != nil {
		log.Printf("Failed to create draft: %v", err)
		return &pb.CreateDraftResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.CreateDraftResponse{
		Success: true,
		Draft:   draft.ToProto(),
	}, nil
}

// GetUserDrafts retrieves drafts for a user
func (s *DraftsService) GetUserDrafts(ctx context.Context, req *pb.GetUserDraftsRequest) (*pb.GetUserDraftsResponse, error) {
	log.Printf("Getting drafts for user %s", req.UserId)

	if req.UserId == "" {
		return &pb.GetUserDraftsResponse{
			Success:      false,
			ErrorMessage: "user_id is required",
		}, nil
	}

	drafts, nextCursor, err := s.repo.GetUserDrafts(req.UserId, req.Limit, req.Cursor, req.IncludeAutoSaved)
	if err != nil {
		log.Printf("Failed to get user drafts: %v", err)
		return &pb.GetUserDraftsResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	// Convert models to protobuf
	pbDrafts := make([]*pb.Draft, len(drafts))
	for i, draft := range drafts {
		pbDrafts[i] = draft.ToProto()
	}

	return &pb.GetUserDraftsResponse{
		Success:    true,
		Drafts:     pbDrafts,
		NextCursor: nextCursor,
	}, nil
}

// GetDraft retrieves a specific draft
func (s *DraftsService) GetDraft(ctx context.Context, req *pb.GetDraftRequest) (*pb.GetDraftResponse, error) {
	log.Printf("Getting draft %s for user %s", req.DraftId, req.UserId)

	if req.DraftId == "" {
		return &pb.GetDraftResponse{
			Success:      false,
			ErrorMessage: "draft_id is required",
		}, nil
	}

	if req.UserId == "" {
		return &pb.GetDraftResponse{
			Success:      false,
			ErrorMessage: "user_id is required",
		}, nil
	}

	draft, err := s.repo.GetDraft(req.DraftId, req.UserId)
	if err != nil {
		log.Printf("Failed to get draft: %v", err)
		return &pb.GetDraftResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.GetDraftResponse{
		Success: true,
		Draft:   draft.ToProto(),
	}, nil
}

// UpdateDraft updates an existing draft
func (s *DraftsService) UpdateDraft(ctx context.Context, req *pb.UpdateDraftRequest) (*pb.UpdateDraftResponse, error) {
	log.Printf("Updating draft %s for user %s", req.DraftId, req.UserId)

	if req.DraftId == "" {
		return &pb.UpdateDraftResponse{
			Success:      false,
			ErrorMessage: "draft_id is required",
		}, nil
	}

	if req.UserId == "" {
		return &pb.UpdateDraftResponse{
			Success:      false,
			ErrorMessage: "user_id is required",
		}, nil
	}

	if req.Content == "" {
		return &pb.UpdateDraftResponse{
			Success:      false,
			ErrorMessage: "content is required",
		}, nil
	}

	// Convert protobuf images to models
	images := make([]repository.DraftImage, len(req.Images))
	for i, img := range req.Images {
		images[i] = repository.DraftImage{
			URI:     img.Uri,
			Width:   img.Width,
			Height:  img.Height,
			AltText: img.AltText,
		}
	}

	// Convert protobuf video to model
	var video *repository.DraftVideo
	if req.Video != nil {
		video = &repository.DraftVideo{
			URI:    req.Video.Uri,
			Width:  req.Video.Width,
			Height: req.Video.Height,
		}
	}

	draft := &repository.Draft{
		DraftID:             req.DraftId,
		UserID:              req.UserId,
		Content:             req.Content,
		ReplyToURI:          req.ReplyToUri,
		QuoteURI:            req.QuoteUri,
		MentionHandle:       req.MentionHandle,
		Images:              images,
		Video:               video,
		Labels:              req.Labels,
		Threadgate:          req.Threadgate,
		InteractionSettings: req.InteractionSettings,
	}

	if err := s.repo.UpdateDraft(draft); err != nil {
		log.Printf("Failed to update draft: %v", err)
		return &pb.UpdateDraftResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.UpdateDraftResponse{
		Success: true,
		Draft:   draft.ToProto(),
	}, nil
}

// DeleteDraft deletes a draft
func (s *DraftsService) DeleteDraft(ctx context.Context, req *pb.DeleteDraftRequest) (*pb.DeleteDraftResponse, error) {
	log.Printf("Deleting draft %s for user %s", req.DraftId, req.UserId)

	if req.DraftId == "" {
		return &pb.DeleteDraftResponse{
			Success:      false,
			ErrorMessage: "draft_id is required",
		}, nil
	}

	if req.UserId == "" {
		return &pb.DeleteDraftResponse{
			Success:      false,
			ErrorMessage: "user_id is required",
		}, nil
	}

	if err := s.repo.DeleteDraft(req.DraftId, req.UserId); err != nil {
		log.Printf("Failed to delete draft: %v", err)
		return &pb.DeleteDraftResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.DeleteDraftResponse{
		Success: true,
	}, nil
}

// AutoSaveDraft creates or updates an auto-saved draft
func (s *DraftsService) AutoSaveDraft(ctx context.Context, req *pb.AutoSaveDraftRequest) (*pb.AutoSaveDraftResponse, error) {
	log.Printf("Auto-saving draft for user %s", req.UserId)

	if req.UserId == "" {
		return &pb.AutoSaveDraftResponse{
			Success:      false,
			ErrorMessage: "user_id is required",
		}, nil
	}

	if req.Content == "" {
		return &pb.AutoSaveDraftResponse{
			Success:      false,
			ErrorMessage: "content is required",
		}, nil
	}

	// Convert protobuf images to models
	images := make([]repository.DraftImage, len(req.Images))
	for i, img := range req.Images {
		images[i] = repository.DraftImage{
			URI:     img.Uri,
			Width:   img.Width,
			Height:  img.Height,
			AltText: img.AltText,
		}
	}

	// Convert protobuf video to model
	var video *repository.DraftVideo
	if req.Video != nil {
		video = &repository.DraftVideo{
			URI:    req.Video.Uri,
			Width:  req.Video.Width,
			Height: req.Video.Height,
		}
	}

	draft, err := s.repo.AutoSaveDraft(
		req.UserId,
		req.Content,
		req.ReplyToUri,
		req.QuoteUri,
		req.MentionHandle,
		images,
		video,
		req.Labels,
		req.Threadgate,
		req.InteractionSettings,
	)

	if err != nil {
		log.Printf("Failed to auto-save draft: %v", err)
		return &pb.AutoSaveDraftResponse{
			Success:      false,
			ErrorMessage: err.Error(),
		}, nil
	}

	return &pb.AutoSaveDraftResponse{
		Success: true,
		Draft:   draft.ToProto(),
	}, nil
}