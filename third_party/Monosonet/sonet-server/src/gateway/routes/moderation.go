package routes

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"

	pb "sonet/src/services/moderation_service/proto"
	"sonet/src/utils/auth"
	"sonet/src/utils/logger"
)

// ModerationRoutes handles all moderation-related HTTP endpoints
type ModerationRoutes struct {
	moderationClient pb.ModerationServiceClient
	logger          *logger.Logger
}

// NewModerationRoutes creates a new instance of ModerationRoutes
func NewModerationRoutes() (*ModerationRoutes, error) {
	// Connect to moderation service via gRPC
	conn, err := grpc.Dial("localhost:50051", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("failed to connect to moderation service: %v", err)
	}

	client := pb.NewModerationServiceClient(conn)

	return &ModerationRoutes{
		moderationClient: client,
		logger:          logger.NewLogger("moderation_routes"),
	}, nil
}

// RegisterRoutes registers all moderation routes
func (mr *ModerationRoutes) RegisterRoutes(router *gin.Engine) {
	moderation := router.Group("/api/v1/moderation")
	{
		// Founder-only endpoints
		moderation.Use(mr.requireFounder)
		{
			// Account moderation
			moderation.NOTE("/accounts/flag", mr.flagAccount)
			moderation.DELETE("/accounts/flag/:flagId", mr.removeFlag)
			moderation.NOTE("/accounts/shadowban", mr.shadowbanAccount)
			moderation.NOTE("/accounts/suspend", mr.suspendAccount)
			moderation.NOTE("/accounts/ban", mr.banAccount)

			// Note moderation
			moderation.DELETE("/notes/:noteId", mr.deleteNote)

			// Moderation queries
			moderation.GET("/accounts/flagged", mr.getFlaggedAccounts)
			moderation.GET("/queue", mr.getModerationQueue)
			moderation.GET("/stats", mr.getModerationStats)
		}
	}
}

// requireFounder middleware ensures only founders can access moderation endpoints
func (mr *ModerationRoutes) requireFounder(c *gin.Context) {
	userID := auth.GetUserIDFromContext(c)
	if userID == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "Authentication required"})
		c.Abort()
		return
	}

	// Check if user is founder via gRPC
	ctx := context.Background()
	resp, err := mr.moderationClient.IsFounder(ctx, &pb.IsFounderRequest{
		UserId: userID,
	})

	if err != nil {
		mr.logger.Error("Failed to check founder status", "error", err, "user_id", userID)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to verify permissions"})
		c.Abort()
		return
	}

	if !resp.IsFounder {
		c.JSON(http.StatusForbidden, gin.H{"error": "Founder access required"})
		c.Abort()
		return
	}

	c.Next()
}

// flagAccount flags an account for moderation review
func (mr *ModerationRoutes) flagAccount(c *gin.Context) {
	var request struct {
		TargetUserID    string `json:"target_user_id" binding:"required"`
		TargetUsername  string `json:"target_username" binding:"required"`
		Reason          string `json:"reason" binding:"required"`
		WarningMessage  string `json:"warning_message"`
	}

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	founderID := auth.GetUserIDFromContext(c)
	founderUsername := auth.GetUsernameFromContext(c)

	// Call moderation service via gRPC
	ctx := context.Background()
	resp, err := mr.moderationClient.FlagAccount(ctx, &pb.FlagAccountRequest{
		TargetUserId:    request.TargetUserID,
		TargetUsername:  request.TargetUsername,
		Reason:          request.Reason,
		WarningMessage:  request.WarningMessage,
		FounderId:       founderID,
		FounderUsername: founderUsername,
	})

	if err != nil {
		mr.logger.Error("Failed to flag account", "error", err, "target_user", request.TargetUserID)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to flag account"})
		return
	}

	if !resp.Success {
		c.JSON(http.StatusBadRequest, gin.H{"error": resp.Error})
		return
	}

	// Log the action (founder identity hidden)
	mr.logger.Info("Account flagged for moderation review", 
		"target_user", request.TargetUserID,
		"reason", request.Reason,
		"expires_at", resp.ExpiresAt)

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": resp.Message,
		"flag_id": resp.FlagId,
		"expires_at": resp.ExpiresAt,
	})
}

// removeFlag removes a flag from an account
func (mr *ModerationRoutes) removeFlag(c *gin.Context) {
	flagID := c.Param("flagId")
	founderID := auth.GetUserIDFromContext(c)

	ctx := context.Background()
	resp, err := mr.moderationClient.RemoveFlag(ctx, &pb.RemoveFlagRequest{
		FlagId:    flagID,
		FounderId: founderID,
	})

	if err != nil {
		mr.logger.Error("Failed to remove flag", "error", err, "flag_id", flagID)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to remove flag"})
		return
	}

	if !resp.Success {
		c.JSON(http.StatusBadRequest, gin.H{"error": resp.Error})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": resp.Message,
	})
}

// shadowbanAccount shadowbans an account
func (mr *ModerationRoutes) shadowbanAccount(c *gin.Context) {
	var request struct {
		TargetUserID   string `json:"target_user_id" binding:"required"`
		TargetUsername string `json:"target_username" binding:"required"`
		Reason         string `json:"reason" binding:"required"`
	}

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	founderID := auth.GetUserIDFromContext(c)

	ctx := context.Background()
	resp, err := mr.moderationClient.ShadowbanAccount(ctx, &pb.ShadowbanAccountRequest{
		TargetUserId:   request.TargetUserID,
		TargetUsername: request.TargetUsername,
		Reason:         request.Reason,
		FounderId:      founderID,
	})

	if err != nil {
		mr.logger.Error("Failed to shadowban account", "error", err, "target_user", request.TargetUserID)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to shadowban account"})
		return
	}

	if !resp.Success {
		c.JSON(http.StatusBadRequest, gin.H{"error": resp.Error})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": resp.Message,
	})
}

// suspendAccount suspends an account
func (mr *ModerationRoutes) suspendAccount(c *gin.Context) {
	var request struct {
		TargetUserID   string `json:"target_user_id" binding:"required"`
		TargetUsername string `json:"target_username" binding:"required"`
		Reason         string `json:"reason" binding:"required"`
		DurationDays   int32  `json:"duration_days" binding:"required,min=1,max=365"`
	}

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	founderID := auth.GetUserIDFromContext(c)

	ctx := context.Background()
	resp, err := mr.moderationClient.SuspendAccount(ctx, &pb.SuspendAccountRequest{
		TargetUserId:   request.TargetUserID,
		TargetUsername: request.TargetUsername,
		Reason:         request.Reason,
		DurationDays:   request.DurationDays,
		FounderId:      founderID,
	})

	if err != nil {
		mr.logger.Error("Failed to suspend account", "error", err, "target_user", request.TargetUserID)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to suspend account"})
		return
	}

	if !resp.Success {
		c.JSON(http.StatusBadRequest, gin.H{"error": resp.Error})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": resp.Message,
		"suspension_until": resp.SuspensionUntil,
	})
}

// banAccount bans an account
func (mr *ModerationRoutes) banAccount(c *gin.Context) {
	var request struct {
		TargetUserID   string `json:"target_user_id" binding:"required"`
		TargetUsername string `json:"target_username" binding:"required"`
		Reason         string `json:"reason" binding:"required"`
		Permanent      bool   `json:"permanent"`
	}

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	founderID := auth.GetUserIDFromContext(c)

	ctx := context.Background()
	resp, err := mr.moderationClient.BanAccount(ctx, &pb.BanAccountRequest{
		TargetUserId:   request.TargetUserID,
		TargetUsername: request.TargetUsername,
		Reason:         request.Reason,
		Permanent:      request.Permanent,
		FounderId:      founderID,
	})

	if err != nil {
		mr.logger.Error("Failed to ban account", "error", err, "target_user", request.TargetUserID)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to ban account"})
		return
	}

	if !resp.Success {
		c.JSON(http.StatusBadRequest, gin.H{"error": resp.Error})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": resp.Message,
	})
}

// deleteNote deletes a note (appears as Sonet moderation)
func (mr *ModerationRoutes) deleteNote(c *gin.Context) {
	noteID := c.Param("noteId")
	var request struct {
		TargetUserID string `json:"target_user_id" binding:"required"`
		Reason       string `json:"reason" binding:"required"`
	}

	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	founderID := auth.GetUserIDFromContext(c)

	ctx := context.Background()
	resp, err := mr.moderationClient.DeleteNote(ctx, &pb.DeleteNoteRequest{
		NoteId:      noteID,
		TargetUserId: request.TargetUserID,
		Reason:       request.Reason,
		FounderId:    founderID,
	})

	if err != nil {
		mr.logger.Error("Failed to delete note", "error", err, "note_id", noteID)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete note"})
		return
	}

	if !resp.Success {
		c.JSON(http.StatusBadRequest, gin.H{"error": resp.Error})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"success": true,
		"message": resp.Message,
	})
}

// getFlaggedAccounts retrieves all flagged accounts
func (mr *ModerationRoutes) getFlaggedAccounts(c *gin.Context) {
	includeExpired := c.Query("include_expired") == "true"
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	founderID := auth.GetUserIDFromContext(c)

	ctx := context.Background()
	resp, err := mr.moderationClient.GetFlaggedAccounts(ctx, &pb.GetFlaggedAccountsRequest{
		FounderId:      founderID,
		IncludeExpired: includeExpired,
		Limit:          int32(limit),
		Offset:         int32(offset),
	})

	if err != nil {
		mr.logger.Error("Failed to get flagged accounts", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve flagged accounts"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"accounts":    resp.Accounts,
		"total_count": resp.TotalCount,
	})
}

// getModerationQueue retrieves the moderation queue
func (mr *ModerationRoutes) getModerationQueue(c *gin.Context) {
	contentType := c.Query("content_type")
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	offset, _ := strconv.Atoi(c.DefaultQuery("offset", "0"))
	founderID := auth.GetUserIDFromContext(c)

	ctx := context.Background()
	resp, err := mr.moderationClient.GetModerationQueue(ctx, &pb.GetModerationQueueRequest{
		FounderId:   founderID,
		ContentType: contentType,
		Limit:       int32(limit),
		Offset:      int32(offset),
	})

	if err != nil {
		mr.logger.Error("Failed to get moderation queue", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve moderation queue"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"items":       resp.Items,
		"total_count": resp.TotalCount,
	})
}

// getModerationStats retrieves moderation statistics
func (mr *ModerationRoutes) getModerationStats(c *gin.Context) {
	periodStart := c.Query("period_start")
	periodEnd := c.Query("period_end")
	founderID := auth.GetUserIDFromContext(c)

	var startTime, endTime *time.Time
	if periodStart != "" {
		if t, err := time.Parse(time.RFC3339, periodStart); err == nil {
			startTime = &t
		}
	}
	if periodEnd != "" {
		if t, err := time.Parse(time.RFC3339, periodEnd); err == nil {
			endTime = &t
		}
	}

	ctx := context.Background()
	resp, err := mr.moderationClient.GetModerationStats(ctx, &pb.GetModerationStatsRequest{
		FounderId:  founderID,
		PeriodStart: startTime,
		PeriodEnd:   endTime,
	})

	if err != nil {
		mr.logger.Error("Failed to get moderation stats", "error", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to retrieve moderation statistics"})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"stats": resp.Stats,
	})
}