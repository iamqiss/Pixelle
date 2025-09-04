package repository

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
	"sonet/src/services/drafts_service/models"
)

// CreateDraft creates a new draft
func (r *DraftsRepositoryImpl) CreateDraft(draft *Draft) error {
	if draft.DraftID == "" {
		draft.DraftID = uuid.New().String()
	}
	
	now := time.Now()
	draft.CreatedAt = now
	draft.UpdatedAt = now

	// Convert images to JSONB
	imagesJSON, err := json.Marshal(draft.Images)
	if err != nil {
		return fmt.Errorf("failed to marshal images: %w", err)
	}

	// Convert video to JSONB
	var videoJSON []byte
	if draft.Video != nil {
		videoJSON, err = json.Marshal(draft.Video)
		if err != nil {
			return fmt.Errorf("failed to marshal video: %w", err)
		}
	}

	// Convert labels to JSONB
	labelsJSON, err := json.Marshal(draft.Labels)
	if err != nil {
		return fmt.Errorf("failed to marshal labels: %w", err)
	}

	query := `
		INSERT INTO drafts (
			draft_id, user_id, title, content, reply_to_uri, quote_uri, mention_handle,
			image_uris, video_uri, labels, threadgate, interaction_settings,
			created_at, updated_at, is_auto_saved
		) VALUES (
			$1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15
		)
	`

	_, err = r.db.Exec(
		query,
		draft.DraftID,
		draft.UserID,
		draft.Title,
		draft.Content,
		draft.ReplyToURI,
		draft.QuoteURI,
		draft.MentionHandle,
		imagesJSON,
		videoJSON,
		labelsJSON,
		draft.Threadgate,
		draft.InteractionSettings,
		draft.CreatedAt,
		draft.UpdatedAt,
		draft.IsAutoSaved,
	)

	if err != nil {
		return fmt.Errorf("failed to create draft: %w", err)
	}

	return nil
}

// GetUserDrafts retrieves drafts for a user
func (r *DraftsRepositoryImpl) GetUserDrafts(userID string, limit int32, cursor string, includeAutoSaved bool) ([]*Draft, string, error) {
	query := `
		SELECT draft_id, user_id, title, content, reply_to_uri, quote_uri, mention_handle,
		       image_uris, video_uri, labels, threadgate, interaction_settings,
		       created_at, updated_at, is_auto_saved
		FROM drafts
		WHERE user_id = $1
	`

	args := []interface{}{userID}
	argIndex := 2

	if !includeAutoSaved {
		query += fmt.Sprintf(" AND is_auto_saved = $%d", argIndex)
		args = append(args, false)
		argIndex++
	}

	if cursor != "" {
		query += fmt.Sprintf(" AND updated_at < (SELECT updated_at FROM drafts WHERE draft_id = $%d)", argIndex)
		args = append(args, cursor)
		argIndex++
	}

	query += fmt.Sprintf(" ORDER BY updated_at DESC LIMIT $%d", argIndex)
	args = append(args, limit)

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get user drafts: %w", err)
	}
	defer rows.Close()

	var drafts []*Draft
	for rows.Next() {
		draft := &Draft{}
		var imagesJSON, videoJSON, labelsJSON []byte

		err := rows.Scan(
			&draft.DraftID,
			&draft.UserID,
			&draft.Title,
			&draft.Content,
			&draft.ReplyToURI,
			&draft.QuoteURI,
			&draft.MentionHandle,
			&imagesJSON,
			&videoJSON,
			&labelsJSON,
			&draft.Threadgate,
			&draft.InteractionSettings,
			&draft.CreatedAt,
			&draft.UpdatedAt,
			&draft.IsAutoSaved,
		)
		if err != nil {
			return nil, "", fmt.Errorf("failed to scan draft: %w", err)
		}

		// Unmarshal images
		if err := json.Unmarshal(imagesJSON, &draft.Images); err != nil {
			return nil, "", fmt.Errorf("failed to unmarshal images: %w", err)
		}

		// Unmarshal video
		if len(videoJSON) > 0 {
			draft.Video = &DraftVideo{}
			if err := json.Unmarshal(videoJSON, draft.Video); err != nil {
				return nil, "", fmt.Errorf("failed to unmarshal video: %w", err)
			}
		}

		// Unmarshal labels
		if err := json.Unmarshal(labelsJSON, &draft.Labels); err != nil {
			return nil, "", fmt.Errorf("failed to unmarshal labels: %w", err)
		}

		drafts = append(drafts, draft)
	}

	var nextCursor string
	if len(drafts) == int(limit) {
		nextCursor = drafts[len(drafts)-1].DraftID
	}

	return drafts, nextCursor, nil
}

// GetDraft retrieves a specific draft
func (r *DraftsRepositoryImpl) GetDraft(draftID, userID string) (*Draft, error) {
	query := `
		SELECT draft_id, user_id, title, content, reply_to_uri, quote_uri, mention_handle,
		       image_uris, video_uri, labels, threadgate, interaction_settings,
		       created_at, updated_at, is_auto_saved
		FROM drafts
		WHERE draft_id = $1 AND user_id = $2
	`

	draft := &Draft{}
	var imagesJSON, videoJSON, labelsJSON []byte

	err := r.db.QueryRow(query, draftID, userID).Scan(
		&draft.DraftID,
		&draft.UserID,
		&draft.Title,
		&draft.Content,
		&draft.ReplyToURI,
		&draft.QuoteURI,
		&draft.MentionHandle,
		&imagesJSON,
		&videoJSON,
		&labelsJSON,
		&draft.Threadgate,
		&draft.InteractionSettings,
		&draft.CreatedAt,
		&draft.UpdatedAt,
		&draft.IsAutoSaved,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("draft not found")
		}
		return nil, fmt.Errorf("failed to get draft: %w", err)
	}

	// Unmarshal images
	if err := json.Unmarshal(imagesJSON, &draft.Images); err != nil {
		return nil, fmt.Errorf("failed to unmarshal images: %w", err)
	}

			// Unmarshal video
		if len(videoJSON) > 0 {
			draft.Video = &DraftVideo{}
			if err := json.Unmarshal(videoJSON, draft.Video); err != nil {
				return nil, fmt.Errorf("failed to unmarshal video: %w", err)
			}
		}

	// Unmarshal labels
	if err := json.Unmarshal(labelsJSON, &draft.Labels); err != nil {
		return nil, fmt.Errorf("failed to unmarshal labels: %w", err)
	}

	return draft, nil
}

// UpdateDraft updates an existing draft
func (r *DraftsRepositoryImpl) UpdateDraft(draft *Draft) error {
	draft.UpdatedAt = time.Now()

	// Convert images to JSONB
	imagesJSON, err := json.Marshal(draft.Images)
	if err != nil {
		return fmt.Errorf("failed to marshal images: %w", err)
	}

	// Convert video to JSONB
	var videoJSON []byte
	if draft.Video != nil {
		videoJSON, err = json.Marshal(draft.Video)
		if err != nil {
			return fmt.Errorf("failed to marshal video: %w", err)
		}
	}

	// Convert labels to JSONB
	labelsJSON, err := json.Marshal(draft.Labels)
	if err != nil {
		return fmt.Errorf("failed to marshal labels: %w", err)
	}

	query := `
		UPDATE drafts SET
			title = $1, content = $2, reply_to_uri = $3, quote_uri = $4, mention_handle = $5,
			image_uris = $6, video_uri = $7, labels = $8, threadgate = $9, interaction_settings = $10,
			updated_at = $11
		WHERE draft_id = $12 AND user_id = $13
	`

	result, err := r.db.Exec(
		query,
		draft.Title,
		draft.Content,
		draft.ReplyToURI,
		draft.QuoteURI,
		draft.MentionHandle,
		imagesJSON,
		videoJSON,
		labelsJSON,
		draft.Threadgate,
		draft.InteractionSettings,
		draft.UpdatedAt,
		draft.DraftID,
		draft.UserID,
	)

	if err != nil {
		return fmt.Errorf("failed to update draft: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("draft not found")
	}

	return nil
}

// DeleteDraft deletes a draft
func (r *DraftsRepositoryImpl) DeleteDraft(draftID, userID string) error {
	query := `DELETE FROM drafts WHERE draft_id = $1 AND user_id = $2`

	result, err := r.db.Exec(query, draftID, userID)
	if err != nil {
		return fmt.Errorf("failed to delete draft: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("draft not found")
	}

	return nil
}

// AutoSaveDraft creates or updates an auto-saved draft
func (r *DraftsRepositoryImpl) AutoSaveDraft(userID string, content string, replyToURI, quoteURI, mentionHandle string, images []DraftImage, video *DraftVideo, labels []string, threadgate, interactionSettings []byte) (*Draft, error) {
	// First, try to find an existing auto-saved draft for this user
	query := `SELECT draft_id FROM drafts WHERE user_id = $1 AND is_auto_saved = TRUE LIMIT 1`
	
	var existingDraftID string
	err := r.db.QueryRow(query, userID).Scan(&existingDraftID)
	
	if err == sql.ErrNoRows {
		// Create new auto-saved draft
		draft := &Draft{
			DraftID:              uuid.New().String(),
			UserID:               userID,
			Content:              content,
			ReplyToURI:           replyToURI,
			QuoteURI:             quoteURI,
			MentionHandle:        mentionHandle,
			Images:               images,
			Video:                video,
			Labels:               labels,
			Threadgate:           threadgate,
			InteractionSettings:  interactionSettings,
			IsAutoSaved:          true,
		}
		
		if err := r.CreateDraft(draft); err != nil {
			return nil, err
		}
		
		return draft, nil
	} else if err != nil {
		return nil, fmt.Errorf("failed to check for existing auto-saved draft: %w", err)
	}
	
	// Update existing auto-saved draft
	draft := &Draft{
		DraftID:              existingDraftID,
		UserID:               userID,
		Content:              content,
		ReplyToURI:           replyToURI,
		QuoteURI:             quoteURI,
		MentionHandle:        mentionHandle,
		Images:               images,
		Video:                video,
		Labels:               labels,
		Threadgate:           threadgate,
		InteractionSettings:  interactionSettings,
		IsAutoSaved:          true,
	}
	
	if err := r.UpdateDraft(draft); err != nil {
		return nil, err
	}
	
	return draft, nil
}