package repository

import (
	"database/sql"
	"fmt"
	"time"

	"sonet/src/services/user_service_go/models"
)

// GetUserPrivacy retrieves the privacy setting for a user
func (r *UserRepositoryImpl) GetUserPrivacy(userID string) (bool, error) {
	query := `SELECT is_private FROM users WHERE user_id = $1`
	
	var isPrivate bool
	err := r.db.QueryRow(query, userID).Scan(&isPrivate)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, fmt.Errorf("user not found")
		}
		return false, fmt.Errorf("failed to get user privacy: %w", err)
	}
	
	return isPrivate, nil
}

// UpdateUserPrivacy updates the privacy setting for a user
func (r *UserRepositoryImpl) UpdateUserPrivacy(userID string, isPrivate bool) error {
	query := `UPDATE users SET is_private = $1, updated_at = $2 WHERE user_id = $3`
	
	result, err := r.db.Exec(query, isPrivate, time.Now(), userID)
	if err != nil {
		return fmt.Errorf("failed to update user privacy: %w", err)
	}
	
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}
	
	if rowsAffected == 0 {
		return fmt.Errorf("user not found")
	}
	
	return nil
}

// GetUserProfile retrieves a user profile with privacy check
func (r *UserRepositoryImpl) GetUserProfile(userID, requesterID string) (*models.User, error) {
	// First check if the user exists and get basic info
	query := `
		SELECT u.user_id, u.username, u.display_name, u.bio, u.avatar_url, u.banner_url,
		       u.location, u.website, u.is_verified, u.is_private, u.created_at, u.updated_at,
		       us.follower_count, us.following_count, us.note_count, us.like_count, us.renote_count, us.comment_count
		FROM users u
		LEFT JOIN user_stats us ON u.user_id = us.user_id
		WHERE u.user_id = $1
	`
	
	var user models.User
	err := r.db.QueryRow(query, userID).Scan(
		&user.UserID, &user.Username, &user.DisplayName, &user.Bio, &user.AvatarURL, &user.BannerURL,
		&user.Location, &user.Website, &user.IsVerified, &user.IsPrivate, &user.CreatedAt, &user.UpdatedAt,
		&user.Stats.FollowerCount, &user.Stats.FollowingCount, &user.Stats.NoteCount,
		&user.Stats.LikeCount, &user.Stats.RenoteCount, &user.Stats.CommentCount,
	)
	
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("user not found")
		}
		return nil, fmt.Errorf("failed to get user profile: %w", err)
	}
	
	// If the profile is private, check if the requester can view it
	if user.IsPrivate && userID != requesterID {
		// Check if requester is following the user
		followQuery := `SELECT COUNT(*) FROM follows WHERE follower_id = $1 AND following_id = $2`
		var followCount int
		err := r.db.QueryRow(followQuery, requesterID, userID).Scan(&followCount)
		if err != nil {
			return nil, fmt.Errorf("failed to check follow status: %w", err)
		}
		
		if followCount == 0 {
			return nil, fmt.Errorf("access denied: private profile")
		}
	}
	
	// Get viewer information if requester is authenticated
	if requesterID != "" {
		viewerQuery := `
			SELECT 
				(SELECT COUNT(*) FROM follows WHERE follower_id = $1 AND following_id = $2) > 0,
				(SELECT COUNT(*) FROM follows WHERE follower_id = $2 AND following_id = $1) > 0,
				(SELECT COUNT(*) FROM user_mutes WHERE muter_id = $1 AND muted_id = $2) > 0,
				(SELECT COUNT(*) FROM user_blocks WHERE blocker_id = $2 AND blocked_id = $1) > 0,
				(SELECT COUNT(*) FROM user_blocks WHERE blocker_id = $1 AND blocked_id = $2) > 0
		`
		
		err := r.db.QueryRow(viewerQuery, requesterID, userID).Scan(
			&user.Viewer.Following,
			&user.Viewer.FollowedBy,
			&user.Viewer.Muted,
			&user.Viewer.BlockedBy,
			&user.Viewer.Blocking,
		)
		
		if err != nil {
			// If viewer query fails, just set defaults
			user.Viewer = models.UserViewer{}
		}
	}
	
	return &user, nil
}