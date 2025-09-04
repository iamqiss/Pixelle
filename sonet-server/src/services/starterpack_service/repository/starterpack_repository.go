package repository

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"sonet/src/services/starterpack_service/models"
)

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
	StarterpackID string           `json:"starterpack_id"`
	ItemType      models.ItemType  `json:"item_type"`
	ItemURI       string           `json:"item_uri"`
	ItemOrder     int32            `json:"item_order"`
	AddedBy       string           `json:"added_by"`
}

// CreateStarterpack creates a new starterpack
func (r *StarterpackRepositoryImpl) CreateStarterpack(req models.CreateStarterpackRequest) (*models.Starterpack, error) {
	starterpackID := uuid.New().String()
	now := time.Now()

	query := `
		INSERT INTO starterpacks (starterpack_id, creator_id, name, description, avatar_url, is_public, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING starterpack_id, creator_id, name, description, avatar_url, is_public, created_at, updated_at
	`

	var starterpack models.Starterpack
	err := r.db.QueryRow(
		query,
		starterpackID,
		req.CreatorID,
		req.Name,
		req.Description,
		req.AvatarURL,
		req.IsPublic,
		now,
		now,
	).Scan(
		&starterpack.StarterpackID,
		&starterpack.CreatorID,
		&starterpack.Name,
		&starterpack.Description,
		&starterpack.AvatarURL,
		&starterpack.IsPublic,
		&starterpack.CreatedAt,
		&starterpack.UpdatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create starterpack: %w", err)
	}

	starterpack.ItemCount = 0
	return &starterpack, nil
}

// GetStarterpack retrieves a starterpack by ID
func (r *StarterpackRepositoryImpl) GetStarterpack(starterpackID, requesterID string) (*models.Starterpack, error) {
	// First check if user can view this starterpack
	canView, err := r.CanViewStarterpack(starterpackID, requesterID)
	if err != nil {
		return nil, fmt.Errorf("failed to check permissions: %w", err)
	}
	if !canView {
		return nil, fmt.Errorf("permission denied")
	}

	query := `
		SELECT s.starterpack_id, s.creator_id, s.name, s.description, s.avatar_url, s.is_public, 
		       s.created_at, s.updated_at, COUNT(si.item_id) as item_count
		FROM starterpacks s
		LEFT JOIN starterpack_items si ON s.starterpack_id = si.starterpack_id
		WHERE s.starterpack_id = $1
		GROUP BY s.starterpack_id, s.creator_id, s.name, s.description, s.avatar_url, s.is_public, s.created_at, s.updated_at
	`

	var starterpack models.Starterpack
	err = r.db.QueryRow(query, starterpackID).Scan(
		&starterpack.StarterpackID,
		&starterpack.CreatorID,
		&starterpack.Name,
		&starterpack.Description,
		&starterpack.AvatarURL,
		&starterpack.IsPublic,
		&starterpack.CreatedAt,
		&starterpack.UpdatedAt,
		&starterpack.ItemCount,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("starterpack not found")
		}
		return nil, fmt.Errorf("failed to get starterpack: %w", err)
	}

	return &starterpack, nil
}

// GetUserStarterpacks retrieves starterpacks created by a user
func (r *StarterpackRepositoryImpl) GetUserStarterpacks(userID, requesterID string, limit int32, cursor string) ([]*models.Starterpack, string, error) {
	// Only allow viewing own starterpacks or public starterpacks
	if userID != requesterID {
		// For now, only allow viewing own starterpacks
		return nil, "", fmt.Errorf("permission denied")
	}

	query := `
		SELECT s.starterpack_id, s.creator_id, s.name, s.description, s.avatar_url, s.is_public, 
		       s.created_at, s.updated_at, COUNT(si.item_id) as item_count
		FROM starterpacks s
		LEFT JOIN starterpack_items si ON s.starterpack_id = si.starterpack_id
		WHERE s.creator_id = $1
	`

	args := []interface{}{userID}
	argIndex := 2

	if cursor != "" {
		query += fmt.Sprintf(" AND s.created_at < (SELECT created_at FROM starterpacks WHERE starterpack_id = $%d)", argIndex)
		args = append(args, cursor)
		argIndex++
	}

	query += `
		GROUP BY s.starterpack_id, s.creator_id, s.name, s.description, s.avatar_url, s.is_public, s.created_at, s.updated_at
		ORDER BY s.created_at DESC
		LIMIT $%d
	`
	args = append(args, limit+1) // Get one extra to determine if there are more

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get user starterpacks: %w", err)
	}
	defer rows.Close()

	var starterpacks []*models.Starterpack
	var nextCursor string

	for rows.Next() {
		var starterpack models.Starterpack
		err := rows.Scan(
			&starterpack.StarterpackID,
			&starterpack.CreatorID,
			&starterpack.Name,
			&starterpack.Description,
			&starterpack.AvatarURL,
			&starterpack.IsPublic,
			&starterpack.CreatedAt,
			&starterpack.UpdatedAt,
			&starterpack.ItemCount,
		)
		if err != nil {
			return nil, "", fmt.Errorf("failed to scan starterpack: %w", err)
		}

		starterpacks = append(starterpacks, &starterpack)
	}

	// Check if we have more results
	if len(starterpacks) > int(limit) {
		nextCursor = starterpacks[len(starterpacks)-1].StarterpackID
		starterpacks = starterpacks[:limit]
	}

	return starterpacks, nextCursor, nil
}

// UpdateStarterpack updates a starterpack
func (r *StarterpackRepositoryImpl) UpdateStarterpack(req models.UpdateStarterpackRequest) (*models.Starterpack, error) {
	// Check if user can edit this starterpack
	canEdit, err := r.CanEditStarterpack(req.StarterpackID, req.RequesterID)
	if err != nil {
		return nil, fmt.Errorf("failed to check permissions: %w", err)
	}
	if !canEdit {
		return nil, fmt.Errorf("permission denied")
	}

	query := `
		UPDATE starterpacks 
		SET name = $1, description = $2, avatar_url = $3, is_public = $4, updated_at = $5
		WHERE starterpack_id = $6
		RETURNING starterpack_id, creator_id, name, description, avatar_url, is_public, created_at, updated_at
	`

	var starterpack models.Starterpack
	err = r.db.QueryRow(
		query,
		req.Name,
		req.Description,
		req.AvatarURL,
		req.IsPublic,
		time.Now(),
		req.StarterpackID,
	).Scan(
		&starterpack.StarterpackID,
		&starterpack.CreatorID,
		&starterpack.Name,
		&starterpack.Description,
		&starterpack.AvatarURL,
		&starterpack.IsPublic,
		&starterpack.CreatedAt,
		&starterpack.UpdatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("starterpack not found")
		}
		return nil, fmt.Errorf("failed to update starterpack: %w", err)
	}

	// Get item count
	itemCount, err := r.GetStarterpackItemCount(req.StarterpackID)
	if err != nil {
		return nil, fmt.Errorf("failed to get item count: %w", err)
	}
	starterpack.ItemCount = itemCount

	return &starterpack, nil
}

// DeleteStarterpack deletes a starterpack
func (r *StarterpackRepositoryImpl) DeleteStarterpack(starterpackID, requesterID string) error {
	// Check if user can edit this starterpack
	canEdit, err := r.CanEditStarterpack(starterpackID, requesterID)
	if err != nil {
		return fmt.Errorf("failed to check permissions: %w", err)
	}
	if !canEdit {
		return fmt.Errorf("permission denied")
	}

	// Delete starterpack items first (cascade should handle this, but explicit for safety)
	_, err = r.db.Exec("DELETE FROM starterpack_items WHERE starterpack_id = $1", starterpackID)
	if err != nil {
		return fmt.Errorf("failed to delete starterpack items: %w", err)
	}

	// Delete the starterpack
	result, err := r.db.Exec("DELETE FROM starterpacks WHERE starterpack_id = $1", starterpackID)
	if err != nil {
		return fmt.Errorf("failed to delete starterpack: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("starterpack not found")
	}

	return nil
}

// AddStarterpackItem adds an item to a starterpack
func (r *StarterpackRepositoryImpl) AddStarterpackItem(req models.AddStarterpackItemRequest) (*models.StarterpackItem, error) {
	// Check if user can add to this starterpack
	canAdd, err := r.CanAddToStarterpack(req.StarterpackID, req.AddedBy)
	if err != nil {
		return nil, fmt.Errorf("failed to check permissions: %w", err)
	}
	if !canAdd {
		return nil, fmt.Errorf("permission denied")
	}

	query := `
		INSERT INTO starterpack_items (item_id, starterpack_id, item_type, item_uri, item_order, created_at)
		VALUES ($1, $2, $3, $4, $5, $6)
		RETURNING item_id, starterpack_id, item_type, item_uri, item_order, created_at
	`

	var item models.StarterpackItem
	err = r.db.QueryRow(
		query,
		uuid.New().String(),
		req.StarterpackID,
		req.ItemType,
		req.ItemURI,
		req.ItemOrder,
		time.Now(),
	).Scan(
		&item.ItemID,
		&item.StarterpackID,
		&item.ItemType,
		&item.ItemURI,
		&item.ItemOrder,
		&item.CreatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to add starterpack item: %w", err)
	}

	return &item, nil
}

// RemoveStarterpackItem removes an item from a starterpack
func (r *StarterpackRepositoryImpl) RemoveStarterpackItem(starterpackID, itemID, removedBy string) error {
	// Check if user can add to this starterpack (same permission as adding)
	canAdd, err := r.CanAddToStarterpack(starterpackID, removedBy)
	if err != nil {
		return fmt.Errorf("failed to check permissions: %w", err)
	}
	if !canAdd {
		return fmt.Errorf("permission denied")
	}

	result, err := r.db.Exec(
		"DELETE FROM starterpack_items WHERE starterpack_id = $1 AND item_id = $2",
		starterpackID,
		itemID,
	)
	if err != nil {
		return fmt.Errorf("failed to remove starterpack item: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("item not found in starterpack")
	}

	return nil
}

// GetStarterpackItems retrieves items of a starterpack
func (r *StarterpackRepositoryImpl) GetStarterpackItems(starterpackID, requesterID string, limit int32, cursor string) ([]*models.StarterpackItem, string, error) {
	// Check if user can view this starterpack
	canView, err := r.CanViewStarterpack(starterpackID, requesterID)
	if err != nil {
		return nil, "", fmt.Errorf("failed to check permissions: %w", err)
	}
	if !canView {
		return nil, "", fmt.Errorf("permission denied")
	}

	query := `
		SELECT item_id, starterpack_id, item_type, item_uri, item_order, created_at
		FROM starterpack_items
		WHERE starterpack_id = $1
	`

	args := []interface{}{starterpackID}
	argIndex := 2

	if cursor != "" {
		query += fmt.Sprintf(" AND created_at < (SELECT created_at FROM starterpack_items WHERE starterpack_id = $1 AND item_id = $%d)", argIndex)
		args = append(args, cursor)
		argIndex++
	}

	query += `
		ORDER BY item_order ASC, created_at ASC
		LIMIT $%d
	`
	args = append(args, limit+1) // Get one extra to determine if there are more

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get starterpack items: %w", err)
	}
	defer rows.Close()

	var items []*models.StarterpackItem
	var nextCursor string

	for rows.Next() {
		var item models.StarterpackItem
		err := rows.Scan(
			&item.ItemID,
			&item.StarterpackID,
			&item.ItemType,
			&item.ItemURI,
			&item.ItemOrder,
			&item.CreatedAt,
		)
		if err != nil {
			return nil, "", fmt.Errorf("failed to scan item: %w", err)
		}

		items = append(items, &item)
	}

	// Check if we have more results
	if len(items) > int(limit) {
		nextCursor = items[len(items)-1].ItemID
		items = items[:limit]
	}

	return items, nextCursor, nil
}

// GetStarterpackItemCount gets the number of items in a starterpack
func (r *StarterpackRepositoryImpl) GetStarterpackItemCount(starterpackID string) (int32, error) {
	var count int32
	query := "SELECT COUNT(*) FROM starterpack_items WHERE starterpack_id = $1"
	
	err := r.db.QueryRow(query, starterpackID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get item count: %w", err)
	}

	return count, nil
}

// CanViewStarterpack checks if a user can view a starterpack
func (r *StarterpackRepositoryImpl) CanViewStarterpack(starterpackID, requesterID string) (bool, error) {
	var isPublic bool
	var creatorID string
	
	query := "SELECT is_public, creator_id FROM starterpacks WHERE starterpack_id = $1"
	err := r.db.QueryRow(query, starterpackID).Scan(&isPublic, &creatorID)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, fmt.Errorf("starterpack not found")
		}
		return false, fmt.Errorf("failed to get starterpack visibility: %w", err)
	}

	// Creator can always view their own starterpacks
	if creatorID == requesterID {
		return true, nil
	}

	// Public starterpacks can be viewed by anyone
	if isPublic {
		return true, nil
	}

	// Private starterpacks can only be viewed by creator
	return false, nil
}

// CanEditStarterpack checks if a user can edit a starterpack
func (r *StarterpackRepositoryImpl) CanEditStarterpack(starterpackID, requesterID string) (bool, error) {
	var creatorID string
	
	query := "SELECT creator_id FROM starterpacks WHERE starterpack_id = $1"
	err := r.db.QueryRow(query, starterpackID).Scan(&creatorID)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, fmt.Errorf("starterpack not found")
		}
		return false, fmt.Errorf("failed to get starterpack creator: %w", err)
	}

	// Only creator can edit
	return creatorID == requesterID, nil
}

// CanAddToStarterpack checks if a user can add items to a starterpack
func (r *StarterpackRepositoryImpl) CanAddToStarterpack(starterpackID, requesterID string) (bool, error) {
	// For now, only starterpack creator can add items
	return r.CanEditStarterpack(starterpackID, requesterID)
}

// GetSuggestedStarterpacks gets suggested starterpacks for a user
func (r *StarterpackRepositoryImpl) GetSuggestedStarterpacks(userID string, limit int32, cursor string) ([]*models.Starterpack, string, error) {
	query := `
		SELECT s.starterpack_id, s.creator_id, s.name, s.description, s.avatar_url, s.is_public, 
		       s.created_at, s.updated_at, COUNT(si.item_id) as item_count
		FROM starterpacks s
		LEFT JOIN starterpack_items si ON s.starterpack_id = si.starterpack_id
		WHERE s.is_public = TRUE AND s.creator_id != $1
	`

	args := []interface{}{userID}
	argIndex := 2

	if cursor != "" {
		query += fmt.Sprintf(" AND s.created_at < (SELECT created_at FROM starterpacks WHERE starterpack_id = $%d)", argIndex)
		args = append(args, cursor)
		argIndex++
	}

	query += `
		GROUP BY s.starterpack_id, s.creator_id, s.name, s.description, s.avatar_url, s.is_public, s.created_at, s.updated_at
		ORDER BY s.created_at DESC
		LIMIT $%d
	`
	args = append(args, limit+1) // Get one extra to determine if there are more

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get suggested starterpacks: %w", err)
	}
	defer rows.Close()

	var starterpacks []*models.Starterpack
	var nextCursor string

	for rows.Next() {
		var starterpack models.Starterpack
		err := rows.Scan(
			&starterpack.StarterpackID,
			&starterpack.CreatorID,
			&starterpack.Name,
			&starterpack.Description,
			&starterpack.AvatarURL,
			&starterpack.IsPublic,
			&starterpack.CreatedAt,
			&starterpack.UpdatedAt,
			&starterpack.ItemCount,
		)
		if err != nil {
			return nil, "", fmt.Errorf("failed to scan starterpack: %w", err)
		}

		starterpacks = append(starterpacks, &starterpack)
	}

	// Check if we have more results
	if len(starterpacks) > int(limit) {
		nextCursor = starterpacks[len(starterpacks)-1].StarterpackID
		starterpacks = starterpacks[:limit]
	}

	return starterpacks, nextCursor, nil
}