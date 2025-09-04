package repository

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/google/uuid"
	"sonet/src/services/list_service/models"
)

// CreateListRequest represents the request to create a list
type CreateListRequest struct {
	OwnerID     string           `json:"owner_id"`
	Name        string           `json:"name"`
	Description string           `json:"description"`
	IsPublic    bool             `json:"is_public"`
	ListType    models.ListType  `json:"list_type"`
}

// UpdateListRequest represents the request to update a list
type UpdateListRequest struct {
	ListID      string           `json:"list_id"`
	RequesterID string           `json:"requester_id"`
	Name        string           `json:"name"`
	Description string           `json:"description"`
	IsPublic    bool             `json:"is_public"`
	ListType    models.ListType  `json:"list_type"`
}

// AddListMemberRequest represents the request to add a member to a list
type AddListMemberRequest struct {
	ListID  string `json:"list_id"`
	UserID  string `json:"user_id"`
	AddedBy string `json:"added_by"`
	Notes   string `json:"notes"`
}

// CreateList creates a new list
func (r *ListRepositoryImpl) CreateList(req CreateListRequest) (*models.List, error) {
	listID := uuid.New().String()
	now := time.Now()

	query := `
		INSERT INTO user_lists (list_id, owner_id, name, description, is_public, list_type, created_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
		RETURNING list_id, owner_id, name, description, is_public, list_type, created_at, updated_at
	`

	var list models.List
	err := r.db.QueryRow(
		query,
		listID,
		req.OwnerID,
		req.Name,
		req.Description,
		req.IsPublic,
		req.ListType,
		now,
		now,
	).Scan(
		&list.ListID,
		&list.OwnerID,
		&list.Name,
		&list.Description,
		&list.IsPublic,
		&list.ListType,
		&list.CreatedAt,
		&list.UpdatedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to create list: %w", err)
	}

	list.MemberCount = 0
	return &list, nil
}

// GetList retrieves a list by ID
func (r *ListRepositoryImpl) GetList(listID, requesterID string) (*models.List, error) {
	// First check if user can view this list
	canView, err := r.CanViewList(listID, requesterID)
	if err != nil {
		return nil, fmt.Errorf("failed to check permissions: %w", err)
	}
	if !canView {
		return nil, fmt.Errorf("permission denied")
	}

	query := `
		SELECT l.list_id, l.owner_id, l.name, l.description, l.is_public, l.list_type, 
		       l.created_at, l.updated_at, COUNT(lm.user_id) as member_count
		FROM user_lists l
		LEFT JOIN user_list_members lm ON l.list_id = lm.list_id
		WHERE l.list_id = $1
		GROUP BY l.list_id, l.owner_id, l.name, l.description, l.is_public, l.list_type, l.created_at, l.updated_at
	`

	var list models.List
	err = r.db.QueryRow(query, listID).Scan(
		&list.ListID,
		&list.OwnerID,
		&list.Name,
		&list.Description,
		&list.IsPublic,
		&list.ListType,
		&list.CreatedAt,
		&list.UpdatedAt,
		&list.MemberCount,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("list not found")
		}
		return nil, fmt.Errorf("failed to get list: %w", err)
	}

	return &list, nil
}

// GetUserLists retrieves lists owned by a user
func (r *ListRepositoryImpl) GetUserLists(userID, requesterID string, limit int32, cursor string) ([]*models.List, string, error) {
	// Only allow viewing own lists or public lists
	if userID != requesterID {
		// For now, only allow viewing own lists
		return nil, "", fmt.Errorf("permission denied")
	}

	query := `
		SELECT l.list_id, l.owner_id, l.name, l.description, l.is_public, l.list_type, 
		       l.created_at, l.updated_at, COUNT(lm.user_id) as member_count
		FROM user_lists l
		LEFT JOIN user_list_members lm ON l.list_id = lm.list_id
		WHERE l.owner_id = $1
	`

	args := []interface{}{userID}
	argIndex := 2

	if cursor != "" {
		query += fmt.Sprintf(" AND l.created_at < (SELECT created_at FROM user_lists WHERE list_id = $%d)", argIndex)
		args = append(args, cursor)
		argIndex++
	}

	query += `
		GROUP BY l.list_id, l.owner_id, l.name, l.description, l.is_public, l.list_type, l.created_at, l.updated_at
		ORDER BY l.created_at DESC
		LIMIT $%d
	`
	args = append(args, limit+1) // Get one extra to determine if there are more

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get user lists: %w", err)
	}
	defer rows.Close()

	var lists []*models.List
	var nextCursor string

	for rows.Next() {
		var list models.List
		err := rows.Scan(
			&list.ListID,
			&list.OwnerID,
			&list.Name,
			&list.Description,
			&list.IsPublic,
			&list.ListType,
			&list.CreatedAt,
			&list.UpdatedAt,
			&list.MemberCount,
		)
		if err != nil {
			return nil, "", fmt.Errorf("failed to scan list: %w", err)
		}

		lists = append(lists, &list)
	}

	// Check if we have more results
	if len(lists) > int(limit) {
		nextCursor = lists[len(lists)-1].ListID
		lists = lists[:limit]
	}

	return lists, nextCursor, nil
}

// UpdateList updates a list
func (r *ListRepositoryImpl) UpdateList(req UpdateListRequest) (*models.List, error) {
	// Check if user can edit this list
	canEdit, err := r.CanEditList(req.ListID, req.RequesterID)
	if err != nil {
		return nil, fmt.Errorf("failed to check permissions: %w", err)
	}
	if !canEdit {
		return nil, fmt.Errorf("permission denied")
	}

	query := `
		UPDATE user_lists 
		SET name = $1, description = $2, is_public = $3, list_type = $4, updated_at = $5
		WHERE list_id = $6
		RETURNING list_id, owner_id, name, description, is_public, list_type, created_at, updated_at
	`

	var list models.List
	err = r.db.QueryRow(
		query,
		req.Name,
		req.Description,
		req.IsPublic,
		req.ListType,
		time.Now(),
		req.ListID,
	).Scan(
		&list.ListID,
		&list.OwnerID,
		&list.Name,
		&list.Description,
		&list.IsPublic,
		&list.ListType,
		&list.CreatedAt,
		&list.UpdatedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("list not found")
		}
		return nil, fmt.Errorf("failed to update list: %w", err)
	}

	// Get member count
	memberCount, err := r.GetListMemberCount(req.ListID)
	if err != nil {
		return nil, fmt.Errorf("failed to get member count: %w", err)
	}
	list.MemberCount = memberCount

	return &list, nil
}

// DeleteList deletes a list
func (r *ListRepositoryImpl) DeleteList(listID, requesterID string) error {
	// Check if user can edit this list
	canEdit, err := r.CanEditList(listID, requesterID)
	if err != nil {
		return fmt.Errorf("failed to check permissions: %w", err)
	}
	if !canEdit {
		return fmt.Errorf("permission denied")
	}

	// Delete list members first (cascade should handle this, but explicit for safety)
	_, err = r.db.Exec("DELETE FROM user_list_members WHERE list_id = $1", listID)
	if err != nil {
		return fmt.Errorf("failed to delete list members: %w", err)
	}

	// Delete the list
	result, err := r.db.Exec("DELETE FROM user_lists WHERE list_id = $1", listID)
	if err != nil {
		return fmt.Errorf("failed to delete list: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("list not found")
	}

	return nil
}

// AddListMember adds a member to a list
func (r *ListRepositoryImpl) AddListMember(req AddListMemberRequest) (*models.ListMember, error) {
	// Check if user can add to this list
	canAdd, err := r.CanAddToList(req.ListID, req.AddedBy)
	if err != nil {
		return nil, fmt.Errorf("failed to check permissions: %w", err)
	}
	if !canAdd {
		return nil, fmt.Errorf("permission denied")
	}

	// Check if user is already in the list
	isMember, err := r.IsUserInList(req.ListID, req.UserID)
	if err != nil {
		return nil, fmt.Errorf("failed to check if user is in list: %w", err)
	}
	if isMember {
		return nil, fmt.Errorf("user is already in the list")
	}

	query := `
		INSERT INTO user_list_members (list_id, user_id, added_by, notes, added_at)
		VALUES ($1, $2, $3, $4, $5)
		RETURNING list_id, user_id, added_by, notes, added_at
	`

	var member models.ListMember
	err = r.db.QueryRow(
		query,
		req.ListID,
		req.UserID,
		req.AddedBy,
		req.Notes,
		time.Now(),
	).Scan(
		&member.ListID,
		&member.UserID,
		&member.AddedBy,
		&member.Notes,
		&member.AddedAt,
	)

	if err != nil {
		return nil, fmt.Errorf("failed to add list member: %w", err)
	}

	return &member, nil
}

// RemoveListMember removes a member from a list
func (r *ListRepositoryImpl) RemoveListMember(listID, userID, removedBy string) error {
	// Check if user can add to this list (same permission as adding)
	canAdd, err := r.CanAddToList(listID, removedBy)
	if err != nil {
		return fmt.Errorf("failed to check permissions: %w", err)
	}
	if !canAdd {
		return fmt.Errorf("permission denied")
	}

	result, err := r.db.Exec(
		"DELETE FROM user_list_members WHERE list_id = $1 AND user_id = $2",
		listID,
		userID,
	)
	if err != nil {
		return fmt.Errorf("failed to remove list member: %w", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return fmt.Errorf("failed to get rows affected: %w", err)
	}

	if rowsAffected == 0 {
		return fmt.Errorf("member not found in list")
	}

	return nil
}

// GetListMembers retrieves members of a list
func (r *ListRepositoryImpl) GetListMembers(listID, requesterID string, limit int32, cursor string) ([]*models.ListMember, string, error) {
	// Check if user can view this list
	canView, err := r.CanViewList(listID, requesterID)
	if err != nil {
		return nil, "", fmt.Errorf("failed to check permissions: %w", err)
	}
	if !canView {
		return nil, "", fmt.Errorf("permission denied")
	}

	query := `
		SELECT list_id, user_id, added_by, notes, added_at
		FROM user_list_members
		WHERE list_id = $1
	`

	args := []interface{}{listID}
	argIndex := 2

	if cursor != "" {
		query += fmt.Sprintf(" AND added_at < (SELECT added_at FROM user_list_members WHERE list_id = $1 AND user_id = $%d)", argIndex)
		args = append(args, cursor)
		argIndex++
	}

	query += `
		ORDER BY added_at DESC
		LIMIT $%d
	`
	args = append(args, limit+1) // Get one extra to determine if there are more

	rows, err := r.db.Query(query, args...)
	if err != nil {
		return nil, "", fmt.Errorf("failed to get list members: %w", err)
	}
	defer rows.Close()

	var members []*models.ListMember
	var nextCursor string

	for rows.Next() {
		var member models.ListMember
		err := rows.Scan(
			&member.ListID,
			&member.UserID,
			&member.AddedBy,
			&member.Notes,
			&member.AddedAt,
		)
		if err != nil {
			return nil, "", fmt.Errorf("failed to scan member: %w", err)
		}

		members = append(members, &member)
	}

	// Check if we have more results
	if len(members) > int(limit) {
		nextCursor = members[len(members)-1].UserID
		members = members[:limit]
	}

	return members, nextCursor, nil
}

// IsUserInList checks if a user is in a list
func (r *ListRepositoryImpl) IsUserInList(listID, userID string) (bool, error) {
	var exists bool
	query := "SELECT EXISTS(SELECT 1 FROM user_list_members WHERE list_id = $1 AND user_id = $2)"
	
	err := r.db.QueryRow(query, listID, userID).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("failed to check if user is in list: %w", err)
	}

	return exists, nil
}

// GetListMemberCount gets the number of members in a list
func (r *ListRepositoryImpl) GetListMemberCount(listID string) (int32, error) {
	var count int32
	query := "SELECT COUNT(*) FROM user_list_members WHERE list_id = $1"
	
	err := r.db.QueryRow(query, listID).Scan(&count)
	if err != nil {
		return 0, fmt.Errorf("failed to get member count: %w", err)
	}

	return count, nil
}

// CanViewList checks if a user can view a list
func (r *ListRepositoryImpl) CanViewList(listID, requesterID string) (bool, error) {
	var isPublic bool
	var ownerID string
	
	query := "SELECT is_public, owner_id FROM user_lists WHERE list_id = $1"
	err := r.db.QueryRow(query, listID).Scan(&isPublic, &ownerID)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, fmt.Errorf("list not found")
		}
		return false, fmt.Errorf("failed to get list visibility: %w", err)
	}

	// Owner can always view their own lists
	if ownerID == requesterID {
		return true, nil
	}

	// Public lists can be viewed by anyone
	if isPublic {
		return true, nil
	}

	// Private lists can only be viewed by owner
	return false, nil
}

// CanEditList checks if a user can edit a list
func (r *ListRepositoryImpl) CanEditList(listID, requesterID string) (bool, error) {
	var ownerID string
	
	query := "SELECT owner_id FROM user_lists WHERE list_id = $1"
	err := r.db.QueryRow(query, listID).Scan(&ownerID)
	if err != nil {
		if err == sql.ErrNoRows {
			return false, fmt.Errorf("list not found")
		}
		return false, fmt.Errorf("failed to get list owner: %w", err)
	}

	// Only owner can edit
	return ownerID == requesterID, nil
}

// CanAddToList checks if a user can add members to a list
func (r *ListRepositoryImpl) CanAddToList(listID, requesterID string) (bool, error) {
	// For now, only list owner can add members
	return r.CanEditList(listID, requesterID)
}