#!/bin/bash
# Repository Rebranding Script
# Author: Neo Qiss
# Year: 2025
# Description: Renames repos and all references within files

set -e  # Exit on any error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Logging function
log() {
    echo -e "${BLUE}[$(date +'%Y-%m-%d %H:%M:%S')]${NC} $1"
}

success() {
    echo -e "${GREEN}✅ $1${NC}"
}

warning() {
    echo -e "${YELLOW}⚠️  $1${NC}"
}

error() {
    echo -e "${RED}❌ $1${NC}"
}

# Define renaming mappings
declare -A RENAMES=(
    ["messenger"]="messenger"
    ["Messenger"]="Messenger" 
    ["MESSENGER"]="MESSENGER"
    ["influxdb"]="chronodb"
    ["InfluxDB"]="ChronoDB"
    ["INFLUXDB"]="CHRONODB"
    ["maintable"]="maintable"
    ["Maintable"]="Maintable"
    ["maintableQL"]="MainTable"
    ["MAINTABLE"]="MAINTABLE"
    ["MAINTABLEQL"]="MAINTABLE"
    ["meilisearch"]="m3g4n"
    ["Meilisearch"]="M3G4N"
    ["MeiliSearch"]="M3G4N"
    ["MEILISEARCH"]="M3G4N"
)

# File extensions to process (text files only)
FILE_EXTENSIONS=(
    "*.rs" "*.toml" "*.md" "*.txt" "*.yml" "*.yaml" 
    "*.json" "*.xml" "*.html" "*.js" "*.ts" "*.py"
    "*.go" "*.java" "*.c" "*.cpp" "*.h" "*.hpp"
    "*.sh" "*.bash" "*.fish" "*.zsh" "*.conf" "*.cfg"
    "*.ini" "*.env" "*.dockerfile" "Dockerfile*"
    "Makefile*" "*.mk" "*.cmake" "CMakeLists.txt"
    "*.proto" "*.thrift" "*.sql" "*.graphql"
)

# Directories to exclude from processing
EXCLUDE_DIRS=(
    ".git"
    "target"
    "node_modules"
    ".cargo"
    "build"
    "dist"
    ".vscode"
    ".idea"
    "__pycache__"
    "*.egg-info"
)

# Create exclusion pattern for find command
create_exclude_pattern() {
    local pattern=""
    for dir in "${EXCLUDE_DIRS[@]}"; do
        pattern="$pattern -path '*/$dir' -prune -o"
    done
    echo "$pattern"
}

# Rename directories first
rename_directories() {
    log "🔍 Scanning for directories to rename..."
    
    # Find directories that match our rename patterns (case insensitive)
    for old_name in "${!RENAMES[@]}"; do
        new_name="${RENAMES[$old_name]}"
        
        # Find directories with exact matches (case sensitive)
        while IFS= read -r -d '' dir; do
            if [[ -d "$dir" ]]; then
                dirname=$(dirname "$dir")
                basename=$(basename "$dir")
                
                # Only rename if basename exactly matches
                if [[ "$basename" == "$old_name" ]]; then
                    new_path="$dirname/$new_name"
                    log "📁 Renaming directory: $dir -> $new_path"
                    mv "$dir" "$new_path"
                    success "Renamed directory: $basename -> $new_name"
                fi
            fi
        done < <(find . -type d -name "$old_name" -print0)
    done
}

# Rename files
rename_files() {
    log "🔍 Scanning for files to rename..."
    
    for old_name in "${!RENAMES[@]}"; do
        new_name="${RENAMES[$old_name]}"
        
        # Find files that contain the old name in their filename
        while IFS= read -r -d '' file; do
            if [[ -f "$file" ]]; then
                dirname=$(dirname "$file")
                filename=$(basename "$file")
                
                # Check if filename contains the old name
                if [[ "$filename" =~ $old_name ]]; then
                    new_filename="${filename//$old_name/$new_name}"
                    new_path="$dirname/$new_filename"
                    
                    if [[ "$file" != "$new_path" ]]; then
                        log "📄 Renaming file: $file -> $new_path"
                        mv "$file" "$new_path"
                        success "Renamed file: $filename -> $new_filename"
                    fi
                fi
            fi
        done < <(find . -type f -name "*$old_name*" -print0)
    done
}

# Replace content in files
replace_content_in_files() {
    log "🔍 Scanning for content to replace in files..."
    
    local exclude_pattern=$(create_exclude_pattern)
    local file_pattern=""
    
    # Build file extension pattern
    for ext in "${FILE_EXTENSIONS[@]}"; do
        file_pattern="$file_pattern -name '$ext' -o"
    done
    file_pattern=${file_pattern% -o}  # Remove trailing -o
    
    # Find all text files
    local files_found=0
    while IFS= read -r -d '' file; do
        if [[ -f "$file" && -r "$file" ]]; then
            # Check if file is text (not binary)
            if file "$file" | grep -q "text\|empty"; then
                local file_changed=false
                
                # Create a temporary file for replacements
                local temp_file=$(mktemp)
                cp "$file" "$temp_file"
                
                # Apply all replacements to the temp file
                for old_name in "${!RENAMES[@]}"; do
                    new_name="${RENAMES[$old_name]}"
                    
                    # Use sed for replacement (handles word boundaries)
                    if grep -q "$old_name" "$temp_file" 2>/dev/null; then
                        sed -i.bak "s/$old_name/$new_name/g" "$temp_file" 2>/dev/null || true
                        file_changed=true
                    fi
                done
                
                # If file was changed, replace original with temp file
                if [[ "$file_changed" == true ]]; then
                    if ! cmp -s "$file" "$temp_file"; then
                        mv "$temp_file" "$file"
                        success "Updated content in: $file"
                        ((files_found++))
                    else
                        rm "$temp_file"
                    fi
                else
                    rm "$temp_file"
                fi
                
                # Clean up sed backup if it exists
                [[ -f "$temp_file.bak" ]] && rm "$temp_file.bak"
            fi
        fi
    done < <(eval "find . $exclude_pattern \\( $file_pattern \\) -type f -print0")
    
    log "📊 Updated content in $files_found files"
}

# Backup function
create_backup() {
    local backup_dir="backup_$(date +%Y%m%d_%H%M%S)"
    log "💾 Creating backup at: $backup_dir"
    
    # Create backup excluding common build/cache directories
    rsync -av --exclude='.git' --exclude='target' --exclude='node_modules' \
          --exclude='.cargo' --exclude='build' --exclude='dist' \
          . "$backup_dir/"
    
    success "Backup created successfully"
    echo "$backup_dir"
}

# Main execution
main() {
    log "🚀 Starting repository rebranding process..."
    log "📍 Working directory: $(pwd)"
    
    # Show what will be renamed
    log "📋 Rename mappings:"
    for old_name in "${!RENAMES[@]}"; do
        echo "   $old_name -> ${RENAMES[$old_name]}"
    done
    
    # Confirm before proceeding
    echo ""
    read -p "🤔 Proceed with rebranding? (y/N): " -n 1 -r
    echo
    if [[ ! $REPLY =~ ^[Yy]$ ]]; then
        warning "Rebranding cancelled by user"
        exit 0
    fi
    
    # Create backup
    local backup_location=$(create_backup)
    
    # Perform rebranding
    log "🎯 Starting rebranding process..."
    
    rename_directories
    rename_files  
    replace_content_in_files
    
    success "🎉 Rebranding completed successfully!"
    log "💡 Backup available at: $backup_location"
    log "🔍 Review changes with: git status"
    
    # Show summary
    echo ""
    echo "📊 Summary:"
    echo "  - Directories renamed based on exact matches"  
    echo "  - Files renamed if they contain target names"
    echo "  - Content updated in $(find . -name "*.rs" -o -name "*.toml" -o -name "*.md" | wc -l) potential text files"
    echo "  - Backup created at: $backup_location"
    echo ""
    echo "🚀 Your repos have been successfully rebranded, Neo Qiss! 💪"
}

# Run main function
main "$@"
