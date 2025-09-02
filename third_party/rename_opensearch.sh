#!/bin/bash

# Exit on any error
set -e

# Define variables
OLD_NAME="OpenSearch"
NEW_NAME="Density"
AWS_PATTERNS=("AWS" "aws" "Amazon Web Services" "boto3" "aws-sdk" "AWS_ACCESS_KEY" "AWS_SECRET_KEY" "AWS_REGION" "aws_")

# Log function for output
log() {
    echo "[INFO] $1"
    }

    # Function to check if a command exists
    command_exists() {
        command -v "$1" >/dev/null 2>&1
        }

        # Check for required tools
        if ! command_exists find; then
            echo "Error: 'find' is required but not installed."
                exit 1
                fi
                if ! command_exists sed; then
                    echo "Error: 'sed' is required but not installed."
                        exit 1
                        fi

                        # Step 1: Rename files and folders
                        log "Renaming files and folders containing 'OpenSearch' to 'Density'..."
                        find . -depth -type d -o -type f -iname "*${OLD_NAME}*" | while read -r item; do
                            new_item=$(echo "$item" | sed -E "s/${OLD_NAME}/${NEW_NAME}/gi")
                                if [ "$item" != "$new_item" ]; then
                                        mv -v "$item" "$new_item"
                                                log "Renamed: $item -> $new_item"
                                                    fi
                                                    done

                                                    # Step 2: Replace OpenSearch with Density in file contents
                                                    log "Replacing 'OpenSearch' with 'Density' in file contents..."
                                                    find . -type f -not -path '*/.git/*' -exec grep -il "${OLD_NAME}" {} \; | while read -r file; do
                                                        sed -i'' -E "s/${OLD_NAME}/${NEW_NAME}/gi" "$file"
                                                            log "Updated file: $file"
                                                            done

                                                            # Step 3: Purge AWS integrations from file contents
                                                            log "Removing AWS integrations from files..."
                                                            for pattern in "${AWS_PATTERNS[@]}"; do
                                                                find . -type f -not -path '*/.git/*' -exec grep -il "${pattern}" {} \; | while read -r file; do
                                                                        # Remove lines containing AWS-related patterns
                                                                                sed -i'' -E "/${pattern}/d" "$file"
                                                                                        log "Removed AWS references ($pattern) from: $file"
                                                                                            done
                                                                                            done

                                                                                            # Step 4: Remove AWS configuration files
                                                                                            log "Removing AWS configuration files..."
                                                                                            find . -type f -name "aws_config" -o -name "aws_credentials" -o -name "*.aws" -exec rm -v {} \;

                                                                                            # Step 5: Clean up empty files after AWS removal
                                                                                            log "Cleaning up empty files..."
                                                                                            find . -type f -empty -exec rm -v {} \;

                                                                                            log "Script completed successfully!"