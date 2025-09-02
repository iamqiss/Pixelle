#!/bin/bash
# Scaffold Nimbux Rust project as a standalone repo
# Author: Neo Qiss
# Year: 2025
# Description: Generates directories, stub files, and initializes Git with first commit

PROJECT_DIR="Nimbux"

# Rust file copyright template
read -r -d '' COPYRIGHT << 'EOF'
// ===========================================
// Nimbux - High-Performance Object Storage
// (c) 2025 Neo Qiss. All Rights Reserved.
// Created by Neo Qiss - Unleash the power of Rust.
// ===========================================

EOF

# Directories to create
DIRS=(
    "$PROJECT_DIR/src/config"
        "$PROJECT_DIR/src/storage"
            "$PROJECT_DIR/src/metadata"
                "$PROJECT_DIR/src/network"
                    "$PROJECT_DIR/src/auth"
                        "$PROJECT_DIR/src/observability"
                            "$PROJECT_DIR/src/utils"
                                "$PROJECT_DIR/examples"
                                    "$PROJECT_DIR/tests"
                                        "$PROJECT_DIR/docs"
                                        )

                                        # Files to create with stubs
                                        declare -A FILES=(
                                            ["$PROJECT_DIR/src/main.rs"]="$COPYRIGHT\nfn main() {\n    println!(\"Nimbux server starting...\");\n}"
                                                ["$PROJECT_DIR/src/lib.rs"]="$COPYRIGHT\n// Core library exports"
                                                    
                                                        # Config
                                                            ["$PROJECT_DIR/src/config/mod.rs"]="$COPYRIGHT\n// Configuration module"
                                                                ["$PROJECT_DIR/src/config/defaults.rs"]="$COPYRIGHT\n// Default configuration values"
                                                                    
                                                                        # Storage
                                                                            ["$PROJECT_DIR/src/storage/mod.rs"]="$COPYRIGHT\n// Storage engine interface"
                                                                                ["$PROJECT_DIR/src/storage/memory.rs"]="$COPYRIGHT\n// In-memory storage backend"
                                                                                    ["$PROJECT_DIR/src/storage/disk.rs"]="$COPYRIGHT\n// Filesystem storage backend"
                                                                                        ["$PROJECT_DIR/src/storage/block.rs"]="$COPYRIGHT\n// Block/shard management"
                                                                                            ["$PROJECT_DIR/src/storage/content_addressable.rs"]="$COPYRIGHT\n// Content-addressable storage"
                                                                                                ["$PROJECT_DIR/src/storage/compression.rs"]="$COPYRIGHT\n// Compression utilities"
                                                                                                    
                                                                                                        # Metadata
                                                                                                            ["$PROJECT_DIR/src/metadata/mod.rs"]="$COPYRIGHT\n// Metadata management"
                                                                                                                ["$PROJECT_DIR/src/metadata/object.rs"]="$COPYRIGHT\n// Object metadata struct & methods"
                                                                                                                    ["$PROJECT_DIR/src/metadata/index.rs"]="$COPYRIGHT\n// Indexing & lookup"
                                                                                                                        ["$PROJECT_DIR/src/metadata/versioning.rs"]="$COPYRIGHT\n// Version control for objects"
                                                                                                                            
                                                                                                                                # Network
                                                                                                                                    ["$PROJECT_DIR/src/network/mod.rs"]="$COPYRIGHT\n// Networking module"
                                                                                                                                        ["$PROJECT_DIR/src/network/http.rs"]="$COPYRIGHT\n// HTTP API module"
                                                                                                                                            ["$PROJECT_DIR/src/network/tcp.rs"]="$COPYRIGHT\n// Custom TCP protocol module"
                                                                                                                                                
                                                                                                                                                    # Auth
                                                                                                                                                        ["$PROJECT_DIR/src/auth/mod.rs"]="$COPYRIGHT\n// Authentication module"
                                                                                                                                                            ["$PROJECT_DIR/src/auth/token.rs"]="$COPYRIGHT\n// Token-based auth"
                                                                                                                                                                
                                                                                                                                                                    # Observability
                                                                                                                                                                        ["$PROJECT_DIR/src/observability/mod.rs"]="$COPYRIGHT\n// Observability module"
                                                                                                                                                                            ["$PROJECT_DIR/src/observability/metrics.rs"]="$COPYRIGHT\n// Metrics collection"
                                                                                                                                                                                ["$PROJECT_DIR/src/observability/logging.rs"]="$COPYRIGHT\n// Logging utilities"
                                                                                                                                                                                    
                                                                                                                                                                                        # Utils
                                                                                                                                                                                            ["$PROJECT_DIR/src/utils/mod.rs"]="$COPYRIGHT\n// Helper utilities"
                                                                                                                                                                                                ["$PROJECT_DIR/src/utils/checksum.rs"]="$COPYRIGHT\n// Checksum functions"
                                                                                                                                                                                                    ["$PROJECT_DIR/src/utils/io.rs"]="$COPYRIGHT\n// Async I/O helpers"
                                                                                                                                                                                                        
                                                                                                                                                                                                            # Errors
                                                                                                                                                                                                                ["$PROJECT_DIR/src/errors.rs"]="$COPYRIGHT\n// Custom error types"
                                                                                                                                                                                                                    
                                                                                                                                                                                                                        # Examples & Tests
                                                                                                                                                                                                                            ["$PROJECT_DIR/examples/simple_client.rs"]="$COPYRIGHT\n// Example Rust client"
                                                                                                                                                                                                                                ["$PROJECT_DIR/tests/storage_tests.rs"]="$COPYRIGHT\n// Storage tests"
                                                                                                                                                                                                                                    ["$PROJECT_DIR/tests/metadata_tests.rs"]="$COPYRIGHT\n// Metadata tests"
                                                                                                                                                                                                                                        ["$PROJECT_DIR/tests/network_tests.rs"]="$COPYRIGHT\n// Network tests"
                                                                                                                                                                                                                                        )

                                                                                                                                                                                                                                        # Create directories
                                                                                                                                                                                                                                        for dir in "${DIRS[@]}"; do
                                                                                                                                                                                                                                            mkdir -p "$dir"
                                                                                                                                                                                                                                            done

                                                                                                                                                                                                                                            # Create files with content
                                                                                                                                                                                                                                            for file in "${!FILES[@]}"; do
                                                                                                                                                                                                                                                echo -e "${FILES[$file]}" > "$file"
                                                                                                                                                                                                                                                done

                                                                                                                                                                                                                                                # Initialize Git repository inside Nimbux
                                                                                                                                                                                                                                                cd "$PROJECT_DIR" || exit
                                                                                                                                                                                                                                                rm -rf .git  # Ensure no existing nested git
                                                                                                                                                                                                                                                git init
                                                                                                                                                                                                                                                git add .
                                                                                                                                                                                                                                                git commit -m "Initial Nimbux scaffold by Neo Qiss 💪"
                                                                                                                                                                                                                                                git branch -M main

                                                                                                                                                                                                                                                echo "✅ Nimbux scaffold created successfully under '$PROJECT_DIR'."
                                                                                                                                                                                                                                                echo "Git repo initialized with first commit. You're ready to start coding, Neo Qiss! 💪"